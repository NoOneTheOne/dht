package main

import (
	"encoding/hex"
	"encoding/json"
	"flag"
	"log"
	"log/slog"
	"math"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/NoOneTheOne/dht/internal/basic"
	"github.com/NoOneTheOne/dht/internal/config"
	"github.com/NoOneTheOne/dht/internal/dht"
	"github.com/NoOneTheOne/dht/internal/logging"
	"github.com/NoOneTheOne/dht/internal/metrics"
	"github.com/NoOneTheOne/dht/internal/nat"
	"github.com/NoOneTheOne/dht/internal/storage/es"
	"github.com/NoOneTheOne/dht/internal/storage/mysql"
)

type bitTorrent struct {
	Time     time.Time    `json:"@timestamp"`
	InfoHash string       `json:"infohash"`
	Name     string       `json:"name"`
	Files    []mysql.File `json:"files,omitempty"`
	Length   int64        `json:"length,omitempty"`
}

func metadataLength(value interface{}) (int64, bool) {
	switch length := value.(type) {
	case int:
		if length < 0 {
			return 0, false
		}
		return int64(length), true
	case int64:
		if length < 0 {
			return 0, false
		}
		return length, true
	default:
		return 0, false
	}
}

func torrentFiles(info map[string]interface{}) ([]mysql.File, int64, bool) {
	name, nameOK := info["name"].(string)
	if !nameOK || name == "" {
		return nil, 0, false
	}
	if value, exists := info["files"]; exists {
		if _, exists := info["length"]; exists {
			return nil, 0, false
		}
		items, ok := value.([]interface{})
		if !ok || len(items) == 0 {
			return nil, 0, false
		}
		files := make([]mysql.File, 0, len(items))
		var total int64
		for _, item := range items {
			entry, ok := item.(map[string]interface{})
			if !ok {
				return nil, 0, false
			}
			length, ok := metadataLength(entry["length"])
			if !ok || length > math.MaxInt64-total {
				return nil, 0, false
			}
			path, ok := entry["path"].([]interface{})
			if !ok || len(path) == 0 {
				return nil, 0, false
			}
			segments := make([]string, len(path))
			for i, part := range path {
				value, ok := part.(string)
				if !ok || value == "" {
					return nil, 0, false
				}
				segments[i] = value
			}
			files = append(files, mysql.File{Path: segments, Length: length})
			total += length
		}
		return files, total, true
	}
	length, ok := metadataLength(info["length"])
	if !ok {
		return nil, 0, false
	}
	return []mysql.File{{Path: []string{name}, Length: length}}, length, true
}

func writeMySQLBatches(input <-chan mysql.Data, ticks <-chan time.Time, write func([]mysql.Data) error) {
	writeMySQLBatchesWithRecovery(input, ticks, nil, func(_ string, items []mysql.Data) error {
		return write(items)
	})
}

func writeMySQLBatchesUntil(input <-chan mysql.Data, ticks <-chan time.Time, stop <-chan struct{}, write func(string, []mysql.Data) error) {
	writeMySQLBatchesWithRecovery(input, ticks, stop, write)
}

func recoveryStore() *mysql.RecoveryStore {
	path := os.Getenv("RECOVERY_FILE_PATH")
	if path == "" {
		path = filepath.Join(os.TempDir(), "dht-spider-recovery.jsonl")
	}
	return mysql.NewRecoveryStore(path)
}

func writeMySQLBatchesWithRecovery(input <-chan mysql.Data, ticks <-chan time.Time, stop <-chan struct{}, write func(string, []mysql.Data) error) {
	writeMySQLBatchesToStore(input, ticks, stop, write, recoveryStore())
}

func writeMySQLBatchesToStore(input <-chan mysql.Data, ticks <-chan time.Time, stop <-chan struct{}, write func(string, []mysql.Data) error, store *mysql.RecoveryStore) {
	batch := make([]mysql.Data, 0, 100)
	batchID := ""
	flush := func() bool {
		if len(batch) == 0 {
			return true
		}
		if batchID == "" {
			var err error
			batchID, err = mysql.NewBatchID()
			if err != nil {
				slog.Error("MySQL batch ID generation failed", "component", "mysql", "error", err)
				return false
			}
		}
		if err := write(batchID, batch); err != nil {
			if spoolErr := store.Append(mysql.RecoveryBatch{ID: batchID, Items: batch}); spoolErr != nil {
				slog.Error("MySQL and recovery write failed; retaining batch with backpressure", "component", "mysql", "event", "mysql_recovery_spool_failed", "batch_id", batchID, "error", err, "spool_error", spoolErr)
				return false
			}
			slog.Error("MySQL batch deferred for recovery", "component", "mysql", "event", "mysql_batch_failed", "batch_id", batchID, "observations", len(batch), "error", err)
		}
		batch = batch[:0]
		batchID = ""
		return true
	}
	finish := func() {
		for !flush() {
			timer := time.NewTimer(time.Second)
			<-timer.C
		}
	}
	replay := time.NewTicker(time.Minute)
	defer replay.Stop()
	for {
		incoming := input
		if len(batch) >= 100 || batchID != "" {
			incoming = nil
		}
		select {
		case item, ok := <-incoming:
			if !ok {
				finish()
				return
			}
			batch = append(batch, item)
			if len(batch) == 100 {
				flush()
			}
		case <-ticks:
			flush()
		case <-replay.C:
			if err := store.Replay(write); err != nil {
				slog.Error("MySQL recovery replay failed", "component", "mysql", "event", "mysql_recovery_failed", "error", err)
			}
		case <-stop:
			finish()
			return
		}
	}
}

func main() {
	defer logging.Init("dht-spider")()
	flag.CommandLine.SetOutput(logging.Writer(slog.LevelError))
	port := flag.Int("port", 6881, "DHT UDP listen port; forward the same public UDP port to this node")
	autoNAT := flag.Bool("nat", false, "automatically map UDP port using UPnP/NAT-PMP (disable for manual forwarding or Docker bridge)")
	queryWorkers := flag.Int("query-workers", 64, "maximum concurrent outbound DHT queries")
	queryTimeout := flag.Duration("query-timeout", 5*time.Second, "timeout per DHT query attempt")
	metadataWorkers := flag.Int("metadata-workers", 64, "maximum concurrent metadata TCP connections")
	flag.Parse()
	if *port < 1 || *port > 65535 || *queryWorkers < 1 || *metadataWorkers < 1 || *queryTimeout <= 0 {
		logging.Fatal("port must be 1..65535, worker limits and query timeout must be positive")
	}
	var c = config.GetConfig()

	natStop := make(chan struct{})
	defer close(natStop)
	if *autoNAT {
		go setupNAT(*port, natStop)
	} else {
		log.Printf("[NAT] Manual forwarding: public UDP %d -> node UDP %d; preserve the source port for outbound UDP", *port, *port)
	}

	w := dht.NewWire(65536, 4096, *metadataWorkers)
	mysqlWrites := make(chan mysql.Data, 100)
	responseDone := make(chan struct{})
	writeDone := make(chan struct{})
	if c.Storage.Type == "mysql" {
		go func() {
			defer close(writeDone)
			store := recoveryStore()
			if store != nil {
				if err := store.Replay(func(id string, items []mysql.Data) error { return mysql.WriteBatchWithID(id, items) }); err != nil {
					slog.Error("MySQL recovery replay failed", "component", "mysql", "event", "mysql_recovery_failed", "error", err)
				}
			}
			ticker := time.NewTicker(time.Second)
			defer ticker.Stop()
			writeMySQLBatchesUntil(mysqlWrites, ticker.C, nil, func(id string, items []mysql.Data) error {
				return mysql.WriteBatchWithID(id, items)
			})
		}()
	} else {
		close(writeDone)
	}
	go func() {
		defer close(responseDone)
		defer close(mysqlWrites)
		for resp := range w.Response() {
			metadata, err := dht.Decode(resp.MetadataInfo)
			if err != nil {
				dht.RecordProtocolEvent("metadata_decode", "decode_error")
				continue
			}

			info, ok := metadata.(map[string]interface{})
			if !ok {
				dht.RecordProtocolEvent("metadata_decode", "invalid_info")
				continue
			}

			name, ok := info["name"].(string)
			if !ok || name == "" {
				dht.RecordProtocolEvent("metadata_decode", "invalid_name")
				continue
			}

			files, length, ok := torrentFiles(info)
			if !ok {
				dht.RecordProtocolEvent("metadata_decode", "invalid_files")
				continue
			}
			infoHash := hex.EncodeToString(resp.InfoHash)

			switch c.Storage.Type {
			case "mysql":
				mysqlWrites <- mysql.Data{
					Name:     name,
					InfoHash: infoHash,
					Time:     time.Now(),
					Length:   length,
					Files:    files,
				}

			case "elasticsearch":
				bt := bitTorrent{
					Time:     time.Now(),
					InfoHash: infoHash,
					Name:     name,
					Files:    files,
					Length:   length,
				}

				if data, err := json.Marshal(bt); err != nil {
					basic.CheckError(err)
				} else {
					if _, err := es.Write(c.Elasticsearch.Index, "_doc", data); err != nil {
						basic.CheckError(err)
					} else {
						slog.Info("Elasticsearch metadata persisted", "component", "elasticsearch", "infohash", infoHash, "length", length)
					}
				}

			default:
				logging.Fatalf("unknown storage type: %s", c.Storage.Type)
			}
		}
	}()
	go w.Run()

	// Configure DHT
	initConfig := dht.NewCrawlConfig()
	initConfig.Address = net.JoinHostPort("", strconv.Itoa(*port))
	initConfig.QueryWorkerLimit = *queryWorkers
	initConfig.QueryTimeout = *queryTimeout
	log.Printf("[DHT] Listening on UDP %d, query workers=%d timeout=%s, metadata workers=%d", *port, *queryWorkers, *queryTimeout, *metadataWorkers)
	queuePeer := func(infoHash, ip string, peerPort int) {
		if len(infoHash) != 20 || !dht.IsPublicPeerIP(ip) {
			return
		}
		dht.RecordTorrentDiscovered()
		w.Request([]byte(infoHash), ip, peerPort)
	}
	initConfig.OnAnnouncePeer = func(infoHash, ip string, port int) {
		queuePeer(infoHash, ip, port)
	}
	initConfig.OnGetPeersResponse = func(infoHash string, peer *dht.Peer) {
		queuePeer(infoHash, peer.IP.String(), peer.Port)
	}

	// Create DHT instance
	d := dht.New(initConfig)

	// Register DHT stats getter for Prometheus metrics
	metrics.SetDHTStatsGetter(func() metrics.DHTStats {
		stats := dht.GetStats()
		dhtStats := d.GetDHTStats()
		requests, responses, active := w.QueueStats()
		events := dht.GetProtocolEvents()
		protocolEvents := make([]metrics.ProtocolEvent, len(events))
		for i, event := range events {
			protocolEvents[i] = metrics.ProtocolEvent{Stage: event.Stage, Reason: event.Reason, Count: event.Count}
		}
		return metrics.DHTStats{
			ProtocolEvents:           protocolEvents,
			QueueDepths:              map[string]int{"packet": dhtStats.PacketQueueDepth, "query": dhtStats.QueryQueueDepth, "metadata_request": requests, "metadata_response": responses, "mysql_write": len(mysqlWrites)},
			ActiveWorkers:            map[string]int{"packet": dhtStats.PacketWorkers, "query": dhtStats.ActiveTransactions, "metadata": active},
			RoutingTableNodes:        dhtStats.RoutingTableNodes,
			BootstrapCount:           dhtStats.BootstrapCount,
			PacketsReceived:          int(stats.PacketsReceived),
			PacketsSent:              int(stats.PacketsSent),
			ActiveTransactions:       dhtStats.ActiveTransactions,
			BlacklistedNodes:         dhtStats.BlacklistedNodes,
			TorrentsDiscovered:       int(stats.TorrentsDiscovered),
			PacketsDropped:           int(stats.PacketsDropped),
			MetadataRequestsDropped:  int(stats.MetadataRequestsDropped),
			MetadataResponsesDropped: int(stats.MetadataResponsesDropped),
		}
	})

	stopSignal := make(chan os.Signal, 1)
	shutdownDone := make(chan struct{})
	go func() {
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-shutdownDone:
				return
			case <-ticker.C:
				requests, responses, active := w.QueueStats()
				stats := d.GetDHTStats()
				slog.Info("DHT protocol summary", "component", "dht", "event", "dht_protocol_summary",
					"protocol_events", dht.GetProtocolEvents(), "packet_queue", stats.PacketQueueDepth,
					"query_queue", stats.QueryQueueDepth, "metadata_request_queue", requests,
					"metadata_response_queue", responses, "mysql_write_queue", len(mysqlWrites),
					"packet_workers", stats.PacketWorkers, "query_workers", stats.ActiveTransactions, "metadata_workers", active)
			}
		}
	}()
	signal.Notify(stopSignal, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		defer close(shutdownDone)
		<-stopSignal
		d.Stop()
		w.Stop()
		<-responseDone
		<-writeDone
	}()
	d.Run()
	<-shutdownDone
}

func setupNAT(port int, stop <-chan struct{}) {
	natDevice, err := nat.Discover()
	if err != nil {
		log.Printf("[NAT] No NAT device found, skipping port mapping")
		return
	}

	log.Printf("[NAT] Using %s for port mapping", natDevice.Type())

	autoMap := nat.NewAutoMap(natDevice)
	defer autoMap.Stop()

	// Map UDP port for DHT
	mapping, err := autoMap.Map("udp", port, port, "DHT Protocol")
	if err != nil {
		log.Printf("[NAT] Failed to map port %d: %v", port, err)
		return
	}

	log.Printf("[NAT] Successfully mapped %s:%d -> :%d", mapping.ExternalIP, mapping.External, mapping.Internal)

	<-stop
}
