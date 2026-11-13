package main

import (
	"encoding/hex"
	"encoding/json"
	"flag"
	"log"
	"math"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/NoOneTheOne/dht/internal/basic"
	"github.com/NoOneTheOne/dht/internal/config"
	"github.com/NoOneTheOne/dht/internal/dht"
	"github.com/NoOneTheOne/dht/internal/metrics"
	"github.com/NoOneTheOne/dht/internal/nat"
	"github.com/NoOneTheOne/dht/internal/storage/es"
	"github.com/NoOneTheOne/dht/internal/storage/mysql"
)

type file struct {
	Path   []interface{} `json:"path"`
	Length int64         `json:"length"`
}

type bitTorrent struct {
	Time     time.Time `json:"@timestamp"`
	InfoHash string    `json:"infohash"`
	Name     string    `json:"name"`
	Files    []file    `json:"files,omitempty"`
	Length   int64     `json:"length,omitempty"`
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

func torrentFiles(info map[string]interface{}) ([]file, int64, bool) {
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
		files := make([]file, 0, len(items))
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
			for _, part := range path {
				if value, ok := part.(string); !ok || value == "" {
					return nil, 0, false
				}
			}
			files = append(files, file{Path: path, Length: length})
			total += length
		}
		return files, total, true
	}
	length, ok := metadataLength(info["length"])
	if !ok {
		return nil, 0, false
	}
	return []file{{Path: []interface{}{name}, Length: length}}, length, true
}

func writeMySQLBatches(input <-chan mysql.Data, ticks <-chan time.Time, write func([]mysql.Data) error) {
	batch := make([]mysql.Data, 0, 100)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		if err := write(batch); err != nil {
			log.Printf("[mysql] batch write failed for %d observations after safe retries; not replayed because commit outcome may be unknown: %v", len(batch), err)
		} else {
			log.Printf("[mysql] persisted %d metadata observations", len(batch))
		}
		batch = batch[:0]
	}
	for {
		select {
		case item, ok := <-input:
			if !ok {
				flush()
				return
			}
			batch = append(batch, item)
			if len(batch) == cap(batch) {
				flush()
			}
		case <-ticks:
			flush()
		}
	}
}

func main() {
	port := flag.Int("port", 6881, "DHT UDP listen port; forward the same public UDP port to this node")
	autoNAT := flag.Bool("nat", false, "automatically map UDP port using UPnP/NAT-PMP (disable for manual forwarding or Docker bridge)")
	queryWorkers := flag.Int("query-workers", 64, "maximum concurrent outbound DHT queries")
	queryTimeout := flag.Duration("query-timeout", 5*time.Second, "timeout per DHT query attempt")
	metadataWorkers := flag.Int("metadata-workers", 64, "maximum concurrent metadata TCP connections")
	flag.Parse()
	if *port < 1 || *port > 65535 || *queryWorkers < 1 || *metadataWorkers < 1 || *queryTimeout <= 0 {
		log.Fatal("port must be 1..65535, worker limits and query timeout must be positive")
	}
	var c = config.GetConfig()

	if logPath := os.Getenv("LOG_FILE_PATH"); logPath != "" {
		logFile, err := os.OpenFile(logPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
		if err != nil {
			log.Fatalf("failed to open log file %s: %v", logPath, err)
		}
		log.SetOutput(logFile)
	}
	log.SetFlags(log.LstdFlags)

	if *autoNAT {
		go setupNAT(*port)
	} else {
		log.Printf("[NAT] Manual forwarding: public UDP %d -> node UDP %d; preserve the source port for outbound UDP", *port, *port)
	}

	w := dht.NewWire(65536, 4096, *metadataWorkers)
	mysqlWrites := make(chan mysql.Data, 100)
	if c.Storage.Type == "mysql" {
		go func() {
			ticker := time.NewTicker(time.Second)
			defer ticker.Stop()
			writeMySQLBatches(mysqlWrites, ticker.C, mysql.WriteBatch)
		}()
	}
	go func() {
		defer close(mysqlWrites)
		for resp := range w.Response() {
			metadata, err := dht.Decode(resp.MetadataInfo)
			if err != nil {
				continue
			}

			info, ok := metadata.(map[string]interface{})
			if !ok {
				continue
			}

			name, ok := info["name"].(string)
			if !ok || name == "" {
				continue
			}

			files, length, ok := torrentFiles(info)
			if !ok {
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
					if res, err := es.Write(c.Elasticsearch.Index, "_doc", data); err != nil {
						basic.CheckError(err)
					} else {
						log.Printf("[es] %s\n%s\n", data, res)
					}
				}

			default:
				log.Fatalf("unknown storage type: %s", c.Storage.Type)
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
		return metrics.DHTStats{
			RoutingTableNodes:  dhtStats.RoutingTableNodes,
			BootstrapCount:     dhtStats.BootstrapCount,
			PacketsReceived:    int(stats.PacketsReceived),
			PacketsSent:        int(stats.PacketsSent),
			ActiveTransactions: dhtStats.ActiveTransactions,
			BlacklistedNodes:   dhtStats.BlacklistedNodes,
			TorrentsDiscovered: int(stats.TorrentsDiscovered),
		}
	})

	d.Run()
}

func setupNAT(port int) {
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

	// Keep the mappings alive until process exits — use a blocking wait
	// that can be interrupted by signals instead of select{}.
	done := make(chan struct{})
	<-done
}
