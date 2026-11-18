package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"log/slog"
	"math"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/NoOneTheOne/dht/internal/config"
	"github.com/NoOneTheOne/dht/internal/dht"
	"github.com/NoOneTheOne/dht/internal/logging"
	"github.com/NoOneTheOne/dht/internal/metrics"
	"github.com/NoOneTheOne/dht/internal/nat"
	"github.com/NoOneTheOne/dht/internal/storage/mysql"
)

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
			// BEP-47 padding files only align pieces; they hold no content.
			if attr, _ := entry["attr"].(string); strings.Contains(attr, "p") {
				continue
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
		if len(files) == 0 { // Only padding entries.
			return nil, 0, false
		}
		return files, total, true
	}
	length, ok := metadataLength(info["length"])
	if !ok {
		return nil, 0, false
	}
	return []mysql.File{{Path: []string{name}, Length: length}}, length, true
}

func torrentMetadata(info map[string]interface{}) (mysql.Data, bool) {
	name, ok := info["name"].(string)
	if !ok || name == "" {
		dht.RecordProtocolEvent("metadata_decode", "invalid_name")
		return mysql.Data{}, false
	}
	files, length, ok := torrentFiles(info)
	if !ok {
		dht.RecordProtocolEvent("metadata_decode", "invalid_files")
		reason := "invalid_entries"
		if value, exists := info["files"]; exists {
			if _, conflicting := info["length"]; conflicting {
				reason = "conflicting_layout"
			} else if entries, list := value.([]interface{}); !list {
				reason = "invalid_list"
			} else if len(entries) == 0 {
				reason = "empty_list"
			}
		} else if _, exists := info["length"]; !exists {
			reason = "missing_layout"
		} else {
			reason = "invalid_length"
		}
		dht.RecordProtocolEvent("filelist", reason)
		return mysql.Data{}, false
	}
	dht.RecordProtocolEvent("filelist", "complete")
	return mysql.Data{Name: name, Files: files, Length: length}, true
}

func writeMySQLBatches(input <-chan mysql.Data, ticks <-chan time.Time, write func([]mysql.Data) error) {
	writeMySQLBatchesWithRecovery(input, ticks, nil, func(_ string, items []mysql.Data) error {
		return write(items)
	})
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

// writeSplit retries a permanently rejected batch item by item, so one bad
// item cannot hold the rest hostage. Each item gets its own batch ID; items
// that still fail go to rejected (permanent) or retry (transient).
func writeSplit(items []mysql.Data, write func(string, []mysql.Data) error, retry, rejected func(mysql.RecoveryBatch) error) {
	for _, item := range items {
		id, err := mysql.NewBatchID()
		if err != nil {
			slog.Error("MySQL item dropped: batch ID generation failed", "component", "mysql", "infohash", item.InfoHash, "error", err)
			continue
		}
		err = write(id, []mysql.Data{item})
		if err == nil {
			continue
		}
		target, event := retry, "mysql_item_deferred"
		if mysql.IsPermanent(err) {
			target, event = rejected, "mysql_item_rejected"
		}
		if spoolErr := target(mysql.RecoveryBatch{ID: id, Items: []mysql.Data{item}}); spoolErr != nil {
			slog.Error("MySQL item dropped: not written and not spooled", "component", "mysql", "event", "mysql_item_dropped", "infohash", item.InfoHash, "error", err, "spool_error", spoolErr)
			continue
		}
		slog.Warn("MySQL item set aside", "component", "mysql", "event", event, "batch_id", id, "infohash", item.InfoHash, "error", err)
	}
}

func writeMySQLBatchesToStore(input <-chan mysql.Data, ticks <-chan time.Time, stop <-chan struct{}, write func(string, []mysql.Data) error, store *mysql.RecoveryStore) {
	rejected := store.Rejected()
	// Replay holds the spool's lock, so split items cannot be re-spooled there.
	replayWrite := func(id string, items []mysql.Data) error {
		err := write(id, items)
		if err != nil && mysql.IsPermanent(err) {
			writeSplit(items, write, rejected.Append, rejected.Append)
			return nil
		}
		return err
	}
	replay := func() {
		if err := store.Replay(replayWrite); err != nil {
			slog.Error("MySQL recovery replay failed", "component", "mysql", "event", "mysql_recovery_failed", "error", err)
		}
	}
	replay()
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
			if mysql.IsPermanent(err) {
				// The transaction rolled back, so splitting cannot double count.
				slog.Warn("MySQL rejected batch; retrying items individually", "component", "mysql", "event", "mysql_batch_split", "batch_id", batchID, "observations", len(batch), "error", err)
				writeSplit(batch, write, store.Append, rejected.Append)
			} else if spoolErr := store.Append(mysql.RecoveryBatch{ID: batchID, Items: batch}); spoolErr != nil {
				slog.Error("MySQL and recovery write failed; retaining batch with backpressure", "component", "mysql", "event", "mysql_recovery_spool_failed", "batch_id", batchID, "error", err, "spool_error", spoolErr)
				return false
			} else {
				slog.Error("MySQL batch deferred for recovery", "component", "mysql", "event", "mysql_batch_failed", "batch_id", batchID, "observations", len(batch), "error", err)
			}
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
	replayTicker := time.NewTicker(time.Minute)
	defer replayTicker.Stop()
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
		case <-replayTicker.C:
			replay()
		case <-stop:
			finish()
			return
		}
	}
}

var crawlerStartedAt = time.Now().UTC()
var natStatus atomic.Value

func init() { natStatus.Store("manual") }

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
	if c.Storage.Type != "mysql" {
		logging.Fatalf("unsupported storage type %q: only mysql is supported", c.Storage.Type)
	}

	natStop := make(chan struct{})
	defer close(natStop)
	if *autoNAT {
		natStatus.Store("discovering")
		go setupNAT(*port, natStop)
	} else {
		log.Printf("[NAT] Manual forwarding: public UDP %d -> node UDP %d; preserve the source port for outbound UDP", *port, *port)
	}

	// stopping closes as soon as a shutdown signal arrives.
	stopping := make(chan struct{})
	w := dht.NewWire(65536, 4096, *metadataWorkers)
	mysqlWrites := make(chan mysql.Data, 100)
	responseDone := make(chan struct{})
	writeDone := make(chan struct{})
	pruneDone := make(chan struct{})
	spool := recoveryStore()
	go func() {
		defer close(pruneDone)
		pruneObservations(stopping, spool)
	}()
	go func() {
		defer close(writeDone)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		writeMySQLBatchesToStore(mysqlWrites, ticker.C, nil, mysql.WriteBatchWithID, spool)
	}()
	items := make(chan mysql.Data, 100)
	go func() {
		defer close(responseDone)
		defer close(mysqlWrites)
		for item := range items {
			mysqlWrites <- item
		}
	}()
	go w.Run()

	// Configure DHT
	initConfig := dht.NewCrawlConfig()
	initConfig.Address = net.JoinHostPort("", strconv.Itoa(*port))
	initConfig.PublicIP = os.Getenv("DHT_PUBLIC_IP")
	initConfig.QueryWorkerLimit = *queryWorkers
	initConfig.QueryTimeout = *queryTimeout
	initConfig.NodeCachePath = os.Getenv("NODE_CACHE_PATH")
	if initConfig.NodeCachePath == "" {
		recoveryPath := os.Getenv("RECOVERY_FILE_PATH")
		if recoveryPath == "" {
			recoveryPath = filepath.Join(os.TempDir(), "dht-spider-recovery.jsonl")
		}
		initConfig.NodeCachePath = filepath.Join(filepath.Dir(recoveryPath), "dht-nodes.json")
	}
	log.Printf("[DHT] Listening on UDP %d, query workers=%d timeout=%s, metadata workers=%d", *port, *queryWorkers, *queryTimeout, *metadataWorkers)
	var d *dht.DHT
	scheduler := newHashScheduler(*metadataWorkers, func(hash string) error { return d.GetPeers(hash) }, w.Request)
	initConfig.OnGetPeers = func(infoHash, _ string, _ int) {
		scheduler.discover(infoHash, "", 0, "get_peers")
	}
	initConfig.OnAnnouncePeer = func(infoHash, ip string, port int) {
		scheduler.discover(infoHash, ip, port, "announce_peer")
	}
	initConfig.OnSampleInfohashes = func(hashes []string) {
		for _, hash := range hashes {
			scheduler.discover(hash, "", 0, "sample")
		}
	}
	initConfig.OnGetPeersResponse = func(infoHash string, peer *dht.Peer) {
		scheduler.discover(infoHash, peer.IP.String(), peer.Port, "lookup")
	}

	d = dht.New(initConfig)
	go scheduler.run(w.Response(), w.Failures(), items)

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

	statusMux := http.NewServeMux()
	statusMux.HandleFunc("GET /status", func(wr http.ResponseWriter, r *http.Request) {
		stats := d.GetDHTStats()
		_, _, active := w.QueueStats()
		events := dht.GetProtocolEvents()
		protocolEvents := make([]metrics.ProtocolEvent, len(events))
		var rejected int64
		for i, event := range events {
			protocolEvents[i] = metrics.ProtocolEvent{Stage: event.Stage, Reason: event.Reason, Count: event.Count}
			if event.Stage == "metadata" && event.Reason == "rejected" {
				rejected += event.Count
			}
		}
		metadata := metrics.OutcomeRate(protocolEvents, "metadata")
		status := metrics.CrawlerStatus{
			StartedAt: crawlerStartedAt, UptimeSeconds: int64(time.Since(crawlerStartedAt).Seconds()),
			Collected: metadata.Succeeded, ConnectedPeers: w.ConnectedPeers(), RejectedPeers: rejected,
			DroppedPeers: dht.GetStats().MetadataRequestsDropped,
			Workers:      map[string]int{"packet": stats.PacketWorkers, "query": stats.ActiveTransactions, "metadata": active},
			WorkerLimits: map[string]int{"packet": initConfig.PacketWorkerLimit, "query": *queryWorkers, "metadata": *metadataWorkers},
			DHTQuery:     metrics.OutcomeRate(protocolEvents, "query"), MetadataFetch: metadata,
			NAT: natStatus.Load().(string), Reachability: d.Reachability(), Ready: stats.RoutingTableNodes > 0,
		}
		wr.Header().Set("Content-Type", "application/json")
		wr.Header().Set("Cache-Control", "no-store")
		_ = json.NewEncoder(wr).Encode(status)
	})
	statusAddr := os.Getenv("SPIDER_STATUS_ADDR")
	if statusAddr == "" {
		statusAddr = "127.0.0.1:30081"
	}
	statusServer := &http.Server{Addr: statusAddr, Handler: statusMux, ReadHeaderTimeout: 3 * time.Second, WriteTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second}
	defer statusServer.Close()
	go func() {
		if err := statusServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("Crawler status server unavailable", "error", err)
		}
	}()

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
		close(stopping) // Cancels an in-progress prune right away.
		close(scheduler.stop)
		d.Stop()
		w.Stop()
		<-responseDone
		<-writeDone
		<-pruneDone
	}()
	d.Run()
	<-shutdownDone
}

// pruneObservations bounds the observations table, which otherwise gains a row
// per infohash per batch forever. It runs shortly after start, then hourly,
// and skips rounds while the recovery spool has batches to replay.
func pruneObservations(stop <-chan struct{}, spool *mysql.RecoveryStore) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		<-stop
		cancel()
	}()
	timer := time.NewTimer(time.Minute)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		timer.Reset(time.Hour)
		if pending, err := spool.Pending(); pending || err != nil {
			slog.Info("MySQL observation pruning deferred until the recovery spool drains", "component", "mysql", "event", "mysql_prune_deferred", "error", err)
			continue
		}
		deleted, err := mysql.PruneObservations(ctx, time.Now().Add(-mysql.ObservationRetention))
		if err != nil && ctx.Err() == nil {
			slog.Error("MySQL observation pruning failed", "component", "mysql", "event", "mysql_prune_failed", "deleted", deleted, "error", err)
		} else if deleted > 0 {
			slog.Info("MySQL observations pruned", "component", "mysql", "event", "mysql_pruned", "deleted", deleted)
		}
	}
}

func setupNAT(port int, stop <-chan struct{}) {
	natDevice, err := nat.Discover()
	if err != nil {
		log.Printf("[NAT] No NAT device found, skipping port mapping")
		natStatus.Store("unavailable")
		return
	}

	log.Printf("[NAT] Using %s for port mapping", natDevice.Type())

	autoMap := nat.NewAutoMap(natDevice)
	defer autoMap.Stop()

	// Map UDP port for DHT
	mapping, err := autoMap.Map("udp", port, port, "DHT Protocol")
	if err != nil {
		log.Printf("[NAT] Failed to map port %d: %v", port, err)
		natStatus.Store("mapping_failed")
		return
	}

	natStatus.Store("mapped")
	log.Printf("[NAT] Successfully mapped %s:%d -> :%d", mapping.ExternalIP, mapping.External, mapping.Internal)

	// Reflect lapsed leases: renewals can keep failing after the first map.
	check := time.NewTicker(30 * time.Second)
	defer check.Stop()
	for {
		select {
		case <-stop:
			return
		case now := <-check.C:
			if current, ok := autoMap.GetMapping("udp", port, port); ok && current.Active(now) {
				natStatus.Store("mapped")
			} else {
				natStatus.Store("mapping_failed")
			}
		}
	}
}
