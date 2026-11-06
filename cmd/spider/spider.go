package main

import (
	"encoding/hex"
	"encoding/json"
	"log"
	"os"
	"time"

	"github.com/NoOneTheOne/dht/pkg/basic"
	"github.com/NoOneTheOne/dht/pkg/config"
	"github.com/NoOneTheOne/dht/pkg/dht"
	"github.com/NoOneTheOne/dht/pkg/es"
	"github.com/NoOneTheOne/dht/pkg/metrics"
	"github.com/NoOneTheOne/dht/pkg/mysql"
	"github.com/NoOneTheOne/dht/pkg/nat"
)

type file struct {
	Path   []interface{} `json:"path"`
	Length int           `json:"length"`
}

type bitTorrent struct {
	Time     time.Time `json:"@timestamp"`
	InfoHash string    `json:"infohash"`
	Name     string    `json:"name"`
	Files    []file    `json:"files,omitempty"`
	Length   int64     `json:"length,omitempty"`
}

func main() {
	var c = config.GetConfig()

	if logPath := os.Getenv("LOG_FILE_PATH"); logPath != "" {
		logFile, err := os.OpenFile(logPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
		if err != nil {
			log.Fatalf("failed to open log file %s: %v", logPath, err)
		}
		log.SetOutput(logFile)
	}
	log.SetFlags(log.LstdFlags)

	// NAT traversal: try to map DHT port (6881 is the standard DHT port)
	go setupNAT(6881)

	// Create wire for metadata exchange
	w := dht.NewWire(65536, 4096, 1024)
	go func() {
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
			if !ok {
				continue
			}

			infoHash := hex.EncodeToString(resp.InfoHash)

			switch c.Storage.Type {
			case "mysql":
				data := &mysql.Data{
					Name:     name,
					InfoHash: infoHash,
					Time:     time.Now(),
				}
				if v, ok := info["length"]; ok {
					if length, ok := v.(int); ok {
						data.Length = int64(length)
					}
				}
				if err := mysql.Write(data); err != nil {
					log.Printf("mysql write error: %v", err)
				} else {
					log.Printf("[mysql] %s | %s | %d", data.Name, data.InfoHash, data.Length)
				}

			case "elasticsearch":
				bt := bitTorrent{
					Time:     time.Now(),
					InfoHash: infoHash,
					Name:     name,
				}
				if v, ok := info["files"]; ok {
					if files, ok := v.([]interface{}); ok {
						bt.Files = make([]file, 0, len(files))
						for _, item := range files {
							f, ok := item.(map[string]interface{})
							if !ok {
								continue
							}
							path, ok := f["path"].([]interface{})
							if !ok {
								continue
							}
							length, ok := f["length"].(int)
							if !ok {
								continue
							}
							bt.Files = append(bt.Files, file{
								Path:   path,
								Length: length,
							})
						}
					}
				} else if v, ok := info["length"]; ok {
					if length, ok := v.(int); ok {
						bt.Length = int64(length)
					}
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
	initConfig.OnAnnouncePeer = func(infoHash, ip string, port int) {
		if len(infoHash) != 20 {
			log.Printf("invalid infohash length: %d", len(infoHash))
			return
		}
		dht.RecordTorrentDiscovered()
		w.Request([]byte(infoHash), ip, port)
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
