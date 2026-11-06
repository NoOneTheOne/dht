package main

import (
	"flag"
	"log"
	"os"
	"time"

	"github.com/NoOneTheOne/dht/internal/dht"
)

func main() {
	infoHash := flag.String("infohash", "", "torrent infohash (40-char hex string)")
	flag.Parse()

	if *infoHash == "" {
		log.Fatal("--infohash is required")
	}

	if logPath := os.Getenv("LOG_FILE_PATH"); logPath != "" {
		logFile, err := os.OpenFile(logPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
		if err != nil {
			log.Fatalf("failed to open log file %s: %v", logPath, err)
		}
		log.SetOutput(logFile)
	}
	log.SetFlags(log.LstdFlags)

	d := dht.New(dht.NewStandardConfig())
	d.OnGetPeersResponse = func(infoHash string, peer *dht.Peer) {
		log.Printf("got peer: %s:%d\n", peer.IP, peer.Port)
	}

	go func() {
		for {
			err := d.GetPeers(*infoHash)
			if err != nil && err != dht.ErrNotReady {
				log.Fatal(err)
			}
			if err == dht.ErrNotReady {
				time.Sleep(time.Second * 1)
				continue
			}
			break
		}
	}()

	d.Run()
}
