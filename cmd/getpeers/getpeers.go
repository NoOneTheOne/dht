package main

import (
	"encoding/hex"
	"flag"
	"log/slog"
	"time"

	"github.com/NoOneTheOne/dht/internal/dht"
	"github.com/NoOneTheOne/dht/internal/logging"
)

func main() {
	defer logging.Init("dht-getpeers")()
	flag.CommandLine.SetOutput(logging.Writer(slog.LevelError))
	infoHash := flag.String("infohash", "", "torrent infohash (40-char hex string)")
	flag.Parse()

	if *infoHash == "" {
		logging.Fatal("--infohash is required")
	}

	d := dht.New(dht.NewStandardConfig())
	d.OnGetPeersResponse = func(infoHash string, peer *dht.Peer) {
		slog.Info("peer discovered", "component", "dht", "infohash", hex.EncodeToString([]byte(infoHash)), "peer_ip", peer.IP.String(), "peer_port", peer.Port)
	}

	go func() {
		for {
			err := d.GetPeers(*infoHash)
			if err != nil && err != dht.ErrNotReady {
				logging.Fatal(err)
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
