package dht

import (
	"sync/atomic"
)

// Stats holds DHT statistics
type Stats struct {
	PacketsReceived          int64
	PacketsSent              int64
	TorrentsDiscovered       int64
	PacketsDropped           int64
	MetadataRequestsDropped  int64
	MetadataResponsesDropped int64
}

// Global stats counter
var globalStats = &Stats{}

// RecordPacketReceived increments the packet received counter
func RecordPacketReceived() {
	atomic.AddInt64(&globalStats.PacketsReceived, 1)
}

// RecordPacketSent increments the packet sent counter
func RecordPacketSent() {
	atomic.AddInt64(&globalStats.PacketsSent, 1)
}

// RecordTorrentDiscovered increments the torrent discovered counter
func RecordTorrentDiscovered() {
	atomic.AddInt64(&globalStats.TorrentsDiscovered, 1)
}

func RecordPacketDropped() {
	atomic.AddInt64(&globalStats.PacketsDropped, 1)
}

func RecordMetadataRequestDropped() {
	atomic.AddInt64(&globalStats.MetadataRequestsDropped, 1)
}

func RecordMetadataResponseDropped() {
	atomic.AddInt64(&globalStats.MetadataResponsesDropped, 1)
}

// GetStats returns current statistics
func GetStats() *Stats {
	return &Stats{
		PacketsReceived:          atomic.LoadInt64(&globalStats.PacketsReceived),
		PacketsSent:              atomic.LoadInt64(&globalStats.PacketsSent),
		TorrentsDiscovered:       atomic.LoadInt64(&globalStats.TorrentsDiscovered),
		PacketsDropped:           atomic.LoadInt64(&globalStats.PacketsDropped),
		MetadataRequestsDropped:  atomic.LoadInt64(&globalStats.MetadataRequestsDropped),
		MetadataResponsesDropped: atomic.LoadInt64(&globalStats.MetadataResponsesDropped),
	}
}
