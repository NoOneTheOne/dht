package dht

import (
	"sync"
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

type ProtocolEvent struct {
	Stage  string `json:"stage"`
	Reason string `json:"reason"`
	Count  int64  `json:"count"`
}

var protocolEvents = struct {
	sync.Mutex
	counts map[[2]string]int64
}{counts: make(map[[2]string]int64)}

func RecordProtocolEvent(stage, reason string) {
	protocolEvents.Lock()
	protocolEvents.counts[[2]string{stage, reason}]++
	protocolEvents.Unlock()
}

func GetProtocolEvents() []ProtocolEvent {
	protocolEvents.Lock()
	defer protocolEvents.Unlock()
	events := make([]ProtocolEvent, 0, len(protocolEvents.counts))
	for key, count := range protocolEvents.counts {
		events = append(events, ProtocolEvent{Stage: key[0], Reason: key[1], Count: count})
	}
	return events
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
