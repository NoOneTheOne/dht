package dht

import (
	"bytes"
	"context"
	"log"
	"math"
	"net"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestBootstrapAddressesDeduplicate(t *testing.T) {
	addresses := bootstrapAddresses([]string{"127.0.0.1:6881", "127.0.0.1:6881", defaultBootstrapNodes[0]})
	seen := make(map[string]bool)
	for _, address := range addresses {
		if seen[address] {
			t.Fatalf("duplicate address: %s", address)
		}
		seen[address] = true
	}
	if len(addresses) != len(defaultBootstrapNodes)+1 {
		t.Fatalf("got %d addresses", len(addresses))
	}
}

func TestBootstrapAddressesFilterInvalid(t *testing.T) {
	for _, tt := range []struct {
		name       string
		configured []string
		want       []string
	}{
		{name: "empty uses defaults", want: defaultBootstrapNodes},
		{
			name: "tracker URLs use defaults",
			configured: []string{
				"udp://tracker.openbittorrent.com:6969/announce",
				"https://tracker.example.com:443/announce",
				"http://tracker.example.com:80/announce",
			},
			want: defaultBootstrapNodes,
		},
		{
			name:       "mixed formats",
			configured: []string{" 127.0.0.1:6881 ", "127.0.0.1:6881", "[::1]:6881", "udp://tracker.example.com:6969/announce", "", "bad", "localhost:0", ":6881", "bad/path:6881"},
			want:       append([]string{"127.0.0.1:6881", "[::1]:6881"}, defaultBootstrapNodes...),
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := bootstrapAddresses(tt.configured); !slices.Equal(got, tt.want) {
				t.Fatalf("addresses = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestResolveBootstrapAddress(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	addr, err := resolveBootstrapAddress(ctx, "udp4", "127.0.0.1:6881")
	if err != nil || !addr.IP.Equal(net.IPv4(127, 0, 0, 1)) || addr.Port != 6881 {
		t.Fatalf("unexpected resolved address: %v, %v", addr, err)
	}
	addr, err = resolveBootstrapAddress(ctx, "udp6", "[::1]:6881")
	if err != nil || !addr.IP.Equal(net.IPv6loopback) || addr.Port != 6881 {
		t.Fatalf("unexpected resolved IPv6 address: %v, %v", addr, err)
	}
	for _, address := range []string{"bad", "127.0.0.1:0", "127.0.0.1:65536", ":6881", "0.0.0.0:6881", "224.0.0.1:6881", "[::1]:6881", "udp://tracker.example.com:6969/announce", "https://tracker.example.com/announce", "tracker.example.com:6969/announce", "bad/path:6881"} {
		if _, err := resolveBootstrapAddress(ctx, "udp4", address); err == nil {
			t.Fatalf("accepted invalid IPv4 bootstrap address: %s", address)
		}
	}
}

func TestBootstrapRetryAndConcurrentJoin(t *testing.T) {
	original := defaultBootstrapNodes
	defaultBootstrapNodes = []string{"127.0.0.1:6881"}
	t.Cleanup(func() { defaultBootstrapNodes = original })
	var logs bytes.Buffer
	originalOutput := log.Writer()
	log.SetOutput(&logs)
	t.Cleanup(func() { log.SetOutput(originalOutput) })
	d := &DHT{
		InitParam: &InitParam{
			Network:    "udp4",
			Try:        1,
			PrimeNodes: []string{"udp://tracker.example.com:6969/announce", "udp://tracker.example.com:6969/announce"},
		},
		node:      &node{id: newBitmapFromString("01234567890123456789")},
		blackList: newBlackList(10),
	}
	d.routingTable = newRoutingTable(8, d)
	d.transactionManager = newTransactionManager(math.MaxUint32, d)
	bm := newBootstrapManager(d)
	var wg sync.WaitGroup
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			bm.join()
			bm.needsRebootstrap()
			bm.getBootstrapStats()
		}()
	}
	wg.Wait()
	count, _ := bm.getBootstrapStats()
	if count != 1 || bm.needsRebootstrap() {
		t.Fatal("concurrent join bypassed retry delay")
	}
	if bm.retryDelay != 2*bootstrapMinRetry {
		t.Fatalf("retry delay = %s", bm.retryDelay)
	}
	for i := 0; i < 8; i++ {
		bm.nextRetry = time.Now().Add(-time.Second)
		bm.join()
	}
	if bm.retryDelay != bootstrapMaxRetry {
		t.Fatalf("retry delay exceeded bound: %s", bm.retryDelay)
	}
	if count := strings.Count(logs.String(), "Ignoring invalid DHT bootstrap address"); count != 1 {
		t.Fatalf("invalid address logged %d times, want once", count)
	}
	if strings.Contains(logs.String(), "Failed to resolve") {
		t.Fatal("invalid address reached bootstrap DNS resolution")
	}
	if !slices.Equal(bm.addresses, defaultBootstrapNodes) {
		t.Fatalf("invalid address reached bootstrap queries: %v", bm.addresses)
	}
	bm.nextRetry = time.Now().Add(-time.Second)
	if !bm.needsRebootstrap() {
		t.Fatal("empty routing table did not retry")
	}
	for i := 0; i < 8; i++ {
		d.routingTable.cachedNodes.Set(i, &node{})
	}
	if bm.needsRebootstrap() || bm.retryDelay != bootstrapMinRetry {
		t.Fatal("healthy routing table did not reset backoff")
	}
}
