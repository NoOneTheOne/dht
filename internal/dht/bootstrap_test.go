package dht

import (
	"context"
	"math"
	"net"
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

func TestResolveBootstrapAddress(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	addr, err := resolveBootstrapAddress(ctx, "udp4", "127.0.0.1:6881")
	if err != nil || !addr.IP.Equal(net.IPv4(127, 0, 0, 1)) || addr.Port != 6881 {
		t.Fatalf("unexpected resolved address: %v, %v", addr, err)
	}
	for _, address := range []string{"bad", "127.0.0.1:0", "127.0.0.1:65536", ":6881", "0.0.0.0:6881", "224.0.0.1:6881", "[::1]:6881"} {
		if _, err := resolveBootstrapAddress(ctx, "udp4", address); err == nil {
			t.Fatalf("accepted invalid IPv4 bootstrap address: %s", address)
		}
	}
}

func TestBootstrapRetryAndConcurrentJoin(t *testing.T) {
	original := defaultBootstrapNodes
	defaultBootstrapNodes = []string{"127.0.0.1:6881"}
	t.Cleanup(func() { defaultBootstrapNodes = original })
	d := &DHT{
		InitParam: &InitParam{Network: "udp4", Try: 1},
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
