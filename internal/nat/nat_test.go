package nat

import (
	"errors"
	"net"
	"sync"
	"testing"
	"time"
)

type testNAT struct {
	attempts int
	fail     bool
}

func (*testNAT) GetExternalAddress() (net.IP, error) { return net.IPv4(203, 0, 113, 1), nil }
func (n *testNAT) AddPortMapping(string, int, int, string, time.Duration) error {
	n.attempts++
	if n.fail {
		return errors.New("gateway unavailable")
	}
	return nil
}
func (*testNAT) DeletePortMapping(string, int, int) error { return nil }
func (*testNAT) Type() string                             { return "test" }

func TestAutoMapRetainsFailedRenewal(t *testing.T) {
	device := &testNAT{}
	auto := NewAutoMap(device)
	defer auto.Stop()
	mapping, err := auto.Map("udp", 6881, 6881, "test")
	if err != nil {
		t.Fatal(err)
	}
	key := mappingKey("udp", 6881, 6881)
	auto.mu.Lock()
	auto.mappings[key].Expires = time.Now().Add(-time.Minute)
	auto.mu.Unlock()
	device.fail = true
	auto.refreshMappings()
	if _, ok := auto.GetMapping("udp", 6881, 6881); !ok {
		t.Fatal("failed renewal discarded desired mapping")
	}
	device.fail = false
	auto.refreshMappings()
	renewed, ok := auto.GetMapping("udp", 6881, 6881)
	if !ok || renewed.Expires.Before(time.Now().Add(50*time.Minute)) || device.attempts != 3 {
		t.Fatal("mapping did not recover after gateway became available")
	}
	if !mapping.Expires.Before(renewed.Expires) {
		t.Fatal("caller snapshot was mutated by renewal")
	}
	renewed.Expires = time.Time{}
	if current, _ := auto.GetMapping("udp", 6881, 6881); current.Expires.IsZero() {
		t.Fatal("returned mapping aliases internal state")
	}
}

func TestAutoMapConcurrentStopAndMap(t *testing.T) {
	auto := NewAutoMap(&testNAT{})
	var workers sync.WaitGroup
	for i := 0; i < 20; i++ {
		workers.Add(1)
		go func(port int) {
			defer workers.Done()
			_, _ = auto.Map("udp", port, port, "test")
			auto.Stop()
		}(6881 + i)
	}
	workers.Wait()
	auto.Stop()
	if _, err := auto.Map("udp", 7000, 7000, "test"); err == nil {
		t.Fatal("stopped mapper accepted new mapping")
	}
}
