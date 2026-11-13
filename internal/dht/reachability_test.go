package dht

import (
	"bytes"
	"context"
	"log"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestReachabilityUnsolicited(t *testing.T) {
	check := newReachabilityCheck()
	for _, ip := range []string{"127.0.0.1", "10.0.0.1", "192.168.1.1", "172.16.0.1", "100.64.0.1", "169.254.1.1", "224.0.0.1", "0.0.0.0", "::1", "fc00::1", "fe80::1", "ff02::1"} {
		if check.unsolicited(&net.UDPAddr{IP: net.ParseIP(ip), Port: 6881}) {
			t.Errorf("non-public source %s counted as public ingress", ip)
		}
	}
	addr := &net.UDPAddr{IP: net.ParseIP("8.8.8.8"), Port: 6881}
	if !check.unsolicited(addr) {
		t.Fatal("new public source was ignored")
	}
	check.recordSend(addr)
	addr.Port++
	if check.unsolicited(addr) {
		t.Fatal("previously contacted IP on another port counted as unsolicited")
	}
	if !check.unsolicited(&net.UDPAddr{IP: net.ParseIP("2001:4860:4860::8888"), Port: 6881}) {
		t.Fatal("public IPv6 source was ignored")
	}
}

func TestReachabilityWait(t *testing.T) {
	t.Run("timeout is unknown despite responses", func(t *testing.T) {
		check := newReachabilityCheck()
		check.recordResponse()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		addr, responses := check.wait(ctx)
		if addr != nil || !responses {
			t.Fatal("responses incorrectly proved public ingress")
		}
		check.recordSend(&net.UDPAddr{IP: net.ParseIP("8.8.8.8"), Port: 6881})
		if check.contacted != nil || check.unsolicited(&net.UDPAddr{IP: net.ParseIP("1.1.1.1"), Port: 6881}) {
			t.Fatal("expired check kept collecting sources")
		}
	})
	t.Run("unsolicited request observed", func(t *testing.T) {
		check := newReachabilityCheck()
		addr := &net.UDPAddr{IP: net.ParseIP("8.8.8.8"), Port: 6881}
		check.recordInbound(addr)
		check.recordInbound(addr)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		got, _ := check.wait(ctx)
		if got != addr {
			t.Fatal("inbound evidence lost")
		}
	})
}

func TestReachabilityRequestValidation(t *testing.T) {
	for _, tt := range []struct {
		name      string
		contacted bool
		valid     bool
		want      bool
	}{
		{name: "valid new public source", valid: true, want: true},
		{name: "contacted public source", contacted: true, valid: true},
		{name: "malformed request"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			d, _ := newKRPCTestDHT(t)
			d.conn.Close()
			d.reachability = newReachabilityCheck()
			addr := &net.UDPAddr{IP: net.ParseIP("8.8.8.8"), Port: 6881}
			if tt.contacted {
				d.reachability.recordSend(addr)
			}
			id := strings.Repeat("r", 20)
			if !tt.valid {
				id = "bad"
			}
			handleRequest(d, addr, makeQuery("test", pingType, map[string]interface{}{"id": id}))
			select {
			case <-d.reachability.inbound:
				if !tt.want {
					t.Fatal("invalid or solicited request counted as ingress evidence")
				}
			default:
				if tt.want {
					t.Fatal("valid unsolicited request was not recorded")
				}
			}
		})
	}
}

func TestReachabilityStartupLog(t *testing.T) {
	var output bytes.Buffer
	original := log.Writer()
	log.SetOutput(&output)
	t.Cleanup(func() { log.SetOutput(original) })
	check := newReachabilityCheck()
	check.recordInbound(&net.UDPAddr{IP: net.ParseIP("8.8.8.8"), Port: 6881})
	check.logStartup(6881)
	for _, text := range []string{"Checking public UDP ingress to local port 6881", "public inbound traffic observed", "exact public NAT port is not verified"} {
		if !strings.Contains(output.String(), text) {
			t.Fatalf("startup log missing %q: %s", text, output.String())
		}
	}
}

func TestReachabilityConcurrent(t *testing.T) {
	check := newReachabilityCheck()
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			addr := &net.UDPAddr{IP: net.ParseIP("8.8.8.8"), Port: 6881}
			check.recordSend(addr)
			check.unsolicited(addr)
			check.recordResponse()
			check.recordInbound(addr)
		}()
	}
	cancel()
	check.wait(ctx)
	wg.Wait()
}
