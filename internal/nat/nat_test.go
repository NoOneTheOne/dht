package nat

import (
	"encoding/binary"
	"encoding/xml"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/huin/goupnp"
	"github.com/huin/goupnp/dcps/internetgateway2"
	"github.com/huin/goupnp/soap"
	natpmp "github.com/jackpal/go-nat-pmp"
)

type testNAT struct {
	mu       sync.Mutex
	attempts int
	fail     bool
	external int
	lease    time.Duration
}

func (*testNAT) GetExternalAddress() (net.IP, error) { return net.IPv4(203, 0, 113, 1), nil }
func (n *testNAT) AddPortMapping(protocol string, external, internal int, _ string, lease time.Duration) (int, time.Duration, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.attempts++
	if n.fail {
		return 0, 0, errors.New("gateway unavailable")
	}
	if n.external != 0 {
		external = n.external
	}
	if n.lease != 0 {
		lease = n.lease
	}
	return external, lease, nil
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
	device.mu.Lock()
	device.fail = true
	device.mu.Unlock()
	auto.refreshMappings()
	if _, ok := auto.GetMapping("udp", 6881, 6881); !ok {
		t.Fatal("failed renewal discarded desired mapping")
	}
	device.mu.Lock()
	device.fail = false
	device.mu.Unlock()
	auto.refreshMappings() // Still inside the backoff window.
	device.mu.Lock()
	attempts := device.attempts
	device.mu.Unlock()
	if attempts != 2 {
		t.Fatalf("renewal retried during backoff: %d attempts", attempts)
	}
	auto.mu.Lock()
	if m := auto.mappings[key]; m.failures != 1 || !m.retryAt.After(time.Now()) {
		t.Fatalf("missing backoff after failure: %+v", m)
	}
	auto.mappings[key].retryAt = time.Now().Add(-time.Second)
	auto.mu.Unlock()
	auto.refreshMappings()
	renewed, ok := auto.GetMapping("udp", 6881, 6881)
	device.mu.Lock()
	attempts = device.attempts
	device.mu.Unlock()
	if !ok || renewed.Expires.Before(time.Now().Add(50*time.Minute)) || attempts != 3 || renewed.failures != 0 {
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

func TestUPnPSOAPMappingArguments(t *testing.T) {
	for _, serviceType := range []string{"WANIPConnection", "WANPPPConnection"} {
		for _, protocol := range []string{"udp", "tcp"} {
			t.Run(serviceType+"/"+protocol, func(t *testing.T) {
				type argument struct {
					XMLName xml.Name
					Value   string `xml:",chardata"`
				}
				type action struct {
					XMLName xml.Name
					Args    []argument `xml:",any"`
				}
				requests := make(chan action, 2)
				namespace := "urn:schemas-upnp-org:service:" + serviceType + ":1"
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					var envelope struct {
						Body struct {
							Action action `xml:",any"`
						} `xml:"Body"`
					}
					if err := xml.NewDecoder(r.Body).Decode(&envelope); err != nil {
						t.Errorf("decode SOAP request: %v", err)
						w.WriteHeader(http.StatusBadRequest)
						return
					}
					// XMLName is required to capture the action and argument element names.
					requests <- envelope.Body.Action
					actionName := strings.TrimSuffix(strings.TrimPrefix(r.Header.Get("SOAPAction"), `"`+namespace+"#"), `"`)
					w.Header().Set("Content-Type", "text/xml")
					fmt.Fprintf(w, `<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/"><s:Body><u:%sResponse xmlns:u="%s"/></s:Body></s:Envelope>`, actionName, namespace)
				}))
				defer server.Close()
				location, err := url.Parse(server.URL)
				if err != nil {
					t.Fatal(err)
				}
				localIP, err := localIPv4ForGateway(location)
				if err != nil || !localIP.Equal(net.IPv4(127, 0, 0, 1)) {
					t.Fatalf("gateway route local IP = %v, err = %v", localIP, err)
				}
				client := goupnp.ServiceClient{SOAPClient: soap.NewSOAPClient(*location), Location: location}
				device := &UPnPNAT{localIP: localIP}
				if serviceType == "WANIPConnection" {
					device.ipService = &internetgateway2.WANIPConnection1{ServiceClient: client}
				} else {
					device.pppService = &internetgateway2.WANPPPConnection1{ServiceClient: client}
				}
				port, lease, err := device.AddPortMapping(protocol, 45000, 6881, "test & mapping", 120*time.Second)
				if err != nil || port != 45000 || lease != 120*time.Second {
					t.Fatalf("AddPortMapping = %d, %v, %v", port, lease, err)
				}
				add := <-requests
				if add.XMLName.Local != "AddPortMapping" || add.XMLName.Space != namespace {
					t.Fatalf("unexpected SOAP action: %v", add.XMLName)
				}
				args := make(map[string]string)
				for _, arg := range add.Args {
					args[arg.XMLName.Local] = arg.Value
				}
				for name, want := range map[string]string{
					"NewRemoteHost": "", "NewExternalPort": "45000", "NewProtocol": strings.ToUpper(protocol),
					"NewInternalPort": "6881", "NewInternalClient": "127.0.0.1", "NewEnabled": "1",
					"NewPortMappingDescription": "test & mapping", "NewLeaseDuration": "120",
				} {
					if got, ok := args[name]; !ok || got != want {
						t.Errorf("SOAP %s = %q (present %v), want %q", name, got, ok, want)
					}
				}
				if err := device.DeletePortMapping(protocol, port, 6881); err != nil {
					t.Fatal(err)
				}
				del := <-requests
				if del.XMLName.Local != "DeletePortMapping" || del.XMLName.Space != namespace {
					t.Fatalf("unexpected SOAP action: %v", del.XMLName)
				}
				args = make(map[string]string)
				for _, arg := range del.Args {
					args[arg.XMLName.Local] = arg.Value
				}
				if args["NewProtocol"] != strings.ToUpper(protocol) || args["NewExternalPort"] != "45000" {
					t.Fatalf("unexpected delete arguments: %v", args)
				}
				device.localIP = nil
				if _, _, err := device.AddPortMapping(protocol, 45000, 6881, "invalid", time.Hour); err == nil {
					t.Fatal("UPnP accepted a missing local IP")
				}
			})
		}
	}
}

func TestLocalIPv4ForGatewayRejectsInvalidLocation(t *testing.T) {
	for _, location := range []*url.URL{nil, {}, {Scheme: "http", Host: "[::1]:80"}} {
		if ip, err := localIPv4ForGateway(location); err == nil {
			t.Errorf("unexpected IPv4 %v for location %v", ip, location)
		}
	}
}

// The dependency fixes the NAT-PMP port at 5351; bind only loopback, never a real gateway.
func serveNATPMP(t *testing.T, respond func([]byte) []byte) *NATPMP {
	t.Helper()
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 5351})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		buf := make([]byte, 64)
		for {
			n, addr, err := conn.ReadFromUDP(buf)
			if err != nil {
				if !errors.Is(err, net.ErrClosed) {
					t.Errorf("read NAT-PMP request: %v", err)
				}
				return
			}
			if _, err := conn.WriteToUDP(respond(buf[:n]), addr); err != nil {
				t.Errorf("write NAT-PMP response: %v", err)
				return
			}
		}
	}()
	t.Cleanup(func() {
		conn.Close()
		<-done
	})
	return &NATPMP{client: natpmp.NewClientWithTimeout(net.IPv4(127, 0, 0, 1), time.Second)}
}

func natpmpMappingResponse(request []byte, external uint16, lease uint32) []byte {
	response := make([]byte, 16)
	response[1] = request[1] | 0x80
	copy(response[8:10], request[4:6])
	binary.BigEndian.PutUint16(response[10:12], external)
	binary.BigEndian.PutUint32(response[12:16], lease)
	return response
}

func TestNATPMPMappingResponse(t *testing.T) {
	for _, tc := range []struct {
		name          string
		external      uint16
		lease         uint32
		code          uint16
		wrongInternal bool
		wantErr       bool
	}{
		{name: "actual port and short lease", external: 45000, lease: 120},
		{name: "zero port", lease: 120, wantErr: true},
		{name: "zero lease", external: 45000, wantErr: true},
		{name: "wrong internal port", external: 45000, lease: 120, wrongInternal: true, wantErr: true},
		{name: "router error", code: 2, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			requests := make(chan []byte, 1)
			device := serveNATPMP(t, func(request []byte) []byte {
				requests <- append([]byte(nil), request...)
				response := natpmpMappingResponse(request, tc.external, tc.lease)
				binary.BigEndian.PutUint16(response[2:4], tc.code)
				if tc.wrongInternal {
					binary.BigEndian.PutUint16(response[8:10], 9999)
				}
				return response
			})
			port, lease, err := device.AddPortMapping("udp", 6881, 6881, "test", time.Hour)
			if (err != nil) != tc.wantErr {
				t.Fatalf("AddPortMapping error = %v, want error %v", err, tc.wantErr)
			}
			if err == nil && (port != int(tc.external) || lease != time.Duration(tc.lease)*time.Second) {
				t.Fatalf("AddPortMapping = %d, %v; want %d, %ds", port, lease, tc.external, tc.lease)
			}
			request := <-requests
			if len(request) != 12 || request[1] != 1 || binary.BigEndian.Uint16(request[4:6]) != 6881 ||
				binary.BigEndian.Uint16(request[6:8]) != 6881 || binary.BigEndian.Uint32(request[8:12]) != 3600 {
				t.Fatalf("unexpected NAT-PMP request: %v", request)
			}
		})
	}
}

func TestAutoMapNATPMPActualEndpointAndRenewal(t *testing.T) {
	requests := make(chan []byte, 10)
	mappingRequests := 0
	device := serveNATPMP(t, func(request []byte) []byte {
		if request[1] == 0 {
			response := make([]byte, 12)
			response[1] = 0x80
			copy(response[8:12], []byte{203, 0, 113, 1})
			return response
		}
		requests <- append([]byte(nil), request...)
		mappingRequests++
		if mappingRequests == 1 {
			return natpmpMappingResponse(request, 45000, 120)
		}
		if mappingRequests == 3 {
			return natpmpMappingResponse(request, 0, 120)
		}
		return natpmpMappingResponse(request, 46000, 60)
	})
	auto := NewAutoMap(device)
	defer auto.Stop()
	before := time.Now()
	mapping, err := auto.Map("udp", 6881, 6881, "test")
	if err != nil {
		t.Fatal(err)
	}
	<-requests
	if mapping.External != 45000 || !mapping.ExternalIP.Equal(net.IPv4(203, 0, 113, 1)) ||
		mapping.Expires.Before(before.Add(120*time.Second)) || mapping.Expires.After(time.Now().Add(120*time.Second)) {
		t.Fatalf("mapping does not contain actual endpoint/lease: %+v", mapping)
	}
	if same, err := auto.Map("udp", 6881, 6881, "test"); err != nil || same.External != 45000 {
		t.Fatalf("requested-key Map lookup = %+v, %v", same, err)
	}
	key := mappingKey("udp", 6881, 6881)
	auto.mu.Lock()
	auto.mappings[key].Expires = time.Now().Add(90 * time.Second)
	auto.mu.Unlock()
	auto.refreshMappings()
	select {
	case request := <-requests:
		t.Fatalf("renewed before half of lease elapsed: %v", request)
	default:
	}
	auto.mu.Lock()
	auto.mappings[key].Expires = time.Now().Add(50 * time.Second)
	auto.mu.Unlock()
	before = time.Now()
	auto.refreshMappings()
	request := <-requests
	if binary.BigEndian.Uint16(request[6:8]) != 45000 || binary.BigEndian.Uint32(request[8:12]) != 120 {
		t.Fatalf("renewal did not use actual port/lease: %v", request)
	}
	renewed, ok := auto.GetMapping("udp", 6881, 6881)
	if !ok || renewed.External != 46000 || renewed.Expires.Before(before.Add(60*time.Second)) ||
		renewed.Expires.After(time.Now().Add(60*time.Second)) {
		t.Fatalf("renewal did not update endpoint/lease: %+v", renewed)
	}
	if all := auto.GetAllMappings(); len(all) != 1 || all[0].External != 46000 {
		t.Fatalf("GetAllMappings returned stale endpoint: %+v", all)
	}
	if mapping.External != 45000 {
		t.Fatal("renewal changed caller's snapshot")
	}
	auto.mu.Lock()
	failedExpiry := time.Now().Add(20 * time.Second)
	auto.mappings[key].Expires = failedExpiry
	auto.mu.Unlock()
	auto.refreshMappings()
	<-requests
	if current, _ := auto.GetMapping("udp", 6881, 6881); current.External != 46000 || !current.Expires.Equal(failedExpiry) {
		t.Fatalf("invalid renewal was reported as successful: %+v", current)
	}
	if err := auto.Unmap("udp", 6881, 6881); err != nil {
		t.Fatal(err)
	}
	request = <-requests
	if binary.BigEndian.Uint16(request[6:8]) != 46000 || binary.BigEndian.Uint32(request[8:12]) != 0 {
		t.Fatalf("delete did not use renewed external port: %v", request)
	}
	if _, ok := auto.GetMapping("udp", 6881, 6881); ok {
		t.Fatal("deleted mapping remains present")
	}
}

func TestAutoMapRefreshLoopRenewsShortLeaseBeforeExpiry(t *testing.T) {
	device := &testNAT{external: 45000, lease: 2 * time.Second}
	auto := NewAutoMap(device)
	defer auto.Stop()
	mapping, err := auto.Map("udp", 6881, 6881, "test")
	if err != nil {
		t.Fatal(err)
	}
	// Wake only when the real loop can have renewed; the timeout is the original expiry.
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.NewTimer(time.Until(mapping.Expires))
	defer deadline.Stop()
	for {
		select {
		case <-ticker.C:
			current, _ := auto.GetMapping("udp", 6881, 6881)
			if current.Expires.After(mapping.Expires) {
				if time.Now().After(mapping.Expires) {
					t.Fatal("short lease renewed after expiry")
				}
				return
			}
		case <-deadline.C:
			t.Fatal("refresh loop did not renew short lease before expiry")
		}
	}
}
