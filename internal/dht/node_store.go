package dht

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"
)

const (
	verifiedNodeCapacity  = 512
	verifiedNodeSeedLimit = 64
	verifiedNodeDiskLimit = 256
	verifiedNodeReadLimit = 128 << 10
	verifiedNodeMaxAge    = 24 * time.Hour
)

// verifiedNodeRecord contains only observations from successful direct queries.
// ID is encoded as base64 by encoding/json, preserving arbitrary binary IDs.
type verifiedNodeRecord struct {
	ID          []byte        `json:"id"`
	Endpoint    string        `json:"endpoint"`
	LastSuccess time.Time     `json:"last_success"`
	RTT         time.Duration `json:"rtt"`
	Failures    int           `json:"consecutive_failures"`
}

// verifiedNodeStore is independent of the routing table.
// Loading historical successes does not establish current routing-table trust.
// Callers must not mutate an input node concurrently with recordSuccess/Failure.
type verifiedNodeStore struct {
	mu    sync.Mutex
	path  string
	nodes map[string]verifiedNodeRecord
}

func newVerifiedNodeStore(path string) *verifiedNodeStore {
	return &verifiedNodeStore{path: path, nodes: make(map[string]verifiedNodeRecord)}
}

// verifiedNodeEndpoint rejects DNS, scoped addresses, and non-public endpoints.
func verifiedNodeEndpoint(endpoint string) (string, *net.UDPAddr, bool) {
	host, portText, err := net.SplitHostPort(endpoint)
	if err != nil {
		return "", nil, false
	}
	ip := net.ParseIP(host)
	port, err := strconv.Atoi(portText)
	if ip == nil || err != nil || port < 1 || port > 65535 || !ip.IsGlobalUnicast() || ip.IsPrivate() {
		return "", nil, false
	}
	a, ok := netip.AddrFromSlice(ip)
	if !ok {
		return "", nil, false
	}
	a = a.Unmap()
	if !a.Is4() && !netip.MustParsePrefix("2000::/3").Contains(a) {
		return "", nil, false
	}
	for _, prefix := range verifiedNodeReservedPrefixes {
		if prefix.Contains(a) {
			return "", nil, false
		}
	}
	addr := &net.UDPAddr{IP: net.IP(a.AsSlice()), Port: port}
	return addr.String(), addr, true
}

var verifiedNodeReservedPrefixes = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),
	netip.MustParsePrefix("100.64.0.0/10"),
	netip.MustParsePrefix("192.0.0.0/24"),
	netip.MustParsePrefix("192.0.2.0/24"),
	netip.MustParsePrefix("192.88.99.0/24"),
	netip.MustParsePrefix("198.18.0.0/15"),
	netip.MustParsePrefix("198.51.100.0/24"),
	netip.MustParsePrefix("203.0.113.0/24"),
	netip.MustParsePrefix("240.0.0.0/4"),
	netip.MustParsePrefix("2001::/23"),
	netip.MustParsePrefix("2001:db8::/32"),
	netip.MustParsePrefix("2002::/16"),
	netip.MustParsePrefix("3fff::/20"),
}

func verifiedNodeIdentity(no *node) ([]byte, string, bool) {
	if no == nil || no.id == nil || no.id.Size != 160 || len(no.id.data) != 20 || no.addr == nil || no.addr.Zone != "" {
		return nil, "", false
	}
	endpoint, _, ok := verifiedNodeEndpoint(no.addr.String())
	return no.id.data, endpoint, ok
}

func (s *verifiedNodeStore) recordSuccess(no *node, latency time.Duration) {
	id, endpoint, ok := verifiedNodeIdentity(no)
	if !ok {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.nodes[endpoint]
	if !bytes.Equal(r.ID, id) {
		r = verifiedNodeRecord{ID: append([]byte(nil), id...), Endpoint: endpoint}
	}
	if latency > 0 {
		if r.RTT > 0 {
			// EWMA, alpha = 1/4; subtraction avoids duration overflow.
			r.RTT += (latency - r.RTT) / 4
		} else {
			r.RTT = latency
		}
	}
	r.LastSuccess = time.Now()
	r.Failures = 0
	s.nodes[endpoint] = r
	s.trimLocked()
}

func (s *verifiedNodeStore) recordFailure(no *node) {
	id, endpoint, ok := verifiedNodeIdentity(no)
	if !ok {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if r, exists := s.nodes[endpoint]; exists && bytes.Equal(r.ID, id) {
		// Three is terminal until a new success, so saturation is sufficient.
		if r.Failures < 3 {
			r.Failures++
		}
		s.nodes[endpoint] = r
	}
}

func verifiedNodeLess(a, b verifiedNodeRecord) bool {
	if a.Failures != b.Failures {
		return a.Failures < b.Failures
	}
	if !a.LastSuccess.Equal(b.LastSuccess) {
		return a.LastSuccess.After(b.LastSuccess)
	}
	if a.RTT != b.RTT {
		// Unknown RTT is not preferable to a measured RTT.
		return a.RTT > 0 && (b.RTT == 0 || a.RTT < b.RTT)
	}
	return a.Endpoint < b.Endpoint
}

func (s *verifiedNodeStore) trimLocked() {
	if len(s.nodes) <= verifiedNodeCapacity {
		return
	}
	records := make([]verifiedNodeRecord, 0, len(s.nodes))
	for _, r := range s.nodes {
		records = append(records, r)
	}
	sort.Slice(records, func(i, j int) bool { return verifiedNodeLess(records[i], records[j]) })
	for _, r := range records[verifiedNodeCapacity:] {
		delete(s.nodes, r.Endpoint)
	}
}

func verifiedNodeFresh(r verifiedNodeRecord, now time.Time) bool {
	return !r.LastSuccess.IsZero() && !r.LastSuccess.After(now) && now.Sub(r.LastSuccess) < verifiedNodeMaxAge
}

func (s *verifiedNodeStore) candidatesLocked(now time.Time, limit int) []verifiedNodeRecord {
	records := make([]verifiedNodeRecord, 0, len(s.nodes))
	for _, r := range s.nodes {
		if verifiedNodeFresh(r, now) && r.Failures < 3 {
			records = append(records, r)
		}
	}
	sort.Slice(records, func(i, j int) bool { return verifiedNodeLess(records[i], records[j]) })
	if len(records) > limit {
		records = records[:limit]
	}
	return records
}

// seeds returns detached candidates, never live/trusted routing entries. The
// caller must revalidate them and apply its own blacklist before routing use.
func (s *verifiedNodeStore) seeds(now time.Time) []*node {
	s.mu.Lock()
	defer s.mu.Unlock()
	records := s.candidatesLocked(now, verifiedNodeSeedLimit)
	result := make([]*node, 0, len(records))
	for _, r := range records {
		_, addr, ok := verifiedNodeEndpoint(r.Endpoint)
		if ok {
			result = append(result, &node{id: newBitmapFromBytes(r.ID), addr: addr})
		}
	}
	return result
}

// load merges historical successes without replacing newer in-memory success.
// Missing files are a normal first start. Invalid JSON/oversized input returns
// an error without modifying the store; individually invalid records are ignored.
func (s *verifiedNodeStore) load(now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.path == "" {
		return nil
	}
	f, err := os.Open(s.path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, verifiedNodeReadLimit+1))
	if err != nil {
		return err
	}
	if len(data) > verifiedNodeReadLimit {
		return fmt.Errorf("verified node store exceeds %d bytes", verifiedNodeReadLimit)
	}
	var records []json.RawMessage
	if err := json.Unmarshal(data, &records); err != nil {
		return fmt.Errorf("decode verified node store: %w", err)
	}
	if records == nil {
		return fmt.Errorf("decode verified node store: expected an array")
	}
	if len(records) > verifiedNodeDiskLimit {
		return fmt.Errorf("verified node store exceeds %d records", verifiedNodeDiskLimit)
	}
	for _, raw := range records {
		var r verifiedNodeRecord
		if err := json.Unmarshal(raw, &r); err != nil {
			continue
		}
		endpoint, _, ok := verifiedNodeEndpoint(r.Endpoint)
		if !ok || len(r.ID) != 20 || !verifiedNodeFresh(r, now) || r.RTT < 0 || r.Failures < 0 {
			continue
		}
		r.Endpoint = endpoint
		if r.Failures > 3 {
			r.Failures = 3
		}
		if old, exists := s.nodes[endpoint]; !exists || r.LastSuccess.After(old.LastSuccess) {
			s.nodes[endpoint] = r
		}
	}
	s.trimLocked()
	return nil
}

// save serializes with all other store operations, preventing older concurrent
// snapshots from overwriting newer ones. It never creates parent directories.
func (s *verifiedNodeStore) save(now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.path == "" {
		return nil
	}
	data, err := json.Marshal(s.candidatesLocked(now, verifiedNodeDiskLimit))
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(s.path), ".verified-nodes-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	// CreateTemp creates files with mode 0600; replacement never follows a
	// destination symlink or exposes a partially written JSON document.
	if _, err = f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}
