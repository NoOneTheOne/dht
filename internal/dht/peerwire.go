package dht

import (
	"bytes"
	"container/list"
	"context"
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	// REQUEST represents request message type
	REQUEST = iota
	// DATA represents data message type
	DATA
	// REJECT represents reject message type
	REJECT
)

const (
	// BLOCK is 2 ^ 14
	BLOCK = 16384
	// MaxMetadataSize represents the max medata it can accept
	MaxMetadataSize = BLOCK * 1000
	// EXTENDED represents it is extended message
	EXTENDED = 20
	// HANDSHAKE represents handshake bit
	HANDSHAKE = 0

	// Allow a metadata piece plus message IDs and 1 KiB of headers.
	maxWireMessageSize      = BLOCK + 2 + 1024
	metadataDownloadTimeout = time.Minute
	localMetadataID         = 1
	maxRequestsPerInfoHash  = 3
	maxMetadataCooldowns    = 65536
)

var (
	errWireMessageTooLarge     = errors.New("peer wire message too large")
	errMetadataProtocol        = errors.New("invalid metadata protocol message")
	errMetadataRejected        = errors.New("metadata request rejected")
	errMetadataHashMismatch    = errors.New("metadata hash mismatch")
	errMetadataResponseDropped = errors.New("metadata response dropped")
)

// metadataStageError preserves errors.Is/As while carrying a bounded stage name.
type metadataStageError struct {
	stage string
	err   error
}

func (err *metadataStageError) Error() string { return err.stage + ": " + err.err.Error() }
func (err *metadataStageError) Unwrap() error { return err.err }

var metadataStageDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "dht_metadata_stage_duration_seconds",
	Help:    "Actual peer metadata stage duration, excluding queue and response delivery waits.",
	Buckets: []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 15, 30, 60},
}, []string{"stage", "result"})

func init() {
	prometheus.MustRegister(metadataStageDuration)
}

func recordMetadataStage(stage string, started time.Time, err error) {
	result := "success"
	if err != nil {
		result = "failure"
	}
	RecordProtocolEvent("metadata_"+stage, result)
	metadataStageDuration.WithLabelValues(stage, result).Observe(time.Since(started).Seconds())
}

func metadataFailureReason(err error) string {
	var stageErr *metadataStageError
	stage := ""
	if errors.As(err, &stageErr) {
		stage = stageErr.stage
	}
	var timeout net.Error
	switch {
	case errors.Is(err, context.Canceled):
		return "stopped"
	case errors.Is(err, errMetadataResponseDropped):
		return "response_backpressure"
	case errors.Is(err, errMetadataRejected):
		return "rejected"
	case errors.Is(err, errMetadataProtocol), errors.Is(err, errWireMessageTooLarge):
		return "protocol_error"
	case errors.Is(err, errMetadataHashMismatch):
		return "hash_mismatch"
	case errors.Is(err, context.DeadlineExceeded) || errors.As(err, &timeout) && timeout.Timeout():
		switch stage {
		case "dial":
			return "dial_timeout"
		case "handshake":
			return "handshake_timeout"
		case "extension":
			return "extension_timeout"
		case "pieces":
			return "piece_timeout"
		}
	}
	if stage == "dial" {
		return "dial_error"
	}
	return "io_error"
}

// Keepalives must not extend the download deadline.
type metadataConn struct {
	net.Conn
	deadline time.Time
}

func (conn *metadataConn) SetReadDeadline(deadline time.Time) error {
	if deadline.IsZero() || deadline.After(conn.deadline) {
		deadline = conn.deadline
	}
	return conn.Conn.SetReadDeadline(deadline)
}

func (conn *metadataConn) SetWriteDeadline(deadline time.Time) error {
	if deadline.IsZero() || deadline.After(conn.deadline) {
		deadline = conn.deadline
	}
	return conn.Conn.SetWriteDeadline(deadline)
}

var handshakePrefix = []byte{
	19, 66, 105, 116, 84, 111, 114, 114, 101, 110, 116, 32, 112, 114,
	111, 116, 111, 99, 111, 108, 0, 0, 0, 0, 0, 16, 0, 1,
}

// read size-length bytes from conn to data.
func read(conn net.Conn, size int, data *bytes.Buffer) error {
	if err := conn.SetReadDeadline(time.Now().Add(time.Second * 15)); err != nil {
		return err
	}
	_, err := io.CopyN(data, conn, int64(size))
	return err
}

// readMessage gets a message from the tcp connection.
func readMessage(conn net.Conn, data *bytes.Buffer) (
	length int, err error) {

	if err = read(conn, 4, data); err != nil {
		return
	}

	frameLength := bytes2int(data.Next(4))
	if frameLength > maxWireMessageSize {
		return 0, errWireMessageTooLarge
	}
	length = int(frameLength)
	if length == 0 {
		return
	}

	if err = read(conn, length, data); err != nil {
		return
	}
	return
}

// sendMessage sends data to the connection.
func sendMessage(conn net.Conn, data []byte) error {
	frame := make([]byte, 4+len(data))
	binary.BigEndian.PutUint32(frame[:4], uint32(len(data)))
	copy(frame[4:], data)
	if err := conn.SetWriteDeadline(time.Now().Add(time.Second * 10)); err != nil {
		return err
	}
	n, err := conn.Write(frame)
	if err == nil && n != len(frame) {
		return io.ErrShortWrite
	}
	return err
}

// sendHandshake sends handshake message to conn.
func sendHandshake(conn net.Conn, infoHash, peerID []byte) error {
	data := make([]byte, 68)
	copy(data[:28], handshakePrefix)
	copy(data[28:48], infoHash)
	copy(data[48:], peerID)

	if err := conn.SetWriteDeadline(time.Now().Add(time.Second * 10)); err != nil {
		return err
	}
	n, err := conn.Write(data)
	if err == nil && n != len(data) {
		return io.ErrShortWrite
	}
	return err
}

// onHandshake handles the handshake response.
func onHandshake(data, infoHash []byte) error {
	if len(data) != 68 || !bytes.Equal(handshakePrefix[:20], data[:20]) ||
		data[25]&0x10 == 0 || !bytes.Equal(data[28:48], infoHash) {
		return errors.New("invalid handshake response")
	}
	return nil
}

// sendExtHandshake requests for the ut_metadata and metadata_size.
func sendExtHandshake(conn net.Conn) error {
	data := append(
		[]byte{EXTENDED, HANDSHAKE},
		Encode(map[string]interface{}{
			"m": map[string]interface{}{"ut_metadata": localMetadataID},
		})...,
	)

	return sendMessage(conn, data)
}

// getUTMetaSize returns the ut_metadata and metadata_size.
func getUTMetaSize(data []byte) (
	utMetadata int, metadataSize int, err error) {

	v, err := Decode(data)
	if err != nil {
		return
	}

	dict, ok := v.(map[string]interface{})
	if !ok {
		err = errors.New("invalid dict")
		return
	}

	if err = ParseKeys(
		dict, [][]string{{"metadata_size", "int"}, {"m", "map"}}); err != nil {
		return
	}

	m := dict["m"].(map[string]interface{})
	if err = ParseKey(m, "ut_metadata", "int"); err != nil {
		return
	}

	utMetadata = m["ut_metadata"].(int)
	metadataSize = dict["metadata_size"].(int)

	if utMetadata <= 0 || utMetadata > 255 {
		err = errors.New("invalid ut_metadata id")
		return
	}
	if metadataSize <= 0 || metadataSize > MaxMetadataSize {
		err = errors.New("invalid metadata_size")
	}
	return
}

// Request represents the request context.
type Request struct {
	InfoHash []byte
	IP       string
	Port     int
}

// Response contains the request context and the metadata info.
type Response struct {
	Request
	MetadataInfo []byte
}

// MetadataFailure reports an accepted request that did not produce a Response.
// Shutdown may discard feedback; consumers must not retry the "stopped" reason.
type MetadataFailure struct {
	Request
	Reason string
}

type metadataCooldown struct {
	key     string
	expires time.Time
}

// Wire represents the wire protocol.
type Wire struct {
	blackList    *blackList
	queue        *syncedMap
	infoHashes   map[string]int // Queued and active requests, guarded by queue's lock.
	requests     chan Request
	responses    chan Response
	failures     chan MetadataFailure
	cooldowns    map[string]*list.Element // Guarded by queue's lock.
	cooldownFIFO *list.List               // Bounded insertion order, not expiration order.
	workerTokens chan struct{}
	stopOnce     sync.Once
	stop         chan struct{}
	stopped      chan struct{}
	workers      sync.WaitGroup
	connMu       sync.Mutex
	connections  map[net.Conn]struct{}
	ctx          context.Context
	cancel       context.CancelFunc
}

// NewWire returns a Wire pointer.
//   - blackListSize: the blacklist size
//   - requestQueueSize: the max requests it can buffer
//   - workerQueueSize: the max goroutine downloading workers
func NewWire(blackListSize, requestQueueSize, workerQueueSize int) *Wire {
	ctx, cancel := context.WithCancel(context.Background())
	return &Wire{
		ctx:          ctx,
		cancel:       cancel,
		blackList:    newBlackList(blackListSize),
		queue:        newSyncedMap(),
		infoHashes:   make(map[string]int),
		requests:     make(chan Request, requestQueueSize),
		responses:    make(chan Response, 1024),
		failures:     make(chan MetadataFailure, max(1, requestQueueSize)),
		cooldowns:    make(map[string]*list.Element),
		cooldownFIFO: list.New(),
		workerTokens: make(chan struct{}, workerQueueSize),
		stop:         make(chan struct{}),
		stopped:      make(chan struct{}),
		connections:  make(map[net.Conn]struct{}),
	}
}

// Request pushes the request to the queue.
func (wire *Wire) Request(infoHash []byte, ip string, port int) (accepted bool) {
	reason := "invalid_endpoint"
	defer func() {
		if !accepted {
			RecordMetadataRequestDropped()
			RecordProtocolEvent("metadata_queue", reason)
		}
	}()
	if len(infoHash) != 20 || port < 1 || port > 65535 {
		return false
	}
	parsedIP := net.ParseIP(ip)
	if parsedIP == nil || parsedIP.IsUnspecified() || parsedIP.IsMulticast() ||
		parsedIP.IsLinkLocalUnicast() {
		return false
	}
	ip = parsedIP.String()
	if wire.blackList.in(ip, port) {
		reason = "blacklisted"
		return false
	}
	hashKey := string(infoHash)
	key := hashKey + ":" + genAddress(ip, port)
	wire.queue.Lock()
	defer wire.queue.Unlock()
	select {
	case <-wire.stop:
		reason = "stopped"
		return false
	default:
	}
	if wire.inFailureCooldownLocked(Request{InfoHash: infoHash, IP: ip, Port: port}, time.Now()) {
		reason = "cooldown"
		return false
	}
	if wire.infoHashes[hashKey] >= maxRequestsPerInfoHash {
		reason = "hash_limit"
		return false
	}
	if _, exists := wire.queue.data[key]; exists {
		reason = "duplicate"
		return false
	}
	wire.queue.data[key] = struct{}{}
	wire.infoHashes[hashKey]++
	select {
	case wire.requests <- Request{InfoHash: bytes.Clone(infoHash), IP: ip, Port: port}:
		return true
	default:
		delete(wire.queue.data, key)
		wire.infoHashes[hashKey]--
		if wire.infoHashes[hashKey] == 0 {
			delete(wire.infoHashes, hashKey)
		}
		reason = "full"
		return false
	}
}

// releaseRequest forgets an endpoint and its hash reservation after completion or discard.
func (wire *Wire) releaseRequest(r Request) {
	hashKey := string(r.InfoHash)
	key := hashKey + ":" + genAddress(r.IP, r.Port)
	wire.queue.Lock()
	defer wire.queue.Unlock()
	if _, exists := wire.queue.data[key]; !exists {
		return
	}
	delete(wire.queue.data, key)
	wire.infoHashes[hashKey]--
	if wire.infoHashes[hashKey] == 0 {
		delete(wire.infoHashes, hashKey)
	}
}

func (wire *Wire) ConnectedPeers() int {
	wire.connMu.Lock()
	defer wire.connMu.Unlock()
	return len(wire.connections)
}

func (wire *Wire) QueueStats() (requests, responses, active int) {
	return len(wire.requests), len(wire.responses), len(wire.workerTokens)
}

// Response returns a chan of Response.
func (wire *Wire) Response() <-chan Response {
	return wire.responses
}

// Failures returns bounded, reliable completion feedback until Stop. Callers
// must drain it alongside Response; a full channel backpressures workers.
// Run closes it after all workers exit. Shutdown may discard pending feedback.
func (wire *Wire) Failures() <-chan MetadataFailure {
	return wire.failures
}

func (wire *Wire) sendFailure(r Request, reason string) {
	select {
	case wire.failures <- MetadataFailure{Request: r, Reason: reason}:
	case <-wire.stop:
	}
}

// Cooldowns use distinct endpoint and hash+endpoint keys. Lookups lazily expire
// only the requested keys; insertion evicts the oldest entry at capacity in O(1).
func (wire *Wire) inFailureCooldownLocked(r Request, now time.Time) bool {
	endpoint := genAddress(r.IP, r.Port)
	for _, key := range []string{"endpoint:" + endpoint, "hash:" + string(r.InfoHash) + ":" + endpoint} {
		if item := wire.cooldowns[key]; item != nil {
			if now.Before(item.Value.(metadataCooldown).expires) {
				return true
			}
			delete(wire.cooldowns, key)
			wire.cooldownFIFO.Remove(item)
		}
	}
	return false
}

func (wire *Wire) inFailureCooldown(r Request) bool {
	wire.queue.Lock()
	defer wire.queue.Unlock()
	return wire.inFailureCooldownLocked(r, time.Now())
}

func (wire *Wire) coolFailure(r Request, reason string) {
	endpoint := genAddress(r.IP, r.Port)
	key, duration := "endpoint:"+endpoint, time.Minute
	switch reason {
	case "dial_timeout", "dial_error":
	case "rejected", "protocol_error":
		key = "hash:" + string(r.InfoHash) + ":" + endpoint
	case "hash_mismatch":
		duration = 5 * time.Minute
	default:
		return
	}
	wire.queue.Lock()
	defer wire.queue.Unlock()
	expires := time.Now().Add(duration)
	if item := wire.cooldowns[key]; item != nil {
		// A later short dial failure must not shorten a hash-mismatch cooldown.
		if item.Value.(metadataCooldown).expires.After(expires) {
			return
		}
		item.Value = metadataCooldown{key: key, expires: expires}
		wire.cooldownFIFO.MoveToBack(item)
		return
	}
	if len(wire.cooldowns) >= maxMetadataCooldowns {
		oldest := wire.cooldownFIFO.Front()
		delete(wire.cooldowns, oldest.Value.(metadataCooldown).key)
		wire.cooldownFIFO.Remove(oldest)
	}
	wire.cooldowns[key] = wire.cooldownFIFO.PushBack(metadataCooldown{key: key, expires: expires})
}

func (wire *Wire) requestPieces(conn net.Conn, utMetadata int, requests <-chan int) error {
	for piece := range requests {
		msg := append([]byte{EXTENDED, byte(utMetadata)}, Encode(map[string]interface{}{
			"msg_type": REQUEST, "piece": piece,
		})...)
		if err := sendMessage(conn, msg); err != nil {
			return err
		}
	}
	return nil
}

// fetchMetadata records exactly one aggregate outcome for an actual download.
func (wire *Wire) fetchMetadata(r Request) (result error) {
	defer func() {
		if result != nil && wire.ctx.Err() != nil {
			result = context.Canceled
		}
		recordMetadataError(result)
	}()
	started := time.Now()
	deadline := started.Add(metadataDownloadTimeout)
	dialer := net.Dialer{Timeout: 15 * time.Second, Deadline: deadline}
	conn, err := dialer.DialContext(wire.ctx, "tcp", genAddress(r.IP, r.Port))
	recordMetadataStage("dial", started, err)
	if err != nil {
		return &metadataStageError{stage: "dial", err: err}
	}
	wire.connMu.Lock()
	if wire.ctx.Err() != nil {
		wire.connMu.Unlock()
		_ = conn.Close()
		return context.Canceled
	}
	wire.connections[conn] = struct{}{}
	wire.connMu.Unlock()
	defer func() {
		wire.connMu.Lock()
		delete(wire.connections, conn)
		wire.connMu.Unlock()
		_ = conn.Close()
	}()
	conn.(*net.TCPConn).SetLinger(0)
	return wire.fetchMetadataConn(conn, r, deadline)
}

// fetchMetadataConn runs a download with a fixed deadline, including writes.
func (wire *Wire) fetchMetadataConn(raw net.Conn, r Request, deadline time.Time) (result error) {
	stage, started := "handshake", time.Now()
	finishStage := func(err error) {
		recordMetadataStage(stage, started, err)
	}
	defer func() {
		if stage != "" {
			finishStage(result)
			if result != nil {
				result = &metadataStageError{stage: stage, err: result}
			}
		}
	}()
	requests := make(chan int, 8)
	var writerDone chan struct{}
	var writerErr error
	defer func() {
		_ = raw.Close()
		close(requests)
		if writerDone != nil {
			<-writerDone
			if errors.Is(result, net.ErrClosed) && writerErr != nil {
				result = writerErr
			}
		}
	}()
	if err := raw.SetDeadline(deadline); err != nil {
		return err
	}
	conn := &metadataConn{Conn: raw, deadline: deadline}
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	data := new(bytes.Buffer)
	data.Grow(BLOCK)
	if err := sendHandshake(conn, r.InfoHash, []byte(randomString(20))); err != nil {
		return err
	}
	if err := read(conn, 68, data); err != nil {
		return err
	}
	if err := onHandshake(data.Next(68), r.InfoHash); err != nil {
		return errMetadataProtocol
	}
	finishStage(nil)
	stage, started = "extension", time.Now()
	if err := sendExtHandshake(conn); err != nil {
		return err
	}
	var pieces [][]byte
	var metadataSize, utMetadata, nextPiece, received int
	for {
		if !time.Now().Before(deadline) {
			return context.DeadlineExceeded
		}
		length, err := readMessage(conn, data)
		if err != nil {
			return err
		}
		if length == 0 {
			continue
		}
		msgType, _ := data.ReadByte()
		if msgType != EXTENDED {
			data.Reset()
			continue
		}
		extendedID, err := data.ReadByte()
		if err != nil {
			return errMetadataProtocol
		}
		payload := bytes.Clone(data.Bytes())
		data.Reset()
		if extendedID == HANDSHAKE {
			id, size, err := getUTMetaSize(payload)
			if err != nil {
				return errMetadataProtocol
			}
			if pieces != nil {
				if id != utMetadata || size != metadataSize {
					return errMetadataProtocol
				}
				continue
			}
			finishStage(nil)
			stage, started = "pieces", time.Now()
			utMetadata, metadataSize = id, size
			pieces = make([][]byte, (size+BLOCK-1)/BLOCK)
			writerDone = make(chan struct{})
			go func() {
				defer close(writerDone)
				writerErr = wire.requestPieces(conn, id, requests)
				if writerErr != nil {
					_ = raw.Close()
				}
			}()
			for nextPiece < min(8, len(pieces)) {
				requests <- nextPiece
				nextPiece++
			}
			continue
		}
		if extendedID != localMetadataID {
			continue
		}
		if pieces == nil {
			return errMetadataProtocol
		}
		d, index, err := DecodeDict(payload, 0)
		if err != nil {
			return errMetadataProtocol
		}
		dict := d.(map[string]interface{})
		if err := ParseKeys(dict, [][]string{{"msg_type", "int"}, {"piece", "int"}}); err != nil {
			return errMetadataProtocol
		}
		piece := dict["piece"].(int)
		if piece < 0 || piece >= nextPiece {
			return errMetadataProtocol
		}
		if dict["msg_type"].(int) == REJECT {
			return errMetadataRejected
		}
		if dict["msg_type"].(int) != DATA {
			continue
		}
		if value, ok := dict["total_size"]; ok {
			if total, ok := value.(int); !ok || total != metadataSize {
				return errMetadataProtocol
			}
		}
		expected := min(BLOCK, metadataSize-piece*BLOCK)
		if len(payload)-index != expected {
			return errMetadataProtocol
		}
		if pieces[piece] != nil {
			if !bytes.Equal(pieces[piece], payload[index:]) {
				return errMetadataProtocol
			}
			continue
		}
		pieces[piece] = payload[index:]
		received++
		if nextPiece < len(pieces) {
			select {
			case requests <- nextPiece:
				nextPiece++
			case <-writerDone:
				return writerErr
			case <-wire.stop:
				return context.Canceled
			case <-timer.C:
				return context.DeadlineExceeded
			}
		}
		if received != len(pieces) {
			continue
		}
		metadataInfo := bytes.Join(pieces, nil)
		hash := sha1.Sum(metadataInfo)
		if !bytes.Equal(r.InfoHash, hash[:]) {
			return errMetadataHashMismatch
		}
		finishStage(nil)
		stage = "" // Response delivery is not peer download time.
		select {
		case wire.responses <- Response{Request: r, MetadataInfo: metadataInfo}:
			return nil
		case <-timer.C:
			RecordMetadataResponseDropped()
			return errMetadataResponseDropped
		case <-wire.stop:
			RecordMetadataResponseDropped()
			return context.Canceled
		}
	}
}

func recordMetadataError(err error) {
	reason := "io_error"
	var timeout net.Error
	switch {
	case err == nil:
		reason = "success"
	case errors.Is(err, context.Canceled):
		reason = "stopped"
	case errors.Is(err, context.DeadlineExceeded) || errors.As(err, &timeout) && timeout.Timeout():
		reason = "timeout"
	case errors.Is(err, errMetadataProtocol), errors.Is(err, errWireMessageTooLarge):
		reason = "protocol_error"
	case errors.Is(err, errMetadataRejected):
		reason = "rejected"
	case errors.Is(err, errMetadataHashMismatch):
		reason = "hash_mismatch"
	case errors.Is(err, errMetadataResponseDropped):
		reason = "response_backpressure"
	}
	RecordProtocolEvent("metadata", reason)
}

// Run starts the peer wire protocol.
func (wire *Wire) Run() {
	maintenanceDone := make(chan struct{})
	go func() {
		defer close(maintenanceDone)
		wire.blackList.clear(wire.stop)
	}()
	defer func() {
		wire.stopOnce.Do(func() { wire.cancel(); close(wire.stop) })
		wire.workers.Wait()
		wire.queue.Lock()
		for {
			select {
			case _, ok := <-wire.requests:
				if !ok {
					goto drained
				}
				RecordMetadataRequestDropped()
				RecordProtocolEvent("metadata_queue", "stopped")
			default:
				goto drained
			}
		}
	drained:
		clear(wire.queue.data)
		clear(wire.infoHashes)
		clear(wire.cooldowns)
		wire.cooldownFIFO.Init()
		wire.queue.Unlock()
		<-maintenanceDone
		close(wire.responses)
		close(wire.failures)
		close(wire.stopped)
	}()
	for {
		select {
		case <-wire.stop:
			return
		case r, ok := <-wire.requests:
			if !ok {
				return
			}
			select {
			case wire.workerTokens <- struct{}{}:
			case <-wire.stop:
				RecordMetadataRequestDropped()
				wire.releaseRequest(r)
				return
			}
			wire.workers.Add(1)
			go func(r Request) {
				defer wire.workers.Done()
				released := false
				release := func() {
					if !released {
						wire.releaseRequest(r)
						released = true
					}
				}
				defer func() {
					release()
					<-wire.workerTokens
				}()
				if wire.ctx.Err() != nil {
					return
				}
				if wire.blackList.in(r.IP, r.Port) || wire.inFailureCooldown(r) {
					RecordMetadataRequestDropped()
					RecordProtocolEvent("metadata_queue", "blacklisted")
					release()
					wire.sendFailure(r, "blacklisted")
					return
				}
				if err := wire.fetchMetadata(r); err != nil {
					reason := metadataFailureReason(err)
					if reason != "stopped" {
						wire.coolFailure(r, reason)
					}
					// Release before feedback so a consumer may immediately reschedule.
					release()
					wire.sendFailure(r, reason)
				}
			}(r)
		}
	}
}

func (wire *Wire) Stop() {
	wire.stopOnce.Do(func() {
		wire.connMu.Lock()
		wire.queue.Lock()
		wire.cancel()
		close(wire.stop)
		wire.queue.Unlock()
		for conn := range wire.connections {
			_ = conn.Close()
		}
		wire.connMu.Unlock()
	})
	<-wire.stopped
}
