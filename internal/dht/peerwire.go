package dht

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"time"
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
)

var (
	errWireMessageTooLarge     = errors.New("peer wire message too large")
	errMetadataProtocol        = errors.New("invalid metadata protocol message")
	errMetadataRejected        = errors.New("metadata request rejected")
	errMetadataHashMismatch    = errors.New("metadata hash mismatch")
	errMetadataResponseDropped = errors.New("metadata response dropped")
)

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

// Wire represents the wire protocol.
type Wire struct {
	blackList    *blackList
	queue        *syncedMap
	infoHashes   map[string]int // Queued and active requests, guarded by queue's lock.
	requests     chan Request
	responses    chan Response
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

func (wire *Wire) QueueStats() (requests, responses, active int) {
	return len(wire.requests), len(wire.responses), len(wire.workerTokens)
}

// Response returns a chan of Response.
func (wire *Wire) Response() <-chan Response {
	return wire.responses
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

// fetchMetadata fetches medata info according to infohash from dht.
func (wire *Wire) fetchMetadata(r Request) {
	deadline := time.Now().Add(metadataDownloadTimeout)
	dialer := net.Dialer{Timeout: 15 * time.Second, Deadline: deadline}
	conn, err := dialer.DialContext(wire.ctx, "tcp", genAddress(r.IP, r.Port))
	if err != nil {
		if wire.ctx.Err() != nil {
			RecordProtocolEvent("metadata", "stopped")
		} else {
			wire.blackList.insert(r.IP, r.Port)
			recordMetadataError(err)
		}
		return
	}
	wire.connMu.Lock()
	if wire.ctx.Err() != nil {
		wire.connMu.Unlock()
		_ = conn.Close()
		RecordProtocolEvent("metadata", "stopped")
		return
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
	err = wire.fetchMetadataConn(conn, r, deadline)
	if wire.ctx.Err() != nil {
		RecordProtocolEvent("metadata", "stopped")
	} else {
		recordMetadataError(err)
	}
}

// fetchMetadataConn runs a download with a fixed deadline, including writes.
func (wire *Wire) fetchMetadataConn(raw net.Conn, r Request, deadline time.Time) (result error) {
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
		wire.queue.Unlock()
		<-maintenanceDone
		close(wire.responses)
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
			if wire.blackList.in(r.IP, r.Port) {
				wire.releaseRequest(r)
				continue
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
				defer func() {
					wire.releaseRequest(r)
					<-wire.workerTokens
				}()
				wire.fetchMetadata(r)
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
