package dht

import (
	"bytes"
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"io"
	"net"
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
)

var errWireMessageTooLarge = errors.New("peer wire message too large")

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
	conn.SetReadDeadline(time.Now().Add(time.Second * 15))

	n, err := io.CopyN(data, conn, int64(size))
	if err != nil || n != int64(size) {
		return errors.New("read error")
	}
	return nil
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
	length := int32(len(data))

	buffer := bytes.NewBuffer(nil)
	if err := binary.Write(buffer, binary.BigEndian, length); err != nil {
		return err
	}

	conn.SetWriteDeadline(time.Now().Add(time.Second * 10))
	_, err := conn.Write(append(buffer.Bytes(), data...))
	return err
}

// sendHandshake sends handshake message to conn.
func sendHandshake(conn net.Conn, infoHash, peerID []byte) error {
	data := make([]byte, 68)
	copy(data[:28], handshakePrefix)
	copy(data[28:48], infoHash)
	copy(data[48:], peerID)

	conn.SetWriteDeadline(time.Now().Add(time.Second * 10))
	_, err := conn.Write(data)
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
	requests     chan Request
	responses    chan Response
	workerTokens chan struct{}
}

// NewWire returns a Wire pointer.
//   - blackListSize: the blacklist size
//   - requestQueueSize: the max requests it can buffer
//   - workerQueueSize: the max goroutine downloading workers
func NewWire(blackListSize, requestQueueSize, workerQueueSize int) *Wire {
	return &Wire{
		blackList:    newBlackList(blackListSize),
		queue:        newSyncedMap(),
		requests:     make(chan Request, requestQueueSize),
		responses:    make(chan Response, 1024),
		workerTokens: make(chan struct{}, workerQueueSize),
	}
}

// Request pushes the request to the queue.
func (wire *Wire) Request(infoHash []byte, ip string, port int) {
	if len(infoHash) != 20 || port < 1 || port > 65535 || wire.blackList.in(ip, port) {
		return
	}
	key := string(infoHash) + ":" + genAddress(ip, port)
	wire.queue.Lock()
	defer wire.queue.Unlock()
	if _, exists := wire.queue.data[key]; exists {
		return
	}
	wire.queue.data[key] = struct{}{}
	select {
	case wire.requests <- Request{InfoHash: bytes.Clone(infoHash), IP: ip, Port: port}:
	default:
		delete(wire.queue.data, key)
	}
}

// Response returns a chan of Response.
func (wire *Wire) Response() <-chan Response {
	return wire.responses
}

func (wire *Wire) requestPieces(
	conn net.Conn, utMetadata int, metadataSize int, piecesNum int) {

	buffer := make([]byte, 1024)
	for i := 0; i < piecesNum; i++ {
		buffer[0] = EXTENDED
		buffer[1] = byte(utMetadata)

		msg := Encode(map[string]interface{}{
			"msg_type": REQUEST,
			"piece":    i,
		})

		length := len(msg) + 2
		copy(buffer[2:length], msg)

		if err := sendMessage(conn, buffer[:length]); err != nil {
			return
		}
	}
	buffer = nil
}

// fetchMetadata fetches medata info according to infohash from dht.
func (wire *Wire) fetchMetadata(r Request) {
	deadline := time.Now().Add(metadataDownloadTimeout)
	dialer := net.Dialer{Timeout: 15 * time.Second, Deadline: deadline}
	conn, err := dialer.Dial("tcp", genAddress(r.IP, r.Port))
	if err != nil {
		wire.blackList.insert(r.IP, r.Port)
		return
	}
	conn.(*net.TCPConn).SetLinger(0)
	defer conn.Close()
	wire.fetchMetadataConn(conn, r, deadline)
}

// fetchMetadataConn runs a download with a fixed deadline, including writes.
func (wire *Wire) fetchMetadataConn(raw net.Conn, r Request, deadline time.Time) {
	if err := raw.SetDeadline(deadline); err != nil {
		return
	}
	conn := &metadataConn{Conn: raw, deadline: deadline}
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	var (
		err          error
		length       int
		msgType      byte
		piecesNum    int
		pieces       [][]byte
		utMetadata   int
		metadataSize int
		received     int
	)

	infoHash := r.InfoHash
	data := bytes.NewBuffer(nil)
	data.Grow(BLOCK)

	if sendHandshake(conn, infoHash, []byte(randomString(20))) != nil ||
		read(conn, 68, data) != nil ||
		onHandshake(data.Next(68), infoHash) != nil ||
		sendExtHandshake(conn) != nil {
		return
	}

	for {
		select {
		case <-timer.C:
			return
		default:
		}
		length, err = readMessage(conn, data)
		if err != nil {
			return
		}

		if length == 0 {
			continue
		}

		msgType, err = data.ReadByte()
		if err != nil {
			return
		}

		switch msgType {
		case EXTENDED:
			extendedID, err := data.ReadByte()
			if err != nil {
				return
			}

			payload, err := io.ReadAll(data)
			if err != nil {
				return
			}

			if extendedID == 0 {
				if pieces != nil {
					return
				}

				utMetadata, metadataSize, err = getUTMetaSize(payload)
				if err != nil {
					return
				}

				piecesNum = metadataSize / BLOCK
				if metadataSize%BLOCK != 0 {
					piecesNum++
				}

				pieces = make([][]byte, piecesNum)
				go wire.requestPieces(conn, utMetadata, metadataSize, piecesNum)

				continue
			}

			// Inbound IDs follow our handshake, not the peer's outgoing ID map.
			if extendedID != localMetadataID {
				continue
			}
			if pieces == nil {
				return
			}

			d, index, err := DecodeDict(payload, 0)
			if err != nil {
				return
			}
			dict := d.(map[string]interface{})

			if err = ParseKeys(dict, [][]string{
				{"msg_type", "int"},
				{"piece", "int"}}); err != nil {
				return
			}

			if dict["msg_type"].(int) == REJECT {
				return
			}
			if dict["msg_type"].(int) != DATA {
				continue
			}

			piece := dict["piece"].(int)
			if piece < 0 || piece >= piecesNum {
				return
			}
			if value, ok := dict["total_size"]; ok {
				if total, ok := value.(int); !ok || total != metadataSize {
					return
				}
			}
			pieceLen := length - 2 - index

			expectedLastPieceLen := metadataSize % BLOCK
			if expectedLastPieceLen == 0 {
				expectedLastPieceLen = BLOCK
			}
			if (piece != piecesNum-1 && pieceLen != BLOCK) ||
				(piece == piecesNum-1 && pieceLen != expectedLastPieceLen) {
				return
			}

			if pieces[piece] == nil {
				received++
			}
			pieces[piece] = payload[index:]

			if received == piecesNum {
				metadataInfo := bytes.Join(pieces, nil)

				info := sha1.Sum(metadataInfo)
				if !bytes.Equal(infoHash, info[:]) {
					return
				}

				select {
				case wire.responses <- Response{
					Request:      r,
					MetadataInfo: metadataInfo,
				}:
				case <-timer.C:
				}
				return
			}
		default:
			data.Reset()
		}
	}
}

// Run starts the peer wire protocol.
func (wire *Wire) Run() {
	go wire.blackList.clear()

	for r := range wire.requests {
		key := string(r.InfoHash) + ":" + genAddress(r.IP, r.Port)
		if wire.blackList.in(r.IP, r.Port) {
			wire.queue.Delete(key)
			continue
		}
		wire.workerTokens <- struct{}{}
		go func(r Request, key string) {
			defer func() {
				wire.queue.Delete(key)
				<-wire.workerTokens
			}()
			wire.fetchMetadata(r)
		}(r, key)
	}
}
