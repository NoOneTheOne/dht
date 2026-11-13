package dht

import (
	"bytes"
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"
)

type discardWireConn struct {
	net.Conn
}

func (discardWireConn) Write(data []byte) (int, error) {
	return len(data), nil
}

func (discardWireConn) SetWriteDeadline(time.Time) error {
	return nil
}

func BenchmarkSendMessage(b *testing.B) {
	for _, size := range []int{32, BLOCK} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			payload := make([]byte, size)
			conn := discardWireConn{}
			b.ReportAllocs()
			for b.Loop() {
				if err := sendMessage(conn, payload); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

type shortWireConn struct {
	discardWireConn
}

func (shortWireConn) Write(data []byte) (int, error) {
	return len(data) - 1, nil
}

func TestSendMessageShortWrite(t *testing.T) {
	if err := sendMessage(shortWireConn{}, []byte("payload")); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("short write = %v", err)
	}
}

func TestSendMessageFrame(t *testing.T) {
	for _, size := range []int{0, 32, BLOCK} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			client, peer := net.Pipe()
			defer client.Close()
			defer peer.Close()
			payload := bytes.Repeat([]byte{'x'}, size)
			done := make(chan error, 1)
			go func() { done <- sendMessage(client, payload) }()
			if err := peer.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
				t.Fatal(err)
			}
			frame := make([]byte, 4+size)
			if _, err := io.ReadFull(peer, frame); err != nil {
				t.Fatal(err)
			}
			if binary.BigEndian.Uint32(frame[:4]) != uint32(size) || !bytes.Equal(frame[4:], payload) {
				t.Fatal("wire frame changed")
			}
			if err := <-done; err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPeerWireMetadataBounds(t *testing.T) {
	for _, tc := range []struct{ id, size int }{
		{0, 1}, {-1, 1}, {256, 1}, {1, 0}, {1, -1}, {1, MaxMetadataSize + 1},
	} {
		payload := []byte(Encode(map[string]interface{}{
			"m": map[string]interface{}{"ut_metadata": tc.id}, "metadata_size": tc.size,
		}))
		if _, _, err := getUTMetaSize(payload); err == nil {
			t.Fatalf("accepted id=%d size=%d", tc.id, tc.size)
		}
	}
	for _, id := range []int{1, 255} {
		payload := []byte(Encode(map[string]interface{}{
			"m": map[string]interface{}{"ut_metadata": id}, "metadata_size": MaxMetadataSize,
		}))
		if _, _, err := getUTMetaSize(payload); err != nil {
			t.Fatal(err)
		}
	}
}

func TestPeerWireHandshakeHash(t *testing.T) {
	hash := bytes.Repeat([]byte{'h'}, 20)
	data := make([]byte, 68)
	copy(data, handshakePrefix)
	copy(data[28:48], hash)
	if err := onHandshake(data, hash); err != nil {
		t.Fatal(err)
	}
	if err := onHandshake(data[:20], hash); err == nil {
		t.Fatal("short handshake accepted")
	}
	data[28] ^= 1
	if err := onHandshake(data, hash); err == nil {
		t.Fatal("wrong hash accepted")
	}
}

func TestPeerWireRejectsInvalidPieces(t *testing.T) {
	for _, tc := range []struct {
		name  string
		piece int
		total interface{}
	}{
		{"negative", -1, BLOCK}, {"out-of-range", 1, BLOCK},
		{"wrong-total", 0, BLOCK + 1}, {"wrong-total-type", 0, "size"},
		{"rejected", 0, BLOCK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, peer := net.Pipe()
			defer client.Close()
			defer peer.Close()
			wire := NewWire(8, 1, 1)
			done := make(chan struct{})
			go func() {
				wire.fetchMetadataConn(client, Request{InfoHash: make([]byte, 20)}, time.Now().Add(time.Second))
				close(done)
			}()
			if err := peerWireTestHandshake(peer); err != nil {
				t.Fatal(err)
			}
			handshake := append([]byte{EXTENDED, HANDSHAKE}, Encode(map[string]interface{}{
				"m": map[string]interface{}{"ut_metadata": 7}, "metadata_size": BLOCK,
			})...)
			if err := sendMessage(peer, handshake); err != nil {
				t.Fatal(err)
			}
			if _, err := readMessage(peer, new(bytes.Buffer)); err != nil {
				t.Fatal(err)
			}
			messageType := DATA
			if tc.name == "rejected" {
				messageType = REJECT
			}
			payload := append([]byte{EXTENDED, localMetadataID}, Encode(map[string]interface{}{
				"msg_type": messageType, "piece": tc.piece, "total_size": tc.total,
			})...)
			payload = append(payload, make([]byte, BLOCK)...)
			if err := sendMessage(peer, payload); err != nil {
				t.Fatal(err)
			}
			select {
			case <-done:
			case <-time.After(300 * time.Millisecond):
				t.Fatal("invalid piece did not stop download")
			}
			if len(wire.responses) != 0 {
				t.Fatal("invalid piece delivered")
			}
		})
	}
}

func TestPeerWireRequestCopiesHash(t *testing.T) {
	wire := NewWire(8, 1, 1)
	hash := bytes.Repeat([]byte{'h'}, 20)
	for _, port := range []int{-1, 0, 65536} {
		wire.Request(hash, "127.0.0.1", port)
	}
	if len(wire.requests) != 0 || wire.queue.Len() != 0 {
		t.Fatal("invalid endpoint reserved queue space")
	}
	wire.Request(hash, "127.0.0.1", 1)
	hash[0] = 'x'
	request := <-wire.requests
	if request.InfoHash[0] != 'h' {
		t.Fatal("queued request aliases caller memory")
	}
}

func TestPeerWireRequestValidatesIP(t *testing.T) {
	for _, ip := range []string{
		"", "localhost", "example.com", "127.0.0.1:6881", "[::1]", "fe80::1%lo0",
		"999.1.1.1", "2001:db8:::1", "0.0.0.0", "::", "::ffff:0.0.0.0",
		"224.0.0.1", "239.255.255.250", "ff02::1", "169.254.1.1", "fe80::1",
	} {
		t.Run(ip, func(t *testing.T) {
			wire := NewWire(8, 1, 1)
			wire.Request(make([]byte, 20), ip, 6881)
			if len(wire.requests) != 0 || wire.queue.Len() != 0 || len(wire.infoHashes) != 0 {
				t.Fatal("invalid IP reserved request capacity")
			}
		})
	}
	for _, ip := range []string{"127.0.0.1", "::1", "192.168.1.1", "10.0.0.1", "172.16.0.1", "fd00::1", "2001:4860:4860::8888"} {
		t.Run(ip, func(t *testing.T) {
			wire := NewWire(8, 1, 1)
			wire.Request(make([]byte, 20), ip, 6881)
			if len(wire.requests) != 1 {
				t.Fatal("valid local or global IP rejected")
			}
			if r := <-wire.requests; r.IP != net.ParseIP(ip).String() {
				t.Fatalf("queued IP = %q", r.IP)
			}
		})
	}
}

func TestPeerWireRequestCanonicalizesIP(t *testing.T) {
	for _, tc := range []struct {
		first, second, want string
	}{
		{"::ffff:127.0.0.1", "127.0.0.1", "127.0.0.1"},
		{"2001:0DB8:0000:0000:0000:0000:0000:0001", "2001:db8::1", "2001:db8::1"},
	} {
		t.Run(tc.want, func(t *testing.T) {
			wire := NewWire(8, 8, 1)
			hash := make([]byte, 20)
			wire.Request(hash, tc.first, 6881)
			wire.Request(hash, tc.second, 6881)
			if len(wire.requests) != 1 || wire.queue.Len() != 1 || wire.infoHashes[string(hash)] != 1 {
				t.Fatal("equivalent IP spellings were not deduplicated")
			}
			r := <-wire.requests
			if r.IP != tc.want {
				t.Fatalf("queued IP = %q, want %q", r.IP, tc.want)
			}
			wire.releaseRequest(r)
			wire.blackList.insert(tc.want, 6881)
			wire.Request(hash, tc.first, 6881)
			if len(wire.requests) != 0 || wire.queue.Len() != 0 || len(wire.infoHashes) != 0 {
				t.Fatal("noncanonical IP spelling bypassed blacklist")
			}
		})
	}
}

func TestPeerWireRejectsOversizedFrameBeforePayload(t *testing.T) {
	for _, size := range []uint32{maxWireMessageSize + 1, ^uint32(0)} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			client, peer := net.Pipe()
			defer client.Close()
			defer peer.Close()
			written := make(chan error, 1)
			go func() {
				var prefix [4]byte
				binary.BigEndian.PutUint32(prefix[:], size)
				_, err := peer.Write(prefix[:]) // Deliberately never send a body.
				written <- err
			}()
			buffer := new(bytes.Buffer)
			result := make(chan error, 1)
			go func() {
				_, err := readMessage(client, buffer)
				result <- err
			}()
			select {
			case err := <-result:
				if !errors.Is(err, errWireMessageTooLarge) {
					t.Fatalf("readMessage error = %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("readMessage waited for an oversized body")
			}
			if err := <-written; err != nil {
				t.Fatal(err)
			}
			if buffer.Len() != 0 || buffer.Cap() > 1024 {
				t.Fatalf("oversized frame grew buffer: len=%d cap=%d", buffer.Len(), buffer.Cap())
			}
		})
	}
}

func TestPeerWireAcceptsMaximumFrame(t *testing.T) {
	client, peer := net.Pipe()
	defer client.Close()
	defer peer.Close()
	payload := bytes.Repeat([]byte{'x'}, maxWireMessageSize)
	written := make(chan error, 1)
	go func() { written <- sendMessage(peer, payload) }()
	buffer := new(bytes.Buffer)
	length, err := readMessage(client, buffer)
	if err != nil || length != len(payload) || !bytes.Equal(buffer.Bytes(), payload) {
		t.Fatalf("maximum frame: length=%d err=%v", length, err)
	}
	if err := <-written; err != nil {
		t.Fatal(err)
	}
}

// Complete both handshakes without talking to any external peer.
func peerWireTestHandshake(peer net.Conn) error {
	if err := peer.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		return err
	}
	var handshake [68]byte
	if _, err := io.ReadFull(peer, handshake[:]); err != nil {
		return err
	}
	if _, err := peer.Write(handshake[:]); err != nil {
		return err
	}
	var prefix [4]byte
	if _, err := io.ReadFull(peer, prefix[:]); err != nil {
		return err
	}
	length := binary.BigEndian.Uint32(prefix[:])
	if length > maxWireMessageSize {
		return fmt.Errorf("unexpected handshake length %d", length)
	}
	_, err := io.CopyN(io.Discard, peer, int64(length))
	return err
}

func TestPeerWireKeepalivesDoNotExtendDownloadDeadline(t *testing.T) {
	client, peer := net.Pipe()
	defer client.Close()
	defer peer.Close()
	wire := NewWire(8, 1, 1)
	deadline := time.Now().Add(100 * time.Millisecond)
	done := make(chan struct{})
	go func() {
		wire.fetchMetadataConn(client, Request{InfoHash: make([]byte, 20)}, deadline)
		close(done)
	}()
	if err := peerWireTestHandshake(peer); err != nil {
		t.Fatal(err)
	}
	count := make(chan int, 1)
	go func() {
		var keepalive [4]byte
		n := 0
		for {
			if _, err := peer.Write(keepalive[:]); err != nil {
				count <- n
				return
			}
			n++
		}
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("keepalives extended the total download deadline")
	}
	client.Close()
	if n := <-count; n < 2 {
		t.Fatalf("only %d keepalives delivered", n)
	}
	if time.Now().Before(deadline) {
		t.Fatal("download stopped before its deadline")
	}
}

func TestPeerWireMetadataFlowAndHash(t *testing.T) {
	metadata := []byte(Encode(map[string]interface{}{
		"name":    "local-test",
		"padding": string(bytes.Repeat([]byte{'x'}, BLOCK)),
	}))
	for _, validHash := range []bool{true, false} {
		t.Run(fmt.Sprintf("validHash=%t", validHash), func(t *testing.T) {
			client, peer := net.Pipe()
			defer client.Close()
			defer peer.Close()
			wire := NewWire(8, 1, 1)
			hash := sha1.Sum(metadata)
			if !validHash {
				hash[0] ^= 1
			}
			request := Request{InfoHash: hash[:], IP: "127.0.0.1", Port: 1234}
			done := make(chan struct{})
			go func() {
				wire.fetchMetadataConn(client, request, time.Now().Add(2*time.Second))
				close(done)
			}()
			if err := peerWireTestHandshake(peer); err != nil {
				t.Fatal(err)
			}
			extHandshake := append([]byte{EXTENDED, HANDSHAKE}, Encode(map[string]interface{}{
				"m":             map[string]interface{}{"ut_metadata": 7},
				"metadata_size": len(metadata),
			})...)
			if err := sendMessage(peer, extHandshake); err != nil {
				t.Fatal(err)
			}
			pieces := (len(metadata) + BLOCK - 1) / BLOCK
			for piece := 0; piece < pieces; piece++ {
				buffer := new(bytes.Buffer)
				if _, err := readMessage(peer, buffer); err != nil {
					t.Fatal(err)
				}
				want := append([]byte{EXTENDED, 7}, Encode(map[string]interface{}{
					"msg_type": REQUEST, "piece": piece,
				})...)
				if !bytes.Equal(buffer.Bytes(), want) {
					t.Fatalf("unexpected piece request: %q", buffer.Bytes())
				}
			}
			for _, extensionID := range []byte{2, 7} {
				if err := sendMessage(peer, []byte{EXTENDED, extensionID, 'x'}); err != nil {
					t.Fatal(err)
				}
			}
			// Send the short last piece first, then a full-sized piece.
			for piece := pieces - 1; piece >= 0; piece-- {
				end := (piece + 1) * BLOCK
				if end > len(metadata) {
					end = len(metadata)
				}
				payload := append([]byte{EXTENDED, 1}, Encode(map[string]interface{}{
					"msg_type": DATA, "piece": piece, "total_size": len(metadata),
				})...)
				payload = append(payload, metadata[piece*BLOCK:end]...)
				if err := sendMessage(peer, payload); err != nil {
					t.Fatal(err)
				}
				if piece == pieces-1 {
					if err := sendMessage(peer, payload); err != nil {
						t.Fatal(err)
					}
				}
			}
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("metadata flow did not finish")
			}
			select {
			case response := <-wire.responses:
				if !validHash {
					t.Fatal("accepted metadata with incorrect hash")
				}
				if !bytes.Equal(response.MetadataInfo, metadata) ||
					!bytes.Equal(response.InfoHash, request.InfoHash) ||
					response.IP != request.IP || response.Port != request.Port {
					t.Fatal("metadata or request changed")
				}
			default:
				if validHash {
					t.Fatal("valid metadata was not delivered")
				}
			}
		})
	}
}
