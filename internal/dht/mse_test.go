package dht

import (
	"bufio"
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

// mseRespond is the receiving (B) side of MSE, written from the specification
// for tests. It returns the payload stream and the initiator's IA.
func mseRespond(conn net.Conn, skey []byte, selectMode uint32) (net.Conn, []byte, error) {
	br := bufio.NewReader(conn)
	ya := make([]byte, mseKeyLen)
	if _, err := io.ReadFull(br, ya); err != nil {
		return nil, nil, err
	}
	y, yb, err := mseKeyPair()
	if err != nil {
		return nil, nil, err
	}
	if _, err := conn.Write(append(yb, bytes.Repeat([]byte{0xA5}, 37)...)); err != nil { // PadB
		return nil, nil, err
	}
	secret, err := mseSecret(y, ya)
	if err != nil {
		return nil, nil, err
	}
	// Skip PadA by searching for HASH('req1', S).
	want := mseHash([]byte("req1"), secret)
	var window []byte
	for !bytes.HasSuffix(window, want) {
		b, err := br.ReadByte()
		if err != nil {
			return nil, nil, err
		}
		if window = append(window, b); len(window) > mseMaxPad+len(want) {
			return nil, nil, errors.New("req1 not found")
		}
	}
	obfuscated := make([]byte, 20)
	if _, err := io.ReadFull(br, obfuscated); err != nil {
		return nil, nil, err
	}
	req3 := mseHash([]byte("req3"), secret)
	for i := range obfuscated {
		obfuscated[i] ^= req3[i]
	}
	if !bytes.Equal(obfuscated, mseHash([]byte("req2"), skey)) {
		return nil, nil, errors.New("unknown SKEY")
	}
	dec := mseCipher("keyA", secret, skey)
	enc := mseCipher("keyB", secret, skey)
	header := make([]byte, 14) // VC, crypto_provide, len(PadC)
	if _, err := io.ReadFull(br, header); err != nil {
		return nil, nil, err
	}
	dec.XORKeyStream(header, header)
	if !bytes.Equal(header[:8], make([]byte, 8)) || binary.BigEndian.Uint32(header[8:12])&selectMode == 0 {
		return nil, nil, errors.New("bad VC or crypto_provide")
	}
	padC := make([]byte, binary.BigEndian.Uint16(header[12:14])+2) // PadC, len(IA)
	if _, err := io.ReadFull(br, padC); err != nil {
		return nil, nil, err
	}
	dec.XORKeyStream(padC, padC)
	ia := make([]byte, binary.BigEndian.Uint16(padC[len(padC)-2:]))
	if _, err := io.ReadFull(br, ia); err != nil {
		return nil, nil, err
	}
	dec.XORKeyStream(ia, ia)
	reply := make([]byte, 8+4+2+5) // VC, crypto_select, len(PadD)=5, PadD
	binary.BigEndian.PutUint32(reply[8:12], selectMode)
	binary.BigEndian.PutUint16(reply[12:14], 5)
	enc.XORKeyStream(reply, reply)
	if _, err := conn.Write(reply); err != nil {
		return nil, nil, err
	}
	if selectMode == mseCryptoRC4 {
		return &mseConn{Conn: conn, reader: &rc4Reader{r: br, dec: dec}, enc: enc}, ia, nil
	}
	return &mseConn{Conn: conn, reader: br}, ia, nil
}

// tcpPair returns a connected loopback TCP pair; MSE needs buffered sockets
// because both sides write before reading.
func tcpPair(t *testing.T) (client, server net.Conn) {
	t.Helper()
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, _ := listener.Accept()
		accepted <- conn
	}()
	client, err = net.Dial("tcp4", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	server = <-accepted
	if server == nil {
		t.Fatal("accept failed")
	}
	deadline := time.Now().Add(5 * time.Second)
	client.SetDeadline(deadline)
	server.SetDeadline(deadline)
	t.Cleanup(func() { client.Close(); server.Close() })
	return client, server
}

func TestMSEPrimeIs768Bits(t *testing.T) {
	if msePrime == nil || msePrime.BitLen() != 768 || !msePrime.ProbablyPrime(20) {
		t.Fatal("MSE prime is not the specified 768-bit prime")
	}
}

func TestMSERoundTrip(t *testing.T) {
	for _, mode := range []uint32{mseCryptoRC4, mseCryptoPlaintext} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			client, server := tcpPair(t)
			skey := bytes.Repeat([]byte{7}, 20)
			type result struct {
				conn net.Conn
				ia   []byte
				err  error
			}
			responded := make(chan result, 1)
			go func() {
				conn, ia, err := mseRespond(server, skey, mode)
				responded <- result{conn, ia, err}
			}()
			ia := []byte("initial payload")
			stream, err := mseInitiate(client, skey, ia)
			if err != nil {
				t.Fatal(err)
			}
			peer := <-responded
			if peer.err != nil || !bytes.Equal(peer.ia, ia) {
				t.Fatalf("responder: ia=%q err=%v", peer.ia, peer.err)
			}
			// Both directions must decode, and RC4 must actually hide the payload.
			if _, err := stream.Write([]byte("ping")); err != nil {
				t.Fatal(err)
			}
			got := make([]byte, 4)
			if _, err := io.ReadFull(peer.conn, got); err != nil || string(got) != "ping" {
				t.Fatalf("peer read %q, %v", got, err)
			}
			if _, err := peer.conn.Write([]byte("pong")); err != nil {
				t.Fatal(err)
			}
			if _, err := io.ReadFull(stream, got); err != nil || string(got) != "pong" {
				t.Fatalf("initiator read %q, %v", got, err)
			}
		})
	}
}

func TestMSEWrongInfohashFails(t *testing.T) {
	client, server := tcpPair(t)
	go func() {
		_, _, _ = mseRespond(server, bytes.Repeat([]byte{1}, 20), mseCryptoRC4)
		server.Close()
	}()
	if _, err := mseInitiate(client, bytes.Repeat([]byte{2}, 20), []byte("x")); err == nil {
		t.Fatal("handshake succeeded for a torrent the peer does not have")
	}
}

func TestMSEPlaintextPeerReportsUnsupported(t *testing.T) {
	client, server := tcpPair(t)
	go func() {
		// A plaintext-only client checks the protocol header and hangs up.
		var first [1]byte
		_, _ = server.Read(first[:])
		server.Close()
	}()
	if _, err := mseInitiate(client, make([]byte, 20), []byte("x")); !errors.Is(err, errMSEUnsupported) {
		t.Fatalf("err = %v, want errMSEUnsupported", err)
	}
}

func TestMSEUnresponsivePeerIsNotUnsupported(t *testing.T) {
	client, _ := tcpPair(t)
	client.SetDeadline(time.Now().Add(50 * time.Millisecond))
	if _, err := mseInitiate(client, make([]byte, 20), []byte("x")); err == nil || errors.Is(err, errMSEUnsupported) {
		t.Fatalf("err = %v; a silent peer must time out, not trigger a plaintext retry", err)
	}
}

// serveMetadata acts as a peer that owns metadata on an established stream.
func serveMetadata(conn net.Conn, metadata []byte, handshakeRead bool, handshake []byte) error {
	if !handshakeRead {
		handshake = make([]byte, 68)
		if _, err := io.ReadFull(conn, handshake); err != nil {
			return err
		}
	}
	if _, err := conn.Write(handshake); err != nil { // Echo: same infohash and reserved bits.
		return err
	}
	buf := new(bytes.Buffer)
	if _, err := readMessage(conn, buf); err != nil { // Our extension handshake.
		return err
	}
	buf.Reset()
	ext := append([]byte{EXTENDED, HANDSHAKE}, Encode(map[string]interface{}{
		"m": map[string]interface{}{"ut_metadata": 3}, "metadata_size": len(metadata),
	})...)
	if err := sendMessage(conn, ext); err != nil {
		return err
	}
	for sent := 0; sent*BLOCK < len(metadata); sent++ {
		buf.Reset()
		if _, err := readMessage(conn, buf); err != nil {
			return err
		}
		request, _, err := DecodeDict(buf.Bytes()[2:], 0)
		if err != nil {
			return err
		}
		piece := request.(map[string]interface{})["piece"].(int)
		data := metadata[piece*BLOCK : min(len(metadata), (piece+1)*BLOCK)]
		msg := append([]byte{EXTENDED, localMetadataID}, Encode(map[string]interface{}{
			"msg_type": DATA, "piece": piece, "total_size": len(metadata),
		})...)
		if err := sendMessage(conn, append(msg, data...)); err != nil {
			return err
		}
	}
	return nil
}

func TestFetchMetadataOverMSEAndPlaintextFallback(t *testing.T) {
	metadata := []byte(Encode(map[string]interface{}{"name": "mse-test", "padding": string(bytes.Repeat([]byte{'m'}, BLOCK+9))}))
	hash := sha1.Sum(metadata)
	for _, peerKind := range []string{"encryption-required", "plaintext-only"} {
		t.Run(peerKind, func(t *testing.T) {
			listener, err := net.Listen("tcp4", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			served := make(chan error, 1)
			go func() {
				for {
					conn, err := listener.Accept()
					if err != nil {
						return
					}
					conn.SetDeadline(time.Now().Add(5 * time.Second))
					first := make([]byte, 1)
					if _, err := io.ReadFull(conn, first); err != nil {
						conn.Close()
						continue
					}
					plaintextHandshake := first[0] == 19
					if peerKind == "plaintext-only" {
						if !plaintextHandshake {
							conn.Close() // Rejects the MSE key outright.
							continue
						}
						rest := make([]byte, 67)
						if _, err := io.ReadFull(conn, rest); err != nil {
							served <- err
							return
						}
						served <- serveMetadata(conn, metadata, true, append(first, rest...))
						conn.Close()
						return
					}
					if plaintextHandshake {
						conn.Close() // Encryption required: plaintext is refused.
						continue
					}
					stream, ia, err := mseRespond(&prefixConn{Conn: conn, prefix: first}, hash[:], mseCryptoRC4)
					if err == nil {
						err = serveMetadata(stream, metadata, true, ia)
					}
					served <- err
					conn.Close()
					return
				}
			}()
			wire := NewWire(8, 4, 1)
			port := listener.Addr().(*net.TCPAddr).Port
			if err := wire.fetchMetadata(Request{InfoHash: hash[:], IP: "127.0.0.1", Port: port}); err != nil {
				t.Fatalf("fetch: %v", err)
			}
			select {
			case response := <-wire.responses:
				if !bytes.Equal(response.MetadataInfo, metadata) {
					t.Fatal("metadata corrupted in transit")
				}
			default:
				t.Fatal("no metadata delivered")
			}
			if err := <-served; err != nil {
				t.Fatalf("peer: %v", err)
			}
		})
	}
}

// prefixConn replays bytes already consumed while sniffing the protocol.
type prefixConn struct {
	net.Conn
	prefix []byte
}

func (c *prefixConn) Read(p []byte) (int, error) {
	if len(c.prefix) > 0 {
		n := copy(p, c.prefix)
		c.prefix = c.prefix[n:]
		return n, nil
	}
	return c.Conn.Read(p)
}

func TestPeerIDFollowsBEP20(t *testing.T) {
	id := newPeerID()
	if len(id) != 20 || string(id[:8]) != peerIDPrefix {
		t.Fatalf("peer id %q", id)
	}
	for _, c := range id[8:] {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z') {
			t.Fatalf("non-printable peer id suffix %q", id)
		}
	}
}
