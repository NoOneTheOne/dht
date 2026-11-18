package dht

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"crypto/rc4"
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"io"
	"math/big"
	mrand "math/rand/v2"
	"net"
	"sync"
)

// Message Stream Encryption (MSE/PE), the obfuscation handshake that most
// BitTorrent clients support and many are configured to require.
// Spec: https://wiki.vuze.com/w/Message_Stream_Encryption
//
// A peer that requires encryption reads our 68-byte plaintext handshake as
// the start of a 96-byte Diffie-Hellman key and waits for the rest, so
// without MSE those peers only ever produce handshake timeouts.

const (
	mseKeyLen          = 96
	mseMaxPad          = 512
	mseCryptoPlaintext = 0x01
	mseCryptoRC4       = 0x02
)

var (
	// 768-bit MODP prime from the MSE specification; the generator is 2.
	msePrime, _ = new(big.Int).SetString(
		"FFFFFFFFFFFFFFFFC90FDAA22168C234C4C6628B80DC1CD129024E088A67CC74"+
			"020BBEA63B139B22514A08798E3404DDEF9519B3CD3A431B302B0A6DF25F1437"+
			"4FE1356D6D51C245E485B576625E7EC6F44C42E9A63A36210000000000090563", 16)
	mseGenerator = big.NewInt(2)

	// errMSEUnsupported means the peer closed the connection instead of
	// answering our key, which is how plaintext-only clients react.
	errMSEUnsupported = errors.New("peer does not support encrypted handshakes")
	errMSEProtocol    = errors.New("invalid encrypted handshake")
)

// mseKeyPair returns a private exponent and its 96-byte public key.
func mseKeyPair() (*big.Int, []byte, error) {
	secret := make([]byte, 20) // 160-bit exponent, as the specification recommends.
	if _, err := rand.Read(secret); err != nil {
		return nil, nil, err
	}
	x := new(big.Int).SetBytes(secret)
	return x, msePad(new(big.Int).Exp(mseGenerator, x, msePrime)), nil
}

// msePad renders n as a fixed-width 96-byte big-endian integer.
func msePad(n *big.Int) []byte {
	out := make([]byte, mseKeyLen)
	n.FillBytes(out)
	return out
}

// mseSecret returns the shared secret S, rejecting degenerate public keys.
func mseSecret(x *big.Int, peerKey []byte) ([]byte, error) {
	y := new(big.Int).SetBytes(peerKey)
	limit := new(big.Int).Sub(msePrime, big.NewInt(1))
	if y.Cmp(big.NewInt(1)) <= 0 || y.Cmp(limit) >= 0 {
		return nil, errMSEProtocol
	}
	return msePad(new(big.Int).Exp(y, x, msePrime)), nil
}

func mseHash(parts ...[]byte) []byte {
	h := sha1.New()
	for _, part := range parts {
		h.Write(part)
	}
	return h.Sum(nil)
}

// mseCipher returns an RC4 stream keyed by HASH(name, S, SKEY) with the
// first 1024 bytes discarded, as the specification requires.
func mseCipher(name string, secret, skey []byte) *rc4.Cipher {
	c, _ := rc4.NewCipher(mseHash([]byte(name), secret, skey)) // 20-byte keys are always valid.
	discard := make([]byte, 1024)
	c.XORKeyStream(discard, discard)
	return c
}

func msePadding() []byte {
	pad := make([]byte, mrand.IntN(mseMaxPad/4)) // Short pads keep the handshake to one segment.
	_, _ = rand.Read(pad)
	return pad
}

// mseConn carries the payload stream after the handshake. A nil cipher means
// the peer selected plaintext for the payload.
type mseConn struct {
	net.Conn
	reader io.Reader
	wmu    sync.Mutex
	enc    *rc4.Cipher
}

func (c *mseConn) Read(p []byte) (int, error) { return c.reader.Read(p) }

func (c *mseConn) Write(p []byte) (int, error) {
	if c.enc == nil {
		return c.Conn.Write(p)
	}
	buf := make([]byte, len(p))
	c.wmu.Lock()
	defer c.wmu.Unlock()
	c.enc.XORKeyStream(buf, p)
	return c.Conn.Write(buf)
}

type rc4Reader struct {
	r   io.Reader
	dec *rc4.Cipher
}

func (r *rc4Reader) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	r.dec.XORKeyStream(p[:n], p[:n])
	return n, err
}

// mseInitiate runs the initiator side of MSE over conn for the torrent skey
// (its infohash) and sends ia, our BitTorrent handshake, as the initial
// payload. The caller's deadlines bound every read and write.
func mseInitiate(conn net.Conn, skey, ia []byte) (net.Conn, error) {
	x, ya, err := mseKeyPair()
	if err != nil {
		return nil, err
	}
	if _, err := conn.Write(append(ya, msePadding()...)); err != nil {
		return nil, err
	}

	br := bufio.NewReaderSize(conn, mseKeyLen+mseMaxPad+64)
	yb := make([]byte, mseKeyLen)
	if _, err := io.ReadFull(br, yb); err != nil {
		var netErr net.Error
		if errors.As(err, &netErr) && netErr.Timeout() {
			return nil, err
		}
		// A plaintext-only client rejects our key and closes the connection.
		return nil, errors.Join(errMSEUnsupported, err)
	}
	secret, err := mseSecret(x, yb)
	if err != nil {
		return nil, err
	}

	enc := mseCipher("keyA", secret, skey)
	dec := mseCipher("keyB", secret, skey)
	// The peer's encrypted VC (eight zero bytes) equals the first eight bytes
	// of its keystream; generating them also advances dec past the VC.
	vc := make([]byte, 8)
	dec.XORKeyStream(vc, vc)

	var msg bytes.Buffer
	msg.Write(mseHash([]byte("req1"), secret))
	req2, req3 := mseHash([]byte("req2"), skey), mseHash([]byte("req3"), secret)
	for i := range req2 {
		req2[i] ^= req3[i]
	}
	msg.Write(req2)
	plain := make([]byte, 8+4+2+2+len(ia)) // VC, crypto_provide, len(PadC)=0, len(IA), IA
	binary.BigEndian.PutUint32(plain[8:12], mseCryptoPlaintext|mseCryptoRC4)
	binary.BigEndian.PutUint16(plain[14:16], uint16(len(ia)))
	copy(plain[16:], ia)
	enc.XORKeyStream(plain, plain)
	msg.Write(plain)
	if _, err := conn.Write(msg.Bytes()); err != nil {
		return nil, err
	}

	// Resynchronise on the VC: it follows the peer's PadB of up to 512 bytes.
	window := make([]byte, 0, mseMaxPad+len(vc))
	for {
		b, err := br.ReadByte()
		if err != nil {
			return nil, err
		}
		window = append(window, b)
		if len(window) >= len(vc) && bytes.Equal(window[len(window)-len(vc):], vc) {
			break
		}
		if len(window) == cap(window) {
			return nil, errMSEProtocol
		}
	}
	header := make([]byte, 6) // crypto_select, len(PadD)
	if _, err := io.ReadFull(br, header); err != nil {
		return nil, err
	}
	dec.XORKeyStream(header, header)
	selected := binary.BigEndian.Uint32(header[:4])
	padLen := int(binary.BigEndian.Uint16(header[4:]))
	if padLen > mseMaxPad {
		return nil, errMSEProtocol
	}
	pad := make([]byte, padLen)
	if _, err := io.ReadFull(br, pad); err != nil {
		return nil, err
	}
	dec.XORKeyStream(pad, pad)

	switch selected {
	case mseCryptoRC4:
		return &mseConn{Conn: conn, reader: &rc4Reader{r: br, dec: dec}, enc: enc}, nil
	case mseCryptoPlaintext:
		return &mseConn{Conn: conn, reader: br}, nil
	default:
		return nil, errMSEProtocol
	}
}
