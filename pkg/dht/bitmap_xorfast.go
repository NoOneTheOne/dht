//go:build amd64 || 386
// +build amd64 386

package dht

import "unsafe"

const wordSize = int(unsafe.Sizeof(uintptr(0)))

func xor(dst, a, b []byte) {
	n := len(a)

	// Check alignment before using word-sized operations.
	// unsafe.Pointer to slice data may not be aligned to wordSize.
	aAligned := uintptr(unsafe.Pointer(&a[0]))%uintptr(wordSize) == 0
	bAligned := uintptr(unsafe.Pointer(&b[0]))%uintptr(wordSize) == 0
	dAligned := uintptr(unsafe.Pointer(&dst[0]))%uintptr(wordSize) == 0

	if aAligned && bAligned && dAligned {
		w := n / wordSize
		if w > 0 {
			dw := *(*[]uintptr)(unsafe.Pointer(&dst))
			aw := *(*[]uintptr)(unsafe.Pointer(&a))
			bw := *(*[]uintptr)(unsafe.Pointer(&b))
			for i := 0; i < w; i++ {
				dw[i] = aw[i] ^ bw[i]
			}
		}

		for i := n - n%wordSize; i < n; i++ {
			dst[i] = a[i] ^ b[i]
		}
	} else {
		// Fallback to byte-wise XOR for unaligned data
		for i := 0; i < n; i++ {
			dst[i] = a[i] ^ b[i]
		}
	}
}
