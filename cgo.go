package sqlite

import (
	"runtime"
	"unsafe"
)

func cString(s string) (uintptr, *runtime.Pinner) {
	if s == "" {
		return 0, nil
	}

	pinner := &runtime.Pinner{}
	bytes := append([]byte(s), 0)
	ptr := unsafe.Pointer(&bytes[0])
	pinner.Pin(ptr)

	return uintptr(ptr), pinner
}

func unpin(pinner *runtime.Pinner) {
	if pinner != nil {
		pinner.Unpin()
	}
}

func goString(ptr uintptr) string {
	if ptr == 0 {
		return ""
	}

	var bytes []byte
	maxLen := 1 << 20 // 1MB safety limit
	for i := 0; i < maxLen; i++ {
		b := *(*byte)(unsafe.Pointer(ptr + uintptr(i)))
		if b == 0 {
			break
		}
		bytes = append(bytes, b)
	}
	return string(bytes)
}

func goStringN(ptr uintptr, n int) string {
	if ptr == 0 || n <= 0 {
		return ""
	}

	maxLen := 1 << 20 // 1MB safety limit
	if n > maxLen {
		n = maxLen
	}

	bytes := make([]byte, n)
	for i := 0; i < n; i++ {
		bytes[i] = *(*byte)(unsafe.Pointer(ptr + uintptr(i)))
	}
	return string(bytes)
}

func goBytesN(ptr uintptr, n int) []byte {
	if ptr == 0 || n <= 0 {
		return []byte{}
	}

	maxLen := 1 << 20 // 1MB safety limit
	if n > maxLen {
		n = maxLen
	}

	bytes := make([]byte, n)
	for i := 0; i < n; i++ {
		bytes[i] = *(*byte)(unsafe.Pointer(ptr + uintptr(i)))
	}
	return bytes
}

func allocateBytes(b []byte) (uintptr, *runtime.Pinner) {
	if len(b) == 0 {
		return 0, nil
	}

	pinner := &runtime.Pinner{}
	ptr := unsafe.Pointer(&b[0])
	pinner.Pin(ptr)

	return uintptr(ptr), pinner
}
