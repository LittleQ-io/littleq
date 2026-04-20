package uid

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"time"
)

// New returns a 32-character time-ordered hex string:
// 16-char nanosecond timestamp prefix + 16-char random suffix.
// Lexicographic order matches temporal order.
func New() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return fmt.Sprintf("%016x%016x", uint64(time.Now().UnixNano()), binary.BigEndian.Uint64(b[:]))
}
