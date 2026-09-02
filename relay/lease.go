package relay

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"time"
)

// NewInstanceID returns hostname plus a random suffix.
func NewInstanceID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "relay"
	}
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("%s-%d-%d", host, os.Getpid(), time.Now().UnixNano())
	}
	return host + "-" + hex.EncodeToString(b[:])
}
