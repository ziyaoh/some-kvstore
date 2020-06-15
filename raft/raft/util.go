package raft

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"net"
	"os"
	"syscall"
	"time"
)

// LowPort is lowest available port on Brown machines
const LowPort int = 32768

// HighPort is highest available port on Brown machines
const HighPort int = 61000

// WinEADDRINUSE to support windows machines
const WinEADDRINUSE = syscall.Errno(10048)

// OpenPort creates a listener on the specified port.
func OpenPort(port int) net.Listener {
	hostname, err := os.Hostname()
	if err != nil {
		panic(err)
	}
	listener, err := net.Listen("tcp4", fmt.Sprintf("%v:%v", hostname, port))
	if err != nil {
		panic(err)
	}
	return listener
}

// randomTimeout uses time.After to create a timeout between minTimeout and 2x that.
func randomTimeout(minTimeout time.Duration) <-chan time.Time {
	random := rand.Int63n(minTimeout.Nanoseconds())
	// fmt.Printf("RANDOM TIMEOUT: %d + %d\n", minTimeout.Nanoseconds(), random)
	return time.After(time.Duration(minTimeout.Nanoseconds() + random))
}

// createCacheID creates a unique ID to store a client request and corresponding
// reply in cache.
func createCacheID(clientID, sequenceNum uint64) string {
	return fmt.Sprintf("%v-%v", clientID, sequenceNum)
}

// Converts bytes to an integer
func bytesToUint64(b []byte) uint64 {
	return binary.LittleEndian.Uint64(b)
}

// Converts a uint to a byte slice
func uint64ToBytes(u uint64) []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, u)
	return buf
}

// SortableUint64Slice is a sortable slice of uint64 type
// had to define this in order to uint64 slice because golang is freaking stupid, especially before 1.8
type SortableUint64Slice []uint64

func (s SortableUint64Slice) Len() int {
	return len(s)
}
func (s SortableUint64Slice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
func (s SortableUint64Slice) Less(i, j int) bool {
	return s[i] < s[j]
}
