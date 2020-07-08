package util

import (
	"bytes"
	"crypto/sha1"
	"fmt"
	"math/big"
	"net"
	"os"

	"github.com/hashicorp/go-msgpack/codec"
)

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

// DecodeMsgPack reverses the encode operation on a byte slice input
func DecodeMsgPack(buf []byte, out interface{}) error {
	r := bytes.NewBuffer(buf)
	hd := codec.MsgpackHandle{}
	dec := codec.NewDecoder(r, &hd)
	return dec.Decode(out)
}

// EncodeMsgPack writes an encoded object to a new bytes buffer
func EncodeMsgPack(in interface{}) ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	hd := codec.MsgpackHandle{}
	enc := codec.NewEncoder(buf, &hd)
	err := enc.Encode(in)
	return buf.Bytes(), err
}

// AddrToID converts a network address to a Raft node ID of specified length.
func AddrToID(addr string, length int) string {
	h := sha1.New()
	h.Write([]byte(addr))
	v := h.Sum(nil)
	keyInt := big.Int{}
	keyInt.SetBytes(v[:length])
	return keyInt.String()
}

// KeyToShard takes in a key and returns the shard number this key belongs to
// TODO
func KeyToShard(key []byte) int {
	return 0
}
