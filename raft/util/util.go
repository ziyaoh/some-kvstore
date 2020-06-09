package util

import (
	"bytes"

	"github.com/hashicorp/go-msgpack/codec"
)

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
