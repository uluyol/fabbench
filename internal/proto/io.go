package proto

import (
	"encoding/binary"
	"io"

	"github.com/golang/protobuf/proto"
)

// This file contains helper io functions compatible with
// Java protobuf's writeDelimitedTo and mergeDelimitedFrom methods.

func WriteDelimitedTo(w io.Writer, m proto.Message) error {
	data, err := proto.Marshal(m)

	var buf [binary.MaxVarintLen64]byte

	n := binary.PutUvarint(buf[:], uint64(len(data)))

	concat := make([]byte, len(data)+n)
	copy(concat, buf[:n])
	copy(concat[n:], data)

	_, err := w.Write(concat)
	return err
}

// TODO: add ReadDelimitedFrom
