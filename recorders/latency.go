package recorders

import (
	"bufio"
	"encoding/binary"
	"io"
	"math"
	"sync"
	"time"
)

type Latency struct {
	mu sync.Mutex

	start time.Time
	errs  int32

	// Buckets for storage.
	//
	// Data is stored up until math.MaxUint16
	// These labels are a bit misleading, but this gets us more accuracy
	// and avoids dealing with 3-byte values in encoding.
	us []uint16
	ms []uint16
	s  []uint16
}

// For testing only
func (r *Latency) Start() time.Time  { return r.start }
func (r *Latency) Errs() int32       { return r.errs }
func (r *Latency) Micros() []uint16  { return r.us }
func (r *Latency) Millis() []uint16  { return r.ms }
func (r *Latency) Seconds() []uint16 { return r.s }

func resetSlice(s []uint16, cap int) []uint16 {
	if s == nil {
		return make([]uint16, 0, cap)
	}
	return s[:0]
}

func (r *Latency) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.start = time.Now()
	r.errs = 0

	// allocate large enough to hopefully never require growth
	r.us = resetSlice(r.us, 2048)
	r.ms = resetSlice(r.ms, 8192)
	r.s = resetSlice(r.s, 2048)
}

func (r *Latency) Record(d time.Duration, err error) {
	r.mu.Lock()
	r.mu.Unlock()
	if err != nil {
		r.errs++
		return
	}
	if d <= 0 {
		r.us = append(r.us, 0)
		return
	}
	switch {
	case d/time.Microsecond <= math.MaxUint16:
		r.us = append(r.us, uint16(d/time.Microsecond))
	case d/time.Millisecond <= math.MaxUint16:
		r.ms = append(r.ms, uint16(d/time.Millisecond))
	case d/time.Second <= math.MaxUint16:
		r.s = append(r.s, uint16(d/time.Second))
	default:
		r.s = append(r.s, ^uint16(0))
	}
}

func (r *Latency) WriteTo(w io.Writer) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	bw := bufio.NewWriter(w)
	defer bw.Flush()

	var buf [binary.MaxVarintLen64]byte

	buf[0] = 0x99 // magic
	buf[1] = 0x01 // magic
	buf[2] = 0x00 // version number

	if _, err := bw.Write(buf[:3]); err != nil {
		return err
	}

	binary.LittleEndian.PutUint64(buf[:8], uint64(r.start.Unix()))
	if _, err := bw.Write(buf[:8]); err != nil {
		return err
	}
	binary.LittleEndian.PutUint32(buf[:4], uint32(r.start.Nanosecond()))
	if _, err := bw.Write(buf[:4]); err != nil {
		return err
	}

	binary.LittleEndian.PutUint32(buf[:4], uint32(r.errs))
	if _, err := bw.Write(buf[:4]); err != nil {
		return err
	}

	fields := [][]uint16{r.us, r.ms, r.s}

	for _, field := range fields {
		m := binary.PutUvarint(buf[:], uint64(len(field)))
		if _, err := bw.Write(buf[:m]); err != nil {
			return err
		}

		for _, v := range field {
			binary.LittleEndian.PutUint16(buf[:2], v)
			if _, err := bw.Write(buf[:2]); err != nil {
				return err
			}
		}
	}
	return nil
}
