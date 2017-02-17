package readers

import (
	"encoding/binary"
	"errors"
	"io"
	"sort"
	"time"
)

type Latency struct {
	start time.Time
	errs  int32

	us []uint16
	ms []uint16
	s  []uint16
}

type DataReader interface {
	io.Reader
	io.ByteReader
}

func ReadLatency(r DataReader) (*Latency, error) {
	var buf [12]byte

	if _, err := r.Read(buf[:3]); err != nil {
		return nil, err
	}

	if buf[0] != 0x99 || buf[1] != 0x01 {
		return nil, errors.New("format is incorrect: wrong magic")
	}

	if buf[2] != 0 {
		return nil, errors.New("cannot handle format versions > 0")
	}

	if _, err := r.Read(buf[:]); err != nil {
		return nil, err
	}

	var res Latency

	sec := binary.LittleEndian.Uint64(buf[:8])
	nsec := binary.LittleEndian.Uint32(buf[8:12])
	res.start = time.Unix(int64(sec), int64(nsec))

	if _, err := r.Read(buf[:4]); err != nil {
		return nil, err
	}
	res.errs = int32(binary.LittleEndian.Uint32(buf[:4]))

	fields := []*[]uint16{&res.us, &res.ms, &res.s}

	for _, field := range fields {
		flen, err := binary.ReadUvarint(r)
		if err != nil {
			return nil, err
		}
		*field = make([]uint16, flen)
		for i := uint64(0); i < flen; i++ {
			if _, err := r.Read(buf[:2]); err != nil {
				return nil, err
			}
			v := binary.LittleEndian.Uint16(buf[:2])
			(*field)[i] = v
		}
	}

	sort.Sort(uint16s(res.us))
	sort.Sort(uint16s(res.ms))
	sort.Sort(uint16s(res.s))

	return &res, nil
}

func (r *Latency) Start() time.Time { return r.start }

func (r *Latency) AllVals() []HistVal {
	total := int(r.errs) + len(r.us) + len(r.ms) + len(r.s)
	slots := len(r.us) + len(r.ms) + len(r.s)
	var i, index int

	if r.errs > 0 {
		slots += 1
		index = int(r.errs)
		i = 1
	}

	vals := make([]HistVal, slots)

	if i == 1 {
		vals[0] = HistVal{Value: -1, Percentile: float64(r.errs) / float64(total)}
	}

	for _, v := range r.us {
		vals[i] = HistVal{Value: time.Duration(v) * time.Microsecond, Percentile: float64(index+1) / float64(total)}
		index++
		i++
	}

	for _, v := range r.ms {
		vals[i] = HistVal{Value: time.Duration(v) * time.Millisecond, Percentile: float64(index+1) / float64(total)}
		index++
		i++
	}

	for _, v := range r.s {
		vals[i] = HistVal{Value: time.Duration(v) * time.Second, Percentile: float64(index+1) / float64(total)}
		index++
		i++
	}

	return vals
}

type HistVal struct {
	Value      time.Duration
	Percentile float64
}

type uint16s []uint16

func (s uint16s) Len() int           { return len(s) }
func (s uint16s) Less(i, j int) bool { return s[i] < s[j] }
func (s uint16s) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
