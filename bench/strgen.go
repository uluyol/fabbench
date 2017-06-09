package bench

import (
	"math/rand"
	"strconv"
	"sync"

	"github.com/uluyol/fabbench/internal/fnv"
	"github.com/uluyol/fabbench/intgen"
)

// stringGen generates strings and is safe for concurrent use
// so long as G is also safe for concurrent use
type stringGen struct {
	G intgen.Gen

	// desired string length, will be ':' padded
	// should be long enough to fit 3 64-bit ints
	// encoded in base36 (i.e. >= 39)
	Len int
}

func (g stringGen) Next(src rand.Source) string {
	return formatKeyName(g.G.Next(src), g.Len)
}

var fmtBufPool = sync.Pool{
	New: func() interface{} { return make([]byte, 1024) },
}

func formatKeyName(v int64, keySize int) string {
	buf := fmtBufPool.Get().([]byte)
	buf = buf[:0]

	buf = strconv.AppendInt(buf, fnv.Hash64(v+0), 36)
	buf = strconv.AppendInt(buf, fnv.Hash64(v+1), 36)
	buf = strconv.AppendInt(buf, fnv.Hash64(v+2), 36)

	oldlen := len(buf)

	if cap(buf) < keySize {
		t := make([]byte, keySize)
		copy(t, buf)
		buf = t
	} else {
		buf = buf[:keySize]
	}
	for i := oldlen; i < keySize; i++ {
		buf[i] = ':'
	}

	ret := string(buf)
	fmtBufPool.Put(buf)

	return ret
}

// valueGen generates values and is safe for concurrent use
type valueGen struct {
	bufPool sync.Pool
}

func newValueGen(size int) *valueGen {
	return &valueGen{
		bufPool: sync.Pool{
			New: func() interface{} { return make([]byte, size) },
		},
	}
}

func (g *valueGen) Next(src rand.Source) string {
	buf := g.bufPool.Get().([]byte)
	sr := smallRand{}
	for i := range buf {
		buf[i] = randStringVals[sr.get(src)]
	}
	s := string(buf)
	g.bufPool.Put(buf)
	return s
}

type smallRand struct {
	cur  int64
	left int
}

func (r *smallRand) get(src rand.Source) int {
	if r.left <= 6 {
		r.cur = src.Int63()
		c.left = 61
	}
	sr := int(r.cur & ((1 << 6) - 1))
	r.cur >>= 6
	r.left -= 6
	return sr
}

// if this is updated, need to update smallRand as well
var randStringVals = []byte("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ-#")

func init() {
	if len(randStringVals) != 64 {
		panic("need to fix randStringVals: incorrect length")
	}
}
