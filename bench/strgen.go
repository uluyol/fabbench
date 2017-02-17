package bench

import (
	"math/rand"
	"strconv"
	"sync"

	"github.com/uluyol/fabbench/internal/fnv"
	"github.com/uluyol/fabbench/intgen"
)

// valueGen generates values and is safe for concurrent use
// so long as G is also safe for concurrent use
type stringGen struct {
	G intgen.Gen

	// desired string length, will be ':' padded
	// should be long enough to fit 3 64-bit ints
	// encoded in base36 (i.e. >= 39)
	Len int
}

func (g stringGen) Next() string {
	return formatKeyName(g.G.Next(), g.Len)
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
	mu   sync.Mutex
	buf  []byte
	rand *rand.Rand
}

func newValueGen(src rand.Source, size int) *valueGen {
	return &valueGen{
		buf:  make([]byte, size),
		rand: rand.New(src),
	}
}

func (g *valueGen) Next() string {
	g.mu.Lock()
	defer g.mu.Unlock()
	for i := range g.buf {
		g.buf[i] = randStringVals[g.rand.Intn(len(randStringVals))]
	}
	return string(g.buf)
}

var randStringVals = []byte("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
