package intgen

import (
	"math/rand"
	"sync/atomic"
)

type Gen interface {
	Next(src rand.Source) int64
}

type Uniform struct {
	n int64
}

func NewUniform(nitems int64) Uniform {
	return Uniform{n: nitems}
}

func (g Uniform) Next(src rand.Source) int64 {
	if g.n&(g.n-1) == 0 { // n is power of two, can mask
		return src.Int63() & (g.n - 1)
	}
	return src.Int63() % g.n
}

type Counter struct {
	Count int64
}

func (g *Counter) Next(_ rand.Source) int64 {
	c := atomic.AddInt64(&g.Count, 1)
	c--
	return c
}
