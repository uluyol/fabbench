package intgen

import (
	"math/rand"
	"sync/atomic"
)

type Gen interface {
	Next(rng *rand.Rand) int64
}

type Uniform struct {
	n int64
}

func NewUniform(nitems int64) Uniform {
	return Uniform{n: nitems}
}

func (g Uniform) Next(rng *rand.Rand) int64 {
	return rng.Int63n(g.n)
}

type Counter struct {
	Count int64
}

func (g *Counter) Next(_ *rand.Rand) int64 {
	c := atomic.AddInt64(&g.Count, 1)
	c--
	return c
}
