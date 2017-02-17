package intgen

import "math/rand"

type Switching struct {
	g    Gen
	min  int64 // inclusive
	max  int64 // exclusive
	prob float32
	r    *rand.Rand

	perm []int64
}

func NewSwitching(g Gen, r rand.Source, min, max int64, prob float32) *Switching {
	p := make([]int64, max-min)
	for i := range p {
		p[i] = int64(i)
	}
	return &Switching{
		g:    g,
		min:  min,
		max:  max,
		prob: prob,
		r:    rand.New(r),
		perm: p,
	}
}

func (g *Switching) Next() int64 {
	orig := g.g.Next()
	mapped := g.perm[orig-g.min] + g.min

	if g.r.Float32() < g.prob {
		other := g.r.Intn(len(g.perm))
		g.perm[orig], g.perm[other] = g.perm[other], g.perm[orig]
	}

	return mapped
}
