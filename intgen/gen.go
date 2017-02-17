package intgen

import (
	"fmt"
	"math/rand"
	"sync"
)

type Gen interface {
	Next() int64
}

type DistGen interface {
	Gen
	ProbOf(n int64) float64
}

type Sync struct {
	mu sync.Mutex
	g  Gen
}

func NewSync(g Gen) Gen {
	return &Sync{g: g}
}

func (g *Sync) Next() int64 {
	g.mu.Lock()
	v := g.g.Next()
	g.mu.Unlock()
	return v
}

type Uniform struct {
	n    int64
	rand *rand.Rand
}

func NewUniform(randSrc rand.Source, nitems int64) Uniform {
	return Uniform{
		n:    nitems,
		rand: rand.New(randSrc),
	}
}

func (g Uniform) Next() int64 {
	return g.rand.Int63n(g.n)
}

func (g Uniform) ProbOf(n int64) float64 {
	return 1 / float64(g.n)
}

type Counter struct {
	Count int64
}

func (g *Counter) Next() int64 {
	c := g.Count
	g.Count++
	return c
}

type ObservingDistGen struct {
	G Gen
	N int64

	hits  []int64
	total int64
}

func (g *ObservingDistGen) Next() int64 {
	if g.hits == nil {
		g.hits = make([]int64, g.N)
		if _, ok := g.G.(DistGen); ok {
			panic("generator is already DistGen")
		}
	}
	n := g.G.Next()
	g.total++
	g.hits[n]++
	return n
}

func (g *ObservingDistGen) ProbOf(n int64) float64 {
	if n < 0 || int(n) >= len(g.hits) {
		panic(fmt.Errorf("out of bounds [0, %d): %d", len(g.hits), n))
	}
	return float64(g.hits[n]) / float64(g.total)
}
