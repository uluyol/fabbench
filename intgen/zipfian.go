package intgen

import (
	"fmt"
	"math"
	"math/rand"
	"runtime"

	"github.com/uluyol/fabbench/internal/fnv"
	"github.com/uluyol/fabbench/internal/perm"
)

// A generator of a zipfian distribution. It produces a
// sequence of items, such that some items are more popular
// than others.
//
// Popular items will be clustered, e.g. item 0 is the most
// popular, item 1 is the second most, etc. If you want them
// to be spread out, use a ScrambledZipfianGen.
//
// The algorithm used is from "Quickly Generating Billion-
// Record Synthetic Databases", Jim Gray et al, SIGMOD 1994.
//
// The implementation is based of that in YCSB.
type Zipfian struct {
	items int64
	base  int64

	zipfianConst float64

	// params
	alpha      float64
	zetan      float64
	eta        float64
	theta      float64
	zeta2theta float64

	rand *rand.Rand
}

func NewZipfian(randSrc rand.Source, min, max int64, theta, zetan float64) *Zipfian {
	items := max - min + 1
	return &Zipfian{
		items:        items,
		base:         min,
		zipfianConst: theta,
		theta:        theta,
		zeta2theta:   zeta(2, theta),
		alpha:        1 / (1 - theta),
		zetan:        zetan,
		eta:          (1 - math.Pow(2/float64(items), 1-theta)) / (1 - zeta(2, theta)/zetan),
		rand:         rand.New(randSrc),
	}
}

func NewSimpleZipfian(randSrc rand.Source, nitems int64, theta float64) *Zipfian {
	return NewZipfian(randSrc, 0, nitems-1, theta, zetastatic(nitems, theta))
}

func zetastatic(n int64, theta float64) float64 {
	partials := make(chan float64)
	nw := runtime.NumCPU()
	wops := int64(math.Ceil(float64(n) / float64(nw)))
	for w := 0; w < nw; w++ {
		start := int64(w) * wops
		end := int64(w+1) * wops
		if end > n {
			end = n
		}
		pZetastatic(partials, start, end, theta)
	}
	var sum float64
	for w := 0; w < nw; w++ {
		sum += <-partials
	}
	return sum
}

func pZetastatic(c chan<- float64, start, end int64, theta float64) {
	go func() {
		var sum float64
		for i := start; i < end; i++ {
			sum += 1 / math.Pow(float64(i+1), theta)
		}
		c <- sum
	}()
}

func zeta(n int64, theta float64) float64 {
	var sum float64
	for i := int64(0); i < n; i++ {
		sum += 1 / math.Pow(float64(i+1), theta)
	}
	return sum
}

func (g *Zipfian) Next() int64 {
	u := g.rand.Float64()
	uz := u * g.zetan

	if uz < 1 {
		return g.base
	}
	if uz < 1+math.Pow(0.5, g.theta) {
		return g.base + 1
	}
	return int64(float64(g.items) * math.Pow(g.eta*u-g.eta+1, g.alpha))
}

type ScrambledZipfian struct {
	g         *Zipfian
	min       int64
	max       int64
	itemCount int64
}

func NewScrambledZipfian(randSrc rand.Source, min, max int64, theta, zetan float64) *ScrambledZipfian {
	const internalZGItemCount = 10000000000

	return &ScrambledZipfian{
		g:         NewZipfian(randSrc, 0, internalZGItemCount, theta, zetan),
		min:       min,
		max:       max,
		itemCount: max - min + 1,
	}
}

func (g *ScrambledZipfian) Next() int64 {
	v := g.max
	for v >= g.max {
		v = g.min + (fnv.Hash64(g.g.Next()) % g.itemCount)
	}
	return v
}

type MapScrambledZipfian struct {
	g         *Zipfian
	perm      perm.Int64
	itemCount int64
}

func NewMapScrambledZipfian(randSrc rand.Source, nitems int64, theta float64) *MapScrambledZipfian {
	return &MapScrambledZipfian{
		g:         NewZipfian(randSrc, 0, nitems-1, theta, zetastatic(nitems, theta)),
		perm:      perm.NewInt64(rand.New(randSrc), nitems),
		itemCount: nitems,
	}
}

func (g *MapScrambledZipfian) Next() int64 {
	i := g.g.Next()
	if i >= g.itemCount || i < 0 {
		panic(fmt.Errorf("invalid index: %d", i))
	}
	return g.perm.Of(i)
}
