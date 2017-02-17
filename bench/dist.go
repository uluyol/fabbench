package bench

import (
	"math"
	"math/rand"
)

type poisson struct {
	l float64
	r *rand.Rand
}

func newPoisson(rsrc rand.Source, lambda float64) poisson {
	if lambda <= 0 {
		panic("lambda must be positive")
	}
	return poisson{math.Exp(-lambda), rand.New(rsrc)}
}

func (g poisson) Next() int64 {
	k := int64(0)
	p := float64(1)

	for p > g.l {
		k++
		p *= g.r.Float64()
	}
	return k - 1
}

type closed struct{}

func (g closed) Next() int64 {
	panic("closed generator should never be called")
}
