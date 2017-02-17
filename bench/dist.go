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

type uniform struct {
	min int64
	siz int64
	r   *rand.Rand
}

func newUniform(rsrc rand.Source, mean, width float64) uniform {
	min := int64(mean - mean*width)
	max := int64(mean + mean*width)
	siz := max - min + 1 // since we draw rand numbers below siz
	return uniform{min: min, siz: siz, r: rand.New(rsrc)}
}

func (g uniform) Next() int64 {
	return g.min + g.r.Int63n(g.siz)
}
