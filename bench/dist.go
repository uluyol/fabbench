package bench

import (
	"math"
	"math/rand"
)

type poisson struct {
	l float64
}

func newPoisson(lambda float64) poisson {
	if lambda <= 0 {
		panic("lambda must be positive")
	}
	return poisson{math.Exp(-lambda)}
}

func (g poisson) Next(rng *rand.Rand) int64 {
	k := int64(0)
	p := float64(1)

	for p > g.l {
		k++
		p *= rng.Float64()
	}
	return k - 1
}

type closed struct{}

func (g closed) Next(_ *rand.Rand) int64 {
	panic("closed generator should never be called")
}

type uniform struct {
	min int64
	siz int64
}

func newUniform(mean, width float64) uniform {
	min := int64(mean - mean*width)
	max := int64(math.Ceil(mean + mean*width))
	siz := max - min + 1 // since we draw rand numbers below siz
	return uniform{min: min, siz: siz}
}

func (g uniform) Next(rng *rand.Rand) int64 {
	t := rng.Int63n(g.siz)
	return g.min + t
}
