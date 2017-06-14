package bench

import (
	"math"
	"math/rand"

	"github.com/uluyol/fabbench/intgen"
)

func newPoisson(λ float64) intgen.Gen {
	if λ <= 0 {
		panic("λ must be positive")
	}
	if λ < 30 {
		return newPoissonSmall(λ)
	}
	return newPoissonLarge(λ)
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
