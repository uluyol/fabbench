package intgen

import (
	"fmt"
	"math/rand"
)

/*

Tried to make an inverse function for the CDF for this.
Unfortunately, that turns out to be challenging.
The step-nature of the function can be worked around
by iterating in steps to find the largest where full steps can be taken,
and an inverse function can be used once that is known.
While doing this however, I ran into edge cases that involved division-by-zero.

The solution used here is easier to understand and works
but involves generating two random numbers.

Oh well.

The pmf for this is

P(x) = floor(Kx/N) * -2/(KN+N) + 2K/(KN+N)

*/

// LinearStep selects numbers up to N
// with linearly decreasing probabilities in K steps.
// The probability of drawing ≥ N is 0.
type LinearStep struct {
	n int64
	k int64

	// precomputed for perf
	step int64 // N/K

	m float64 // -2 / (KN+N)
	b float64 // 2K / (KN+N)
}

func NewLinearStep(n, k int64) LinearStep {
	g := LinearStep{
		n:    n,
		k:    k,
		step: n / k,
		m:    -2 / float64(k*n+n),
		b:    float64(2*k) / float64(k*n+n),
	}

	if g.step*g.k != g.n {
		panic("invalid n, k: n must be a multiple of k")
	}

	return g
}

func (g LinearStep) Next(src rand.Source) int64 {

	// sum from 0 to K (inclusive both sides)
	stop := src.Int63() % (g.k * (g.k + 1) / 2)

	var cum int64
	for i := int64(1); i <= g.k; i++ {
		cum += i
		if stop < cum {
			return (i-1)*g.step + src.Int63()%g.step
		}
	}
	panic(fmt.Errorf("unable to stop: stop %d cum %d", stop, cum))
}

func (g LinearStep) ProbOf(n int64) float64 {
	stepno := (n * g.k) / g.n
	return float64(stepno)*g.m + g.b
}
