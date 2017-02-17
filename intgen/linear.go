package intgen

import (
	"fmt"
	"math"
	"math/rand"
)

/*

LinearGen's constants come from the following

Σ P(x) = 1 and P(N) = 0 where P(x) = mx + b

Solving for m and b gives

m = -2 / [ N(N+1) ]
b = 2 / (N + 1)

From there, we solve for the CDF at x, finding
CDF(x) = 2 - x(x+1) / (N(N+1))

Solving for x, we find

x = [ 2N-1 - √( 4N^2+4N+1 - 4CDFN(N+1) ) ] / 2

For performance, we precompute a few constants.

*/

// Linear selects numbers up to N
// with linearly decreasing probabilities where
// the probability reaches 0 at N.
type Linear struct {
	n int64
	a int64   // precomputed 2N-1
	s float64 // precomputed 4N^2+4N+1
	d float64 // precomputed 4N(N+1)
	m float64 // precomputed -2 / [ N(N+1) ]
	b float64 // precomputed 2 / (N+1)
	r *rand.Rand
}

func NewLinear(s rand.Source, n int64) *Linear {
	n++ // FIXME: hack to deal with spike at 0
	return &Linear{
		n: n,
		a: 2*n - 1,
		s: float64(4*n*n + 4*n + 1),
		d: float64(4 * n * (n + 1)),
		m: -2 / float64(n*(n+1)),
		b: 2 / float64(n+1),
		r: rand.New(s),
	}
}

func (g *Linear) Next() int64 {
	// TODO: Stop hacking around 0 spike.
	//       To avoid the spike that we see at 0,
	//       we are setting n to n+1,
	//       retrying if we ever get 0,
	//       and then just subtracting 1.
	//       this is a ugly hack that happens to work.
	//       Fix this.
	var f int64
	for f == 0 {
		cdf := g.r.Float64()

		toroot := g.s - g.d*cdf
		tosub := int64(math.Sqrt(toroot))
		top := g.a - tosub
		f = top / 2
		if f >= g.n {
			panic(fmt.Errorf("impossible next: %d", f))
		} else if f < 0 {
			// can have significant rounding error
			// when calculating sqrt
			f = 0
		}
	}
	return f - 1
}

func (g *Linear) ProbOf(n int64) float64 {
	return g.m*float64(n) + g.b
}
