package bench

import (
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/uluyol/fabbench/intgen"
)

func TestDistMeanAccurate(t *testing.T) {
	t.Parallel()
	tests := []struct {
		dist string
		mean int64
		gen  intgen.Gen
	}{
		{"poisson", 5, newPoisson(5)},
		{"poisson", 10, newPoisson(10)},
		{"poisson", 29, newPoisson(29)},
		{"poisson", 30, newPoisson(30)},
		{"poisson", 777, newPoisson(777)},
		{"poisson", 12221, newPoisson(12221)},
		{"poisson", 9912, newPoisson(9912)},
		{"poisson", 9999999912, newPoisson(9999999912)},
		{"uniform", 100, newUniform(100, 0.1)},
		{"uniform", 55, newUniform(55, 0)},
		{"uniform", 1999, newUniform(1999, 0.2)},
		{"uniform", 555635, newUniform(555635, 0.05)},
	}

	for _, test := range tests {
		tc := test
		t.Run(fmt.Sprintf("Dist=%s,Mean=%d", test.dist, test.mean), func(t *testing.T) {
			t.Parallel()
			r := rand.New(rand.NewSource(56))
			const NumSamples = 10000
			var sum int64
			for i := 0; i < NumSamples; i++ {
				sum += tc.gen.Next(r)
			}
			mean := sum / NumSamples
			if mean < 9*tc.mean/10 || 11*tc.mean/10 < mean {
				t.Errorf("want mean %d have %d for %v", tc.mean, mean, tc.gen)
			}
		})
	}
}

func TestUniformArrivalDist(t *testing.T) {
	tests := []struct {
		mean, width float64
	}{
		{100, 0.5},
		{55, 1},
		{888888, 0.0001},
		{123, 1},
	}

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	for _, test := range tests {
		g := newUniform(test.mean, test.width)
		mean := float64(g.Next(rng))
		count := float64(1)

		for i := 0; i < 100000; i++ {
			mean = mean*count/(count+1) + float64(g.Next(rng))/(count+1)
			count++
		}

		if math.Floor(mean+0.5) != test.mean {
			t.Errorf("case %f-%f: got mean %f", test.mean, test.width, math.Floor(mean+0.5))
		}
	}
}

var sink int64

func BenchmarkPoisson(b *testing.B) {
	rng := rand.New(rand.NewSource(0))
	g := newPoisson(1000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sink = g.Next(rng)
	}
}
