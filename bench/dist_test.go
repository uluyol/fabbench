package bench

import (
	"math"
	"math/rand"
	"testing"
	"time"
)

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
