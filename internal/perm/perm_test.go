package perm

import (
	"math/rand"
	"testing"
	"time"
)

func TestInt64(t *testing.T) {
	nitems := []int64{
		0, 2, 4, 8, 10, 15, 17, 199, 512, 24234, 55555, 80808, 1000000, 1<<24 - 1, 1 << 24,
		1<<24 + 1, 1<<24 + 17, 1<<25 - 1, 1 << 25, 1<<26 + 1<<24, 1 << 28, 1<<28 + 3,
	}

	for _, ni := range nitems {
		hit := make([]bool, ni)
		perm := NewInt64(rand.New(rand.NewSource(time.Now().UnixNano())), ni)

		for i := int64(0); i < ni; i++ {
			j := perm.Of(i)
			if j >= ni {
				t.Errorf("nitems %d: too big %d", ni, j)
				continue
			}
			if hit[j] {
				t.Errorf("nitems %d: already hit %d, cur: %d", ni, j, i)
			}
			hit[j] = true
		}
	}
}
