package perm

import "math/rand"

const (
	itemThresh int64 = 1 << 24
	mask       int64 = 1<<24 - 1
)

type Int64 struct {
	perm   []int32
	left   []int32
	inPerm int64
}

func NewInt64(r *rand.Rand, nitems int64) Int64 {
	if nitems < 0 {
		panic("nitems must be non-negative")
	}
	if nitems < itemThresh {
		return Int64{perm: perm(r, uint32(nitems)), inPerm: nitems}
	}
	inPerm := itemThresh * (nitems / itemThresh)
	leftCount := nitems % itemThresh
	return Int64{perm: perm(r, uint32(itemThresh)), left: perm(r, uint32(leftCount)), inPerm: inPerm}
}

func (p Int64) Of(i int64) int64 {
	if p.inPerm > i {
		top := i & ^mask
		bot := i & mask
		return top | int64(p.perm[bot])
	}
	return int64(p.left[i-p.inPerm]) + p.inPerm
}

// perm compute a permutation.
//
// Adapted from math/rand.*Rand.Perm
func perm(r *rand.Rand, n uint32) []int32 {
	m := make([]int32, n)
	// In the following loop, the iteration when i=0 always swaps m[0] with m[0].
	// A change to remove this useless iteration is to assign 1 to i in the init
	// statement. But Perm also effects r. Making this change will affect
	// the final state of r. So this change can't be made for compatibility
	// reasons for Go 1.
	for i := uint32(0); i < n; i++ {
		j := r.Int31n(int32(i + 1))
		m[i] = m[j]
		m[j] = int32(i)
	}
	return m
}
