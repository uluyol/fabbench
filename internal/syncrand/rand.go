package syncrand

import (
	"math/rand"
	"runtime"
	"sync"
)

type LockedSource struct {
	mu sync.Mutex
	s  rand.Source
}

func NewLockedSource(seed int64) rand.Source {
	return &LockedSource{s: rand.NewSource(seed)}
}

func (s *LockedSource) Seed(seed int64) {
	s.mu.Lock()
	s.s.Seed(seed)
	s.mu.Unlock()
}

func (s *LockedSource) Int63() int64 {
	s.mu.Lock()
	v := s.s.Int63()
	s.mu.Unlock()
	return v
}

type Sharded struct {
	shards []*rand.Rand
}

func NewSharded(src rand.Source) Sharded {
	srcShards := make([]LockedSource, runtime.NumCPU())
	rngShards := make([]*rand.Rand, len(srcShards))
	for i := 0; i < len(srcShards); i++ {
		srcShards[i].s = rand.NewSource(src.Int63())
		rngShards[i] = rand.New(&srcShards[i])
	}
	return Sharded{rngShards}
}

func (s Sharded) Get(i int) *rand.Rand {
	return s.shards[i%len(s.shards)]
}
