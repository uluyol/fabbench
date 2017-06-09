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

type ShardedSource struct {
	shards []LockedSource
}

func NewShardedSource(src rand.Source) ShardedSource {
	shards := make([]LockedSource, runtime.NumCPU())
	for i := 0; i < len(shards); i++ {
		shards[i].s = rand.NewSource(src.Int63())
	}
	return ShardedSource{shards}
}

func (s ShardedSource) Get(i int) rand.Source {
	return &s.shards[i%len(s.shards)]
}
