package ranges

import (
	"testing"
	"time"
)

func TestSplitDuration(t *testing.T) {
	tests := []struct {
		v    time.Duration
		size time.Duration
		num  int
	}{
		{1000, 5, 200},
		{123, 10, 13},
		{0, 100, 0},
		{889, 10000, 1},
		{50, 7, 8},
		{13, 3, 5},
		{99, 100, 1},
		{99, 99, 1},
		{123, 123, 1},
		{8773, 800, 11},
	}

	for i, test := range tests {
		chunks := SplitDuration(test.v, test.size)
		if len(chunks) != test.num {
			t.Errorf("case %d: want %d chunks got %d", i, test.num, len(chunks))
			continue
		}
		var sum time.Duration
		for _, v := range chunks {
			if v <= 0 {
				t.Errorf("case %d: got chunk with val <= 0", i)
			}
			sum += v
		}
		if sum != test.v {
			t.Errorf("case %d: chunks don't sum to input: want %d, got %d", i, test.v, sum)
		}
	}
}

func TestSplitRecords(t *testing.T) {
	tests := []struct {
		numRec     int64
		numWorkers int64
		wantSize   int64
		wantLast   int64
	}{
		{1000, 10, 100, 100},
		{999, 10, 100, 99},
		{5000, 3, 1667, 1666},
		{222, 111, 2, 2},
	}

	for i, test := range tests {
		shards := SplitRecords(test.numRec, test.numWorkers)
		var cum int64
		for j := 0; j < len(shards)-1; j++ {
			if shards[j].Start != cum {
				t.Errorf("case %d: got wrong Start for chunk %d: want %d got %d", i, j, cum, shards[j].Start)
			}
			if shards[j].Count != test.wantSize {
				t.Errorf("case %d: got wrong Count for chunk %d: want %d got %d", i, j, test.wantSize, shards[j].Count)
			}
			cum += test.wantSize
		}
		last := shards[len(shards)-1]
		if last.Start != cum {
			t.Errorf("case %d: got wrong Start for last chunk: want %d got %d", i, cum, last.Start)
		}
		if last.Count != test.wantLast {
			t.Errorf("case %d: got wrong Count for last chunk: want %d got %d", i, test.wantLast, last.Count)
		}
		if cum+test.wantLast != test.numRec {
			t.Errorf("case %d: test error: counts do not add up: want %d got %d", i, test.numRec, cum+test.wantLast)
		}
	}
}
