package ranges

import (
	"fmt"
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
		fmt.Println(chunks)
	}
}
