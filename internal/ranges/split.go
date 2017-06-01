package ranges

import "time"

func SplitDuration(v time.Duration, maxSize time.Duration) []time.Duration {
	numChunk := (v + maxSize - 1) / maxSize
	if numChunk <= 0 {
		return nil
	}
	chunks := make([]time.Duration, int64(numChunk))
	cum := time.Duration(0)
	for i := time.Duration(0); i < numChunk-1; i++ {
		chunks[i] = v / numChunk
		cum += chunks[i]
	}
	chunks[len(chunks)-1] = v - cum
	return chunks
}

type WorkShard struct {
	Start int64
	Count int64
}

func SplitRecords(n int64, numWorkers int64) []WorkShard {
	maxSize := (n + numWorkers - 1) / numWorkers
	chunks := make([]WorkShard, numWorkers)
	var cum int64
	for i := int64(0); i < numWorkers-1; i++ {
		chunks[i] = WorkShard{Start: cum, Count: maxSize}
		cum += maxSize
	}
	chunks[len(chunks)-1] = WorkShard{Start: cum, Count: n - cum}
	return chunks
}
