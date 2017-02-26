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
