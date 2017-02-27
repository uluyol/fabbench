package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/uluyol/fabbench/bench"
	"github.com/uluyol/fabbench/internal/ranges"
)

var (
	maxRT = flag.Duration("maxrt", 15*time.Second, "maximum runtime of any specific step")
)

func loadTrace(p string) ([]bench.TraceStep, error) {
	f, err := os.Open(p)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	t, err := bench.ParseTrace(f)
	if err != nil {
		return nil, fmt.Errorf("%s:%v", p, err)
	}

	return t, nil
}

func main() {
	log.SetPrefix("fabtraceproc: ")
	log.SetFlags(0)
	flag.Parse()

	t, err := bench.ParseTrace(os.Stdin)
	if err != nil {
		log.Fatal(err)
	}

	for _, step := range t {
		for _, chunkDur := range ranges.SplitDuration(step.Duration, *maxRT) {
			chunk := step
			chunk.Duration = chunkDur
			fmt.Println(chunk.String())
		}
	}
}
