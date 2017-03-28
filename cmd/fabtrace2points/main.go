package main

import (
	"fmt"
	"log"
	"os"

	"github.com/uluyol/fabbench/bench"
)

func main() {
	log.SetPrefix("fabtrace2points: ")
	log.SetFlags(0)

	t, err := bench.ParseTrace(os.Stdin)
	if err != nil {
		log.Fatal(err)
	}

	var now float64
	for _, step := range t {
		fmt.Printf("%f,%d\n", now, step.AvgQPS)
		now += step.Duration.Seconds()
	}
}
