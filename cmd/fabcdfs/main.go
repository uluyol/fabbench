package main

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/uluyol/fabbench/readers"
)

func main() {
	log.SetPrefix("fabcdfs: ")
	log.SetFlags(0)
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: fabcdfs log.gz > cdfs.cs")
		fmt.Fprintln(os.Stderr, "\nOUTPUT FORMAT")
		fmt.Fprintln(os.Stderr, "\t#start StepNum UnixSec Nano")
		fmt.Fprintln(os.Stderr, "\tStepNum,Percenile,Micros")
		os.Exit(2)
	}

	f, err := os.Open(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	gr, err := gzip.NewReader(f)
	if err != nil {
		log.Fatal(err)
	}
	defer gr.Close()

	br := bufio.NewReader(gr)

	for i := 0; ; i++ {
		l, err := readers.ReadLatency(br)
		if err != nil {
			break
		}
		fmt.Printf("#start %d %d %d\n", i, l.Start().Unix(), l.Start().Nanosecond())
		for _, v := range l.AllVals() {
			fmt.Printf("%d,%f,%d\n", i, v.Percentile, int64(v.Value/time.Microsecond))
		}
	}
}
