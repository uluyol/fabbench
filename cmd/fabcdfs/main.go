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
		fmt.Fprintln(os.Stderr, "\t#start StepNum=N NumSamples=K UnixTime=Sec,Nano")
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
		allVals := l.AllVals()
		fmt.Printf("#start StepNum=%d NumSamples=%d UnixTime=%d,%d\n", i, len(allVals), l.Start().Unix(), l.Start().Nanosecond())

		prev := readers.HistVal{Value: -1, Percentile: -1}

		for _, cur := range allVals {
			if cur.Value/time.Microsecond != prev.Value/time.Microsecond {
				if prev.Percentile >= 0 {
					fmt.Printf("%d,%f,%d\n", i, prev.Percentile, prev.Value/time.Microsecond)
				}
			}
			prev = cur
		}

		if prev.Percentile >= 0 {
			fmt.Printf("%d,%f,%d\n", i, prev.Percentile, prev.Value/time.Microsecond)
		}
	}
}
