package main

import (
	"bufio"
	"compress/gzip"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/uluyol/fabbench/readers"
)

var (
	start = flag.Int64("start", 0, "filter from this start time (in unix seconds)")
	end   = flag.Int64("end", -1, "filter from this end time (in unix seconds)")
	merge = flag.Bool("merge", false, "merge points that have the same value in the ccdf")
)

func usage() {
	fmt.Fprintln(os.Stderr, "usage: fabcdfs [flags] log.gz > cdfs.csv")
	fmt.Fprintln(os.Stderr, "\nOUTPUT FORMAT")
	fmt.Fprintln(os.Stderr, "\t#start StepNum=N NumSamples=K UnixTime=Sec,Nano")
	fmt.Fprintln(os.Stderr, "\tStepNum,Percenile,Micros")
	flag.PrintDefaults()
	os.Exit(2)
}

func main() {
	log.SetPrefix("fabcdfs: ")
	log.SetFlags(0)
	flag.Usage = usage
	flag.Parse()
	if flag.NArg() != 1 {
		usage()
	}

	f, err := os.Open(flag.Arg(0))
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

	startTime := time.Unix(*start, 0)
	endTime := time.Unix(*end, 0)
	if *end == -1 {
		endTime = time.Date(2050, time.January, 1, 1, 1, 1, 0, time.UTC)
	}

	l, err := readers.ReadLatency(br)
	if err != nil {
		log.Fatal(err)
	}

	for i := range l.Hists {
		hist := l.Hists[i]
		errs := l.Errs[i]

		start, ok := hist.StartTime()
		if !ok {
			start = time.Unix(0, 0)
		}
		end, ok := hist.EndTime()
		if !ok {
			end = time.Date(2050, time.January, 1, 1, 1, 1, 0, time.UTC)
		}

		if start.Before(startTime) || end.After(endTime) {
			continue
		}

		allVals := hist.AllVals()

		fmt.Printf("#start StepNum=%d NumSamples=%d UnixStart=%d,%d UnixEnd=%d,%d Errs=%d\n",
			i, hist.TotalCount(), start.Unix(), start.Nanosecond(),
			end.Unix(), end.Nanosecond(), errs)

		for _, cur := range allVals {
			if *merge {
				if cur.Count > 0 {
					fmt.Printf("%d,%f,%d\n", i, cur.Percentile, cur.Value/int64(time.Microsecond))
				}
			} else {
				for n := int64(0); n < cur.Count; n++ {
					fmt.Printf("%d,%f,%d\n", i, cur.Percentile, cur.Value/int64(time.Microsecond))
				}
			}
		}
	}
}
