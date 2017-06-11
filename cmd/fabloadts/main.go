package main

import (
	"bufio"
	"compress/gzip"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/uluyol/fabbench/readers"
	"github.com/uluyol/hdrhist"
)

var (
	start = flag.Int64("start", 0, "filter from this start time (in unix seconds)")
	end   = flag.Int64("end", -1, "filter from this end time (in unix seconds)")
)

func usage() {
	fmt.Fprintln(os.Stderr, "usage: fabloadts [flags] log.gz > cdfs.csv")
	fmt.Fprintln(os.Stderr, "\nOUTPUT FORMAT")
	fmt.Fprintln(os.Stderr, "\t#start StepNum=N NumSamples=K UnixTime=Sec,Nano")
	fmt.Fprintln(os.Stderr, "\tStepNum,Percenile,Micros")
	flag.PrintDefaults()
	os.Exit(2)
}

func main() {
	log.SetPrefix("fabloadts: ")
	log.SetFlags(0)
	flag.Usage = usage
	flag.Parse()
	if flag.NArg() != 1 {
		usage()
	}

	startTime := time.Unix(*start, 0)
	endTime := time.Unix(*end, 0)
	if *end == -1 {
		endTime = time.Date(2050, time.January, 1, 1, 1, 1, 0, time.UTC)
	}

	w := bufio.NewWriter(os.Stdout)
	defer w.Flush()

	accum := hdrhist.New(3)

	for _, p := range flag.Args() {
		if err := procFile(p, w, startTime, endTime, accum); err != nil {
			log.Fatal(err)
		}
	}
}

func procFile(p string, w io.Writer, startTime, endTime time.Time, accum *hdrhist.Hist) error {
	f, err := os.Open(p)
	if err != nil {
		return err
	}
	defer f.Close()

	gr, err := gzip.NewReader(f)
	if err != nil {
		return err
	}
	defer gr.Close()

	br := bufio.NewReader(gr)

	l, err := readers.ReadLatency(br)
	if err != nil {
		return err
	}

	for i := range l.Hists {
		hist := l.Hists[i]
		errs := l.Errs[i]

		hstart, ok := hist.StartTime()
		if !ok {
			hstart = time.Unix(0, 0)
		}
		hend, ok := hist.EndTime()
		if !ok {
			hend = time.Date(2050, time.January, 1, 1, 1, 1, 0, time.UTC)
		}

		if *end != -1 && (hstart.Before(startTime) || hend.After(endTime)) {
			continue
		}

		dur := float64(hend.Sub(hstart)) / float64(time.Second)
		fmt.Fprintf(w, "%f,%f,%f\n", float64(hstart.UnixNano())/1e9, float64(hist.TotalCount())/dur, float64(errs)/dur)
	}
	return nil
}
