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

func usage() {
	fmt.Fprintln(os.Stderr, "usage: fabstartend log.gz...")
	fmt.Fprintln(os.Stderr, "\nOUTPUT FORMAT")
	fmt.Fprintln(os.Stderr, "\tstart\tend")
	flag.PrintDefaults()
	os.Exit(2)
}

func main() {
	log.SetPrefix("fabstartend: ")
	log.SetFlags(0)
	flag.Usage = usage
	flag.Parse()
	if flag.NArg() != 1 {
		usage()
	}

	for _, p := range flag.Args() {
		if err := procFile(p); err != nil {
			log.Fatal(err)
		}
	}
}

func procFile(p string) error {
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

	start := time.Unix(0, 0)
	end := time.Date(2050, time.January, 1, 1, 1, 1, 0, time.UTC)
	if len(l.Hists) > 0 {
		if hstart, ok := l.Hists[0].StartTime(); ok {
			start = hstart
		}
		if hend, ok := l.Hists[len(l.Hists)-1].EndTime(); ok {
			end = hend
		}
	}

	fmt.Printf("%f\t%f\n", timeToSec(start), timeToSec(end))
	return nil
}

func timeToSec(t time.Time) float64 {
	s := float64(t.Unix())
	ns := float64(t.Nanosecond())
	return s + (ns / 1e9)
}
