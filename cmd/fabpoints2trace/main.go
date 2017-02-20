package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"strconv"
	"strings"
	"time"
)

var (
	runtime    = flag.Duration("runtime", 1*time.Hour, "runtime of total experiment")
	maxQPS     = flag.Float64("maxqps", 0, "max qps to use")
	minRT      = flag.Duration("minrt", 5*time.Second, "minimum runtime of any specific step")
	minQPSDiff = flag.Int64("minqpsdiff", 1, "minimum change in qps across steps")
	rwFrac     = flag.Float64("rw", 0.9, "frac of requests that are reads")
	ad         = flag.String("ad", "poisson", "inter arrival distribution")
	rkd        = flag.String("rkd", "zipfian-0.99999", "read key distribution")
	wkd        = flag.String("wkd", "uniform", "write key distribution")
)

func slice2chan(in [][2]float64) <-chan [2]float64 {
	out := make(chan [2]float64)
	go func() {
		for _, p := range in {
			out <- p
		}
		close(out)
	}()
	return out
}

func chan2slice(in <-chan [2]float64) [][2]float64 {
	var out [][2]float64
	for p := range in {
		out = append(out, p)
	}
	return out
}

func filterRT(in <-chan [2]float64, minRT time.Duration) <-chan [2]float64 {
	out := make(chan [2]float64)
	go func() {
		prev := <-in
		for cur := range in {
			if time.Duration(cur[0]-prev[0]) < minRT {
				continue
			}
			out <- prev
			prev = cur
		}
		out <- prev
		close(out)
	}()
	return out
}

func filterQPS(in <-chan [2]float64, minQPSDiff int64) <-chan [2]float64 {
	qpsThresh := float64(minQPSDiff)
	out := make(chan [2]float64)
	go func() {
		prev := <-in
		for cur := range in {
			if math.Abs(cur[1]-prev[1]) < qpsThresh {
				continue
			}
			out <- prev
			prev = cur
		}
		out <- prev
		close(out)
	}()
	return out
}

func main() {
	log.SetPrefix("fabpoints2trace: ")
	log.SetFlags(0)
	flag.Parse()
	if *maxQPS <= 0 {
		log.Fatal("maxqps must be set")
	}

	var steps [][2]float64
	var maxY float64 = math.Inf(-1)

	s := bufio.NewScanner(os.Stdin)
	for s.Scan() {
		fields := strings.Split(s.Text(), ",")
		if len(fields) != 2 {
			log.Fatal("invalid format: must be csv with pairs of time, qps values")
		}
		x, err := strconv.ParseFloat(fields[0], 64)
		if err != nil {
			log.Fatal("invalid format: must be csv with pairs of time, qps values")
		}
		y, err := strconv.ParseFloat(fields[1], 64)
		if err != nil {
			log.Fatal("invalid format: must be csv with pairs of time, qps values")
		}
		if y > maxY {
			maxY = y
		}
		steps = append(steps, [2]float64{x, y})
	}
	if s.Err() != nil {
		log.Fatal("error reading input: %v", s.Err())
	}

	x2t := float64(*runtime) / steps[len(steps)-1][0]

	for i := range steps {
		steps[i][0] = steps[i][0] * x2t
		steps[i][1] = *maxQPS * steps[i][1] / maxY
	}

	filtered := chan2slice(filterRT(filterQPS(slice2chan(steps), *minQPSDiff), *minRT))

	for i := 0; i < len(filtered)-1; i++ {
		dur := time.Duration(filtered[i+1][0] - filtered[i][0])
		qps := int64(filtered[i][1])

		fmt.Printf("d=%s rw=%f qps=%d ad=%s rkd=%s wkd=%s\n", dur, *rwFrac, qps, *ad, *rkd, *wkd)
	}
}
