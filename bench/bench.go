package bench

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"sync"
	"time"

	"github.com/uluyol/fabbench/db"
	"github.com/uluyol/fabbench/intgen"
)

type Config struct {
	RecordCount int64 `json:"recordCount"`
	KeySize     int   `json:"keySize"`
	ValSize     int   `json:"valSize"`
}

type counter struct {
	mu sync.Mutex
	c  int64
}

func (c *counter) getAndInc() int64 {
	c.mu.Lock()
	v := c.c
	c.c++
	c.mu.Unlock()
	return v
}

type Loader struct {
	DB         db.DB
	Config     Config
	Rand       *rand.Rand
	NumWorkers int

	LoadStart int64
	LoadCount int64
}

func (l *Loader) Run(ctx context.Context) error {
	keyGen := stringGen{
		G:   intgen.NewSync(&intgen.Counter{Count: l.LoadStart}),
		Len: l.Config.KeySize,
	}
	valGen := newValueGen(rand.NewSource(l.Rand.Int63()), l.Config.ValSize)

	newCtx, cancel := context.WithCancel(ctx)

	if l.LoadStart < 0 {
		panic("load start must be non-negative")
	}

	loadCount := l.LoadCount
	if loadCount < 0 {
		loadCount = l.Config.RecordCount
	}

	nops := new(counter)
	errs := make(chan err)

	for i := 0; i < l.NumWorkers; i++ {
		go func() {
			var retErr error
			defer func() {
				errs <- retErr
			}()

			for i := nops.getAndInc(); i < loadCount; i = nops.getAndInc() {
				key := keyGen.Next()
				val := valGen.Next()
				retErr = db.Put(newCtx, key, val)
				if retErr != nil {
					return
				}
				select {
				case <-newCtx.Done():
					retErr = newCtx.Err()
					return
				default:
				}
			}
		}()
	}

	var retErr error
	for i := 0; i < l.NumWorkers; i++ {
		err := <-errs
		if err != nil && retErr == nil {
			retErr = err
			cancel()
		}
	}

	return retErr
}

func makeSyncGen(d keyDist, rsrc rand.Source, nitems int64) intgen.Gen {
	var g intgen.Gen
	switch d.Kind {
	case kdUniform:
		g = intgen.NewUniform(rsrc, nitems)
	case kdZipfian:
		g = intgen.NewMapScrambledZipfian(rsrc, nitems, d.zfTheta())
	case kdLinear:
		g = intgen.NewLinear(rsrc, nitems)
	case kdLinStep:
		g = intgen.NewLinearStep(rsrc, nitems, d.lsSteps())
	default:
		panic(fmt.Errorf("invalid key dist %v", d))
	}
	return intgen.NewSync(g)
}

func makeArrivalDist(d arrivalDist, rsrc rand.Source, meanPeriod float64) intgen.Gen {
	var g intgen.Gen
	switch d.Kind {
	case adClosed:
		g = closed{}
	case adUniform:
		g = intgen.NewUniform(rsrc, int64(2*meanPeriod))
	case adPoisson:
		g = intgen.NewPoisson(rsrc, meanPeriod)
	default:
		panic(fmt.Errorf("invalid arrival dist %v", d))
	}
	return g
}

type Runner struct {
	DB     db.DB
	Config Config
	Rand   *rand.Rand
	Trace  []TraceStep

	LatencyRecorder *recorders.Latency
	LatencyWriter   io.Writer

	TraceRecorder *recorders.Trace
	TraceWriter   io.Writer
}

func (r *Runner) Run(ctx context.Context) error {
	type result struct {
		latency time.Duration
		err     error
	}

	valGen := newValueGen(rand.NewSource(r.Rand.Int63()), l.Config.ValSize)

	for _, ts := range trace {
		rg := makeSyncGen(ts.ReadKeyDist, rand.NewSource(r.Rand.Int63()), r.Config.RecordCount)
		wg := makeSyncGen(ts.WriteKeyDist, rand.NewSource(r.Rand.Int63()), r.Config.RecordCount)
		readKeyGen := stringGen{G: rg, Len: r.Config.KeySize}
		writeKeyGen := stringGen{G: wg, Len: r.Config.KeySize}

		meanPeriod := float64(time.Second) / float64(ts.AvgQPS)
		// shrink period so that dist calculation doesn't take too long
		meanPeriod /= 10 * float64(time.Microsecond)

		// TODO: implement me
		arrivalDist := makeArrivalDist(ts.ArrivalDist, rand.NewSource(r.Rand.Int63()), float64(meanPeriod))

		rc := make(chan result)

	}
}

type openLoopArgs struct{}

func issueOpen(ctx context.Context, rc chan<- result, tc chan<- traceResult, args openLoopArgs) {}

type closedLoopArgs struct{}

func issueClosed(ctx context.Context, rc chan<- result, tc chan<- traceResult, args closedLoopArgs) {}