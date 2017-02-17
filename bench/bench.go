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
	"github.com/uluyol/fabbench/recorders"
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
				default: // don't wait
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

	ReadRecorder  recorders.Latency
	ReadWriter    io.Writer
	WriteRecorder recorder.Latency
	WriteWriter   io.Writer
}

type result struct {
	latency time.Duration
	err     error
}

func recordAndWrite(c <-chan result, wg *sync.WaitGroup, rec *recorders.Latency, w io.Writer) {
	for res := range c {
		rec.Record(res.latency, res.err)
	}
	rec.WriteTo(w)
	wg.Done()
}

func (r *Runner) Run(ctx context.Context) error {
	valGen := newValueGen(rand.NewSource(r.Rand.Int63()), l.Config.ValSize)

	for _, ts := range trace {
		select {
		case <-ctx.Done():
			break
		default: // don't wait
		}
		rg := makeSyncGen(ts.ReadKeyDist, rand.NewSource(r.Rand.Int63()), r.Config.RecordCount)
		wg := makeSyncGen(ts.WriteKeyDist, rand.NewSource(r.Rand.Int63()), r.Config.RecordCount)
		readKeyGen := stringGen{G: rg, Len: r.Config.KeySize}
		writeKeyGen := stringGen{G: wg, Len: r.Config.KeySize}

		readC := make(chan result)
		writeC := make(chan result)

		r.ReadRecorder.Reset()
		r.WriteRecorder.Reset()

		var wg sync.WaitGroup
		wg.Add(1)
		go recordAndWrite(readC, &wg, &r.ReadRecorder, r.ReadWriter)
		wg.Add(1)
		go recordAndWrite(writeC, &wg, &r.WriteRecorder, r.WriteWriter)

		args := issueArgs{
			db:          r.DB,
			readKeyGen:  readKeyGen,
			writeKeyGen: writeKeyGen,
			valGen:      valGen,
			rwRatio:     ts.RWRatio,
			rand:        r.Rand,
			readC:       readC,
			writeC:      writeC,
		}

		if ts.ArrivaDist.Kind == adClosed {
			nops := int64(ts.Duration.Seconds() * float64(ts.AvgQPS))
			issueClosed(ctx, rc, args, ts.ArrivalDist.clWorkers(), nops)
		} else {
			meanPeriod := float64(time.Second) / float64(ts.AvgQPS)
			// shrink period so that dist calculation doesn't take too long
			meanPeriod /= 10 * float64(time.Microsecond)
			arrivalGen := makeArrivalDist(ts.ArrivalDist, rand.NewSource(r.Rand.Int63()), float64(meanPeriod))

			issueOpen(ctx, rc, args, arrivalGen, ts.Duration)
		}

		close(readC)
		close(writeC)
		wg.Wait()
	}
}

type issueArgs struct {
	db          db.DB
	readKeyGen  stringGen
	writeKeyGen stringGen
	valGen      valueGen
	rwRatio     float32
	rand        *rand.Rand

	readC, writeC chan<- result
}

func issueOpen(ctx context.Context, args issueArgs, arrivalGen intgen.Gen, execDuration time.Duration) {
	var wg sync.WaitGroup
	start := time.Now()
	for time.Since(start) < execDuration {
		select {
		case <-ctx.Done():
			break
		default: // don't wait
		}
		nextIsRead := args.rand.Float32() < args.rwRatio
		wait := time.Duration(arrivalGen()) * 10 * time.Microsecond
		time.Sleep(wait)
		reqStart := time.Now()
		wg.Add(1)
		go func(reqStart time.Time, isRead bool) {
			defer wg.Done()
			if isRead {
				key := args.readKeyGen.Next()
				_, err := args.db.Get(ctx, key)
				latency := time.Since(reqStart)
				args.readC <- result{latency, err}
			} else {
				key := args.writeKeyGen.Next()
				val := args.valGen.Next()
				err := args.db.Put(ctx, key, val)
				latency := time.Since(start)
				args.writeC <- result{latency, err}
			}
		}(reqStart, nextIsRead)
	}
	wg.Wait()
}

func issueClosed(ctx context.Context, args issueArgs, workers int, totalOps int64) {
	nops := new(counter)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(rng *rand.Rand) {
			defer wg.Done()

			for i := nops.getAndInc(); i < totalOps; i = nops.getAndInc() {
				select {
				case <-ctx.Done():
					break
				default: // don't wait
				}
				start := time.Now()
				if rng.Float32() < args.rwRatio {
					key := args.readKeyGen.Next()
					_, err := args.db.Get(ctx, key)
					latency := time.Since(start)
					args.readC <- result{latency, err}
				} else {
					key := args.writeKeyGen.Next()
					val := args.valGen.Next()
					err := args.db.Put(ctx, key, val)
					latency := time.Since(start)
					args.writeC <- result{latency, err}
				}
			}
		}(rand.New(args.rand.Int63()))
	}
	wg.Wait()
}
