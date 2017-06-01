package bench

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/uluyol/fabbench/db"
	"github.com/uluyol/fabbench/intgen"
	"github.com/uluyol/fabbench/recorders"
	"github.com/uluyol/hdrhist"
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

func (c *counter) get() int64 {
	c.mu.Lock()
	v := c.c
	c.mu.Unlock()
	return v
}

type Logger interface {
	Print(v ...interface{})
	Printf(format string, v ...interface{})
}

type Loader struct {
	Log             Logger
	DB              db.DB
	Config          Config
	Rand            *rand.Rand
	NumWorkers      int
	AllowedFailFrac float64

	LoadStart int64
	LoadCount int64
}

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

type periodicLogger struct {
	log    Logger
	period time.Duration
	out    func(l Logger)
	done   chan struct{}
	wg     sync.WaitGroup
}

func openPeriodicLogger(log Logger, period time.Duration, out func(l Logger)) *periodicLogger {
	if log == nil {
		return nil
	}
	l := &periodicLogger{
		log:    log,
		period: period,
		out:    out,
		done:   make(chan struct{}),
	}
	l.goPrinter()
	return l
}

func (l *periodicLogger) goPrinter() {
	l.wg.Add(1)
	go func() {
		t := time.NewTicker(l.period)
		defer t.Stop()
		defer l.wg.Done()
		for {
			select {
			case <-t.C:
				l.out(l.log)
			case <-l.done:
				l.out(l.log)
				return
			}
		}
	}()
}

func (l *periodicLogger) Close() {
	if l == nil {
		return
	}
	close(l.done)
	l.wg.Wait()
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
	nfail := new(counter)
	errs := make(chan error)

	msgLogger := openPeriodicLogger(l.Log, 10*time.Second, func(l Logger) {
		done := min(nops.get(), loadCount)
		pctDone := 100 * float64(done) / float64(loadCount)
		l.Printf("%d/%d (%.0f%%) records written", done, loadCount, pctDone)
	})
	defer msgLogger.Close()

	for i := 0; i < l.NumWorkers; i++ {
		go func() {
			var retErr error
			defer func() {
				errs <- retErr
			}()

			for i := nops.getAndInc(); i < loadCount; i = nops.getAndInc() {
				key := keyGen.Next()
				val := valGen.Next()
				if err := l.DB.Put(newCtx, key, val); err != nil {
					curFail := nfail.getAndInc()
					if float64(curFail)/float64(loadCount) > l.AllowedFailFrac {
						retErr = err
						return
					}
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
		g = intgen.NewZipfianN(rsrc, nitems, d.zfTheta())
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
		g = newUniform(rsrc, meanPeriod, d.uniWidth())
	case adPoisson:
		g = newPoisson(rsrc, meanPeriod)
	default:
		panic(fmt.Errorf("invalid arrival dist %v", d))
	}
	return g
}

type Runner struct {
	Log    Logger
	DB     db.DB
	Config Config
	Rand   *rand.Rand
	Trace  []TraceStep

	ReadRecorder  *recorders.Latency
	ReadWriter    *hdrhist.LogWriter
	WriteRecorder *recorders.Latency
	WriteWriter   *hdrhist.LogWriter
}

type result struct {
	latency time.Duration
	err     error
}

func recordAndWrite(c <-chan result, wg *sync.WaitGroup, rec *recorders.Latency, w *hdrhist.LogWriter) {
	for res := range c {
		rec.Record(res.latency, res.err)
	}
	rec.WriteTo(w)
	wg.Done()
}

type resultCounter struct {
	succ int32
	fail int32
}

func (c *resultCounter) countAndFwdTo(fwdC chan<- result) chan<- result {
	ch := make(chan result)
	go func() {
		for r := range ch {
			if r.err != nil {
				atomic.AddInt32(&c.fail, 1)
			} else {
				atomic.AddInt32(&c.succ, 1)
			}
			fwdC <- r
		}
		close(fwdC)
	}()
	return ch
}

func (c *resultCounter) getAndReset() (succ int32, fail int32) {
	succ = atomic.SwapInt32(&c.succ, 0)
	fail = atomic.SwapInt32(&c.fail, 0)
	return succ, fail
}

func (r *Runner) Run(ctx context.Context) error {
	valGen := newValueGen(rand.NewSource(r.Rand.Int63()), r.Config.ValSize)

	for tsIndex, ts := range r.Trace {
		if r.Log != nil {
			r.Log.Printf("starting trace step %d: %s", tsIndex, &ts)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default: // don't wait
		}
		rig := makeSyncGen(ts.ReadKeyDist, rand.NewSource(r.Rand.Int63()), r.Config.RecordCount)
		wig := makeSyncGen(ts.WriteKeyDist, rand.NewSource(r.Rand.Int63()), r.Config.RecordCount)
		readKeyGen := stringGen{G: rig, Len: r.Config.KeySize}
		writeKeyGen := stringGen{G: wig, Len: r.Config.KeySize}

		readRecordC := make(chan result)
		writeRecordC := make(chan result)

		r.ReadRecorder.Reset()
		r.WriteRecorder.Reset()

		var wg sync.WaitGroup
		wg.Add(1)
		go recordAndWrite(readRecordC, &wg, r.ReadRecorder, r.ReadWriter)
		wg.Add(1)
		go recordAndWrite(writeRecordC, &wg, r.WriteRecorder, r.WriteWriter)

		var readCounter, writeCounter resultCounter

		readC := readCounter.countAndFwdTo(readRecordC)
		writeC := writeCounter.countAndFwdTo(writeRecordC)

		msgLogger := openPeriodicLogger(r.Log, 10*time.Second, func(l Logger) {
			rs, rf := readCounter.getAndReset()
			ws, wf := writeCounter.getAndReset()
			l.Printf("since last mesg: %d good, %d errored reads; %d good, %d errored writes", rs, rf, ws, wf)
		})

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

		if ts.ArrivalDist.Kind == adClosed {
			nops := int64(ts.Duration.Seconds() * float64(ts.AvgQPS))
			issueClosed(ctx, args, ts.ArrivalDist.clWorkers(), nops)
		} else {
			meanPeriod := float64(time.Second) / float64(ts.AvgQPS)
			// shrink period so that dist calculation doesn't take too long
			meanPeriod /= 10 * float64(time.Microsecond)
			arrivalGen := makeArrivalDist(ts.ArrivalDist, rand.NewSource(r.Rand.Int63()), float64(meanPeriod))

			issueOpen(ctx, args, arrivalGen, ts.Duration)
		}

		close(readC)
		close(writeC)
		wg.Wait()
		msgLogger.Close()
	}
	return nil
}

type issueArgs struct {
	db          db.DB
	readKeyGen  stringGen
	writeKeyGen stringGen
	valGen      *valueGen
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
		wait := time.Duration(arrivalGen.Next()) * 10 * time.Microsecond
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
		}(rand.New(rand.NewSource(args.rand.Int63())))
	}
	wg.Wait()
}
