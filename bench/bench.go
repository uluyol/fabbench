package bench

import (
	"context"
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/uluyol/fabbench/db"
	"github.com/uluyol/fabbench/internal/ranges"
	"github.com/uluyol/fabbench/internal/syncrand"
	"github.com/uluyol/fabbench/intgen"
	"github.com/uluyol/fabbench/recorders"
)

type Config struct {
	_           struct{}
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
	_               struct{}
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
	done   chan struct{}
	prDone <-chan struct{}
}

func openPeriodicLogger(log Logger, period time.Duration, out func(l Logger)) *periodicLogger {
	if log == nil {
		return nil
	}
	pdone := make(chan struct{})
	l := &periodicLogger{
		done:   make(chan struct{}),
		prDone: pdone,
	}
	l.goPrinter(log, period, out, pdone)
	return l
}

func (l *periodicLogger) goPrinter(log Logger, period time.Duration, out func(l Logger), pdone chan<- struct{}) {
	go func() {
		t := time.NewTicker(period)
		defer t.Stop()
		defer func() {
			close(pdone)
		}()
		for {
			select {
			case <-t.C:
				out(log)
			case <-l.done:
				out(log)
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
	<-l.prDone
}

func (l *Loader) Run(ctx context.Context) error {
	keyGen := stringGen{
		G:   &intgen.Counter{Count: l.LoadStart},
		Len: l.Config.KeySize,
	}
	valGen := newValueGen(l.Config.ValSize)

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
		go func(rng *rand.Rand) {
			var retErr error
			defer func() {
				errs <- retErr
			}()

			for i := nops.getAndInc(); i < loadCount; i = nops.getAndInc() {
				key := keyGen.Next(rng)
				val := valGen.Next(rng)
				if _, err := l.DB.Put(newCtx, key, val); err != nil {
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
		}(rand.New(rand.NewSource(l.Rand.Int63())))
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

type zfArgs struct {
	nitems  int64
	zfTheta float64
}

var zfGenCache = struct {
	mu    sync.Mutex
	items map[zfArgs]intgen.Gen
}{
	items: make(map[zfArgs]intgen.Gen),
}

func makeReqGen(d keyDist, nitems int64) intgen.Gen {
	var g intgen.Gen
	switch d.Kind {
	case kdUniform:
		g = intgen.NewUniform(nitems)
	case kdZipfian:
		zfGenCache.mu.Lock()
		defer zfGenCache.mu.Unlock()
		g = zfGenCache.items[zfArgs{nitems, d.zfTheta()}]
		if g == nil {
			g = intgen.NewZipfianN(nitems, d.zfTheta())
			zfGenCache.items[zfArgs{nitems, d.zfTheta()}] = g
		}
	case kdLinear:
		g = intgen.NewLinear(nitems)
	case kdLinStep:
		g = intgen.NewLinearStep(nitems, d.lsSteps())
	default:
		panic(fmt.Errorf("invalid key dist %v", d))
	}
	return g
}

func makeArrivalDist(d arrivalDist, meanPeriod float64) intgen.Gen {
	var g intgen.Gen
	switch d.Kind {
	case adClosed:
		g = closed{}
	case adUniform:
		g = newUniform(meanPeriod, d.uniWidth())
	case adPoisson:
		g = newPoisson(meanPeriod)
	default:
		panic(fmt.Errorf("invalid arrival dist %v", d))
	}
	return g
}

type Runner struct {
	_      struct{}
	Log    Logger
	DB     db.DB
	Config Config
	Rand   *rand.Rand
	Trace  []TraceStep

	ReadRecorder  *recorders.MultiLatency
	ReadWriter    recorders.MultiLogWriter
	WriteRecorder *recorders.MultiLatency
	WriteWriter   recorders.MultiLogWriter
}

type result struct {
	_    struct{}
	step int

	// optional, ignores name, latency, err if present
	timeBeg *time.Time
	timeEnd *time.Time

	name    string
	latency time.Duration
	err     error

	isDone bool
}

func resDoneReq(step int, name string, latency time.Duration, err error) result {
	return result{
		step:    step,
		name:    name,
		latency: latency,
		err:     err,
	}
}

func resBegin(step int, t time.Time) result {
	return result{step: step, timeBeg: &t}
}

func resEnd(step int, t time.Time) result {
	return result{step: step, timeEnd: &t}
}

func resRunIsDone() result {
	return result{step: -1, isDone: true}
}

func recordAndWrite(c <-chan result, wg *sync.WaitGroup, rec *recorders.MultiLatency, w recorders.MultiLogWriter) {
	for res := range c {
		exitLoop := false
		switch {
		case res.isDone:
			exitLoop = true
		case res.timeBeg != nil:
			rec.SetStart(res.step, *res.timeBeg)
		case res.timeEnd != nil:
			rec.SetEnd(res.step, *res.timeEnd)
		default:
			rec.Record(res.name, res.step, res.latency, res.err)
		}
		if exitLoop {
			break
		}
	}
	rec.WriteTo(w)
	wg.Done()
}

type resultCounter struct {
	succ int32
	fail int32
}

func (c *resultCounter) countAndFwdTo(fwdC chan<- result) chan<- result {
	ch := make(chan result, cap(fwdC))
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

func (r *Runner) Run(parentCtx context.Context) error {
	valGen := newValueGen(r.Config.ValSize)

	var runWG sync.WaitGroup

	readRecordC := make(chan result, 2*runtime.NumCPU())
	writeRecordC := make(chan result, 2*runtime.NumCPU())

	runWG.Add(1)
	go recordAndWrite(readRecordC, &runWG, r.ReadRecorder, r.ReadWriter)
	runWG.Add(1)
	go recordAndWrite(writeRecordC, &runWG, r.WriteRecorder, r.WriteWriter)

	var readCounter, writeCounter resultCounter

	readC := readCounter.countAndFwdTo(readRecordC)
	writeC := writeCounter.countAndFwdTo(writeRecordC)

	msgLogger := openPeriodicLogger(r.Log, 10*time.Second, func(l Logger) {
		rs, rf := readCounter.getAndReset()
		ws, wf := writeCounter.getAndReset()
		l.Printf("since last mesg: %d good, %d errored reads; %d good, %d errored writes", rs, rf, ws, wf)
	})

	var reqWG sync.WaitGroup

	ctx, cancelCtx := context.WithCancel(parentCtx)

	for tsIndex, ts := range r.Trace {
		if r.Log != nil {
			r.Log.Printf("starting trace step %d: %s", tsIndex, &ts)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default: // don't wait
		}
		rig := makeReqGen(ts.ReadKeyDist, r.Config.RecordCount)
		wig := makeReqGen(ts.WriteKeyDist, r.Config.RecordCount)
		readKeyGen := stringGen{G: rig, Len: r.Config.KeySize}
		writeKeyGen := stringGen{G: wig, Len: r.Config.KeySize}

		args := issueArgs{
			db:          r.DB,
			readKeyGen:  readKeyGen,
			writeKeyGen: writeKeyGen,
			valGen:      valGen,
			rwRatio:     ts.RWRatio,
			rand:        r.Rand,
			readC:       readC,
			writeC:      writeC,
			tsStep:      tsIndex,
		}

		start := time.Now()
		readC <- resBegin(tsIndex, start)
		writeC <- resBegin(tsIndex, start)
		if ts.ArrivalDist.Kind == adClosed {
			nops := int64(ts.Duration.Seconds() * float64(ts.AvgQPS))
			issueClosed(ctx, args, &reqWG, ts.ArrivalDist.clWorkers(), nops)
		} else {
			shards := ranges.SplitRecords(int64(ts.AvgQPS), int64(runtime.NumCPU()))
			//shards := ranges.SplitRecords(int64(ts.AvgQPS), 1)
			var wg sync.WaitGroup
			wg.Add(len(shards))
			for w := range shards {
				meanPeriod := float64(time.Second) / float64(shards[w].Count)
				// shrink period so that dist calculation doesn't take too long
				meanPeriod /= float64(time.Microsecond)
				arrivalGen := makeArrivalDist(ts.ArrivalDist, float64(meanPeriod))
				args.rand = rand.New(rand.NewSource(r.Rand.Int63()))
				go func(wargs issueArgs, wag intgen.Gen, dur time.Duration) {
					issueOpen(ctx, wargs, &reqWG, wag, dur)
					wg.Done()
				}(args, arrivalGen, ts.Duration)
			}
			wg.Wait()
		}
		end := time.Now()
		readC <- resEnd(tsIndex, end)
		writeC <- resEnd(tsIndex, end)
	}

	cancelCtx()

	readC <- resRunIsDone()
	writeC <- resRunIsDone()

	msgLogger.Close()
	runWG.Wait()

	return nil
}

type issueArgs struct {
	db          db.DB
	readKeyGen  stringGen
	writeKeyGen stringGen
	valGen      *valueGen
	rwRatio     float32
	rand        *rand.Rand
	tsStep      int

	readC, writeC chan<- result
}

func issueOpen(ctx context.Context, args issueArgs, reqWG *sync.WaitGroup, arrivalGen intgen.Gen, execDuration time.Duration) {
	shardedRand := syncrand.NewSharded(args.rand)
	reqi := 0
	start := time.Now()
	plannedStart := start
	for time.Since(start) < execDuration {
		reqi++
		if reqi%128 == 0 {
			select {
			case <-ctx.Done():
				break
			default: // don't wait
			}
		}
		nextIsRead := args.rand.Float32() < args.rwRatio
		sleepDur := time.Duration(arrivalGen.Next(args.rand)) * time.Microsecond
		plannedStart = plannedStart.Add(sleepDur)
		time.Sleep(plannedStart.Sub(time.Now()))
		reqWG.Add(1)
		go func(rng *rand.Rand, reqStart time.Time, isRead bool) {
			defer reqWG.Done()
			if isRead {
				key := args.readKeyGen.Next(rng)
				_, meta, err := args.db.Get(ctx, key)
				latency := time.Since(reqStart)
				args.readC <- resDoneReq(args.tsStep, getHost(meta), latency, err)
			} else {
				key := args.writeKeyGen.Next(rng)
				val := args.valGen.Next(rng)
				meta, err := args.db.Put(ctx, key, val)
				latency := time.Since(start)
				args.writeC <- resDoneReq(args.tsStep, getHost(meta), latency, err)
			}
		}(shardedRand.Get(reqi), time.Now(), nextIsRead)
	}
}

func issueClosed(ctx context.Context, args issueArgs, _ *sync.WaitGroup, workers int, totalOps int64) {
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
					key := args.readKeyGen.Next(rng)
					_, meta, err := args.db.Get(ctx, key)
					latency := time.Since(start)
					args.readC <- resDoneReq(args.tsStep, getHost(meta), latency, err)
				} else {
					key := args.writeKeyGen.Next(rng)
					val := args.valGen.Next(rng)
					meta, err := args.db.Put(ctx, key, val)
					latency := time.Since(start)
					args.writeC <- resDoneReq(args.tsStep, getHost(meta), latency, err)
				}
			}
		}(rand.New(rand.NewSource(args.rand.Int63())))
	}
	wg.Wait()
}

func getHost(m db.Meta) string {
	if hi, ok := db.GetHostInfo(m); ok {
		return hi.ID()
	}
	return ""
}
