package bench

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"runtime"
	"testing"
	"time"

	"github.com/uluyol/fabbench/db"
	_ "github.com/uluyol/fabbench/db/dummy"
	"github.com/uluyol/fabbench/readers"
	"github.com/uluyol/fabbench/recorders"
	"github.com/uluyol/hdrhist"
)

type tLogger struct {
	t *testing.T
}

func (l tLogger) Print(args ...interface{}) {
	l.t.Log(args...)
}

func (l tLogger) Printf(format string, args ...interface{}) {
	l.t.Logf(format, args...)
}

func TestLoadRun(t *testing.T) {
	t.Parallel()
	conn, err := db.Dial("dummy", nil, []byte{})
	t.Log(conn, err)
	defer conn.Close()
	cfg := Config{
		RecordCount: 1e3,
		KeySize:     1 << 8,
		ValSize:     1 << 12,
	}
	l := Loader{
		Log:             tLogger{t},
		DB:              conn,
		Config:          cfg,
		Rand:            rand.New(rand.NewSource(0)),
		NumWorkers:      4 * runtime.NumCPU(),
		AllowedFailFrac: 0,
	}
	if err := l.Run(context.Background()); err != nil {
		t.Fatalf("unable to load: %v", err)
	}
	r := Runner{
		Log:    tLogger{t},
		DB:     conn,
		Config: cfg,
		Rand:   rand.New(rand.NewSource(0)),
		Trace:  mustMakeTrace([]string{"d=1s rw=0.5 qps=1000 ad=poisson rkd=uniform wkd=uniform"}),
	}
	if err := r.Run(context.Background()); err != nil {
		t.Fatalf("unable to run: %v", err)
	}
	r.Trace = mustMakeTrace([]string{"d=1s rw=0.9 qps=10000 ad=closed-17 rkd=zipfian-0.99 wkd=linear"})
	if err := r.Run(context.Background()); err != nil {
		t.Fatalf("unable to run: %v", err)
	}
}

func TestRunClosedOps(t *testing.T) {

}

type nopWriter struct{}

func (w nopWriter) Write(b []byte) (int, error) {
	return len(b), nil
}

func TestRunOpenQPS(t *testing.T) {
	tests := []struct {
		trace []string
	}{
		{[]string{"rkd=zipfian-0 wkd=uniform rw=0.5 d=1s ad=poisson qps=500", "qps=300", "qps=10000", "qps=30000"}},
		{[]string{"rkd=linear wkd=uniform rw=0.7 ad=uniform-0.2 d=1s qps=100", "qps=700", "qps=18000", "qps=25000"}},
	}

	conn, err := db.Dial("dummy", nil, nil)
	if err != nil {
		t.Fatal("failed to setup: %v", err)
	}
	defer conn.Close()
	cfg := Config{
		RecordCount: 1e3,
		KeySize:     1 << 8,
		ValSize:     1 << 6,
	}
	hcfg := hdrhist.Config{
		LowestDiscernible: 1,
		HighestTrackable:  1e6,
		SigFigs:           3,
		AutoResize:        true,
	}
	for i, test := range tests {
		trace := mustMakeTrace(test.trace)

		var rbuf, wbuf bytes.Buffer

		{
			rr := recorders.NewLatency(hcfg, len(trace))
			wr := recorders.NewLatency(hcfg, len(trace))
			rw := hdrhist.NewLogWriter(&rbuf)
			ww := hdrhist.NewLogWriter(&wbuf)
			r := Runner{
				Log:    tLogger{t},
				DB:     conn,
				Config: cfg,
				Rand:   rand.New(rand.NewSource(883)),
				Trace:  trace,

				ReadRecorder:  rr,
				WriteRecorder: wr,
				ReadWriter:    rw,
				WriteWriter:   ww,
			}

			if err := r.Run(context.Background()); err != nil {
				t.Errorf("case %d: unable to run: %v", i, err)
				continue
			}
		}

		rr, err := readers.ReadLatency(&rbuf)
		if err != nil {
			t.Fatalf("case %d: unable to read written read latencies: %v", i, err)
		}
		wr, err := readers.ReadLatency(&wbuf)
		if err != nil {
			t.Fatalf("case %d: unable to read written write latencies: %v", i, err)
		}

		for step := range trace {
			if rr.Errs[step] != 0 {
				t.Errorf("case %d: step %d: got read errors", i, step)
				continue
			}
			have := float64(rr.Hists[step].TotalCount()) / (float64(trace[step].Duration) / float64(time.Second))
			want := float64(trace[step].AvgQPS) * float64(trace[step].RWRatio)
			if have < want*0.8 || want*1.1 < have {
				t.Errorf("case %d: step %d: have %f r/s, want %f r/s", i, step, have, want)
			}
			if wr.Errs[step] != 0 {
				t.Errorf("case %d: step %d: got write errors", i, step)
				continue
			}
			have = float64(wr.Hists[step].TotalCount()) / (float64(trace[step].Duration) / float64(time.Second))
			want = float64(trace[step].AvgQPS) * float64(1-trace[step].RWRatio)
			if have < want*0.8 || want*1.1 < have {
				t.Errorf("case %d: step %d: have %f w/s, want %f w/s", i, step, have, want)
			}
		}
	}
}

type linesReader struct {
	remaining []string
	lf        bool
}

func (r *linesReader) Read(b []byte) (int, error) {
	if r.lf {
		if len(b) == 0 {
			return 0, nil
		}
		b[0] = '\n'
		r.lf = false
		return 1, nil
	}
	if len(r.remaining) <= 0 {
		return 0, io.EOF
	}
	n := copy(b, r.remaining[0])
	r.remaining[0] = r.remaining[0][n:]
	if len(r.remaining[0]) <= 0 {
		r.remaining = r.remaining[1:]
		r.lf = true
	}
	return n, nil
}

func mustMakeTrace(lines []string) []TraceStep {
	trace, err := ParseTrace(&linesReader{lines, false})
	if err != nil {
		panic(fmt.Errorf("couldn't parse trace: %v", err))
	}
	return trace
}
