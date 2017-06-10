package bench

import (
	"context"
	"fmt"
	"math/rand"
	"runtime"
	"testing"
	"time"

	"github.com/uluyol/fabbench/db"
	_ "github.com/uluyol/fabbench/db/dummy"
)

func TestLoadRun(t *testing.T) {
	t.Parallel()
	conn, _ := db.Dial("dummy", nil, nil)
	defer conn.Close()
	cfg := Config{
		RecordCount: 1e3,
		KeySize:     1 << 8,
		ValSize:     1 << 12,
	}
	l := Loader{
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
		DB:           conn,
		Config:       cfg,
		Rand:         rand.New(rand.NewSource(0)),
		Trace:        mustMakeTrace([]string{"d=1s rw=0.5 qps=1000 ad=poisson rkd=uniform wkd=uniform"}),
		ReqTimeout:   10 * time.Second,
		MaxWorkerQPS: 103,
	}
	if err := r.Run(context.Background()); err != nil {
		t.Fatalf("unable to run: %v", err)
	}
	r.Trace = mustMakeTrace([]string{"d=1s rw=0.9 qps=10000 ad=closed-17 rkd=zipfian-0.99 wkd=linear"})
	if err := r.Run(context.Background()); err != nil {
		t.Fatalf("unable to run: %v", err)
	}
}

func mustMakeTrace(lines []string) []TraceStep {
	var trace []TraceStep
	for _, l := range lines {
		var ts TraceStep
		if err := parseTraceStep(l, &ts); err != nil {
			panic(fmt.Errorf("couldn't parse trace: %v", err))
		}
		trace = append(trace, ts)
	}
	return trace
}
