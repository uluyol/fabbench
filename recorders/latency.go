package recorders

import (
	"strconv"
	"sync"
	"time"

	"github.com/uluyol/hdrhist"
)

type Latency struct {
	mu sync.Mutex

	rec  hdrhist.Recorder
	errs int32

	scratch *hdrhist.Hist
}

func NewLatency(cfg hdrhist.Config) *Latency {
	l := &Latency{}
	l.rec.Init(cfg)
	return l
}

func (r *Latency) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.scratch = r.rec.IntervalHist(r.scratch)
	r.errs = 0
}

func (r *Latency) Record(d time.Duration, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err != nil {
		r.errs++
		return
	}

	r.rec.Record(int64(d))
}

func (r *Latency) WriteTo(w *hdrhist.LogWriter) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	h := r.rec.IntervalHist(r.scratch)
	if err := w.WriteIntervalHist(h); err != nil {
		return err
	}
	err := w.WriteComment("fabbench: error count for previous: " + strconv.Itoa(int(r.errs)))

	r.scratch = h
	return err
}
