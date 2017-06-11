package recorders

import (
	"strconv"
	"time"

	"github.com/uluyol/hdrhist"
)

// Latency records latencies. It is NOT safe for concurrent use.
type Latency struct {
	recs []hdrhist.Hist
	errs []int32
}

func NewLatency(cfg hdrhist.Config, numStep int) *Latency {
	l := &Latency{
		recs: make([]hdrhist.Hist, numStep),
		errs: make([]int32, numStep),
	}
	for i := range l.recs {
		l.recs[i].Init(cfg)
	}
	return l
}

func (r *Latency) Start(step int) { r.SetStart(step, time.Now()) }
func (r *Latency) End(step int)   { r.SetEnd(step, time.Now()) }

func (r *Latency) SetStart(step int, t time.Time) {
	if r == nil {
		return
	}

	r.recs[step].Clear()
	r.recs[step].SetStartTime(t)
	r.errs[step] = 0
}

func (r *Latency) SetEnd(step int, t time.Time) {
	if r == nil {
		return
	}

	r.recs[step].SetEndTime(t)
}

func (r *Latency) Record(step int, d time.Duration, err error) {
	if r == nil {
		return
	}
	if err != nil {
		r.errs[step]++
		return
	}

	r.recs[step].Record(int64(d))
}

func (r *Latency) WriteTo(w *hdrhist.LogWriter) error {
	if r == nil {
		return nil
	}
	for i := range r.recs {
		if err := w.WriteIntervalHist(&r.recs[i]); err != nil {
			return err
		}
		err := w.WriteComment("fabbench: error count for previous: " + strconv.Itoa(int(r.errs[i])))
		if err != nil {
			return err
		}
	}

	return nil
}
