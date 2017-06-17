package recorders

import (
	"strconv"
	"time"

	"github.com/uluyol/hdrhist"
)

type MultiLatency struct {
	cfg   hdrhist.Config
	descs []string
	all   Latency
	sub   map[string]*Latency
}

func NewMultiLatency(cfg hdrhist.Config, descs []string) *MultiLatency {
	ml := &MultiLatency{
		cfg:   cfg,
		descs: descs,
		sub:   make(map[string]*Latency),
	}
	ml.all.init(cfg, descs)
	return ml
}

func (r *MultiLatency) SetStart(step int, t time.Time) {
	if r == nil {
		return
	}

	r.all.SetStart(step, t)
	for _, l := range r.sub {
		l.SetStart(step, t)
	}
}

func (r *MultiLatency) SetEnd(step int, t time.Time) {
	if r == nil {
		return
	}

	r.all.SetEnd(step, t)
	for _, l := range r.sub {
		l.SetEnd(step, t)
	}
}

func (r *MultiLatency) Record(name string, step int, d time.Duration, e error) {
	if r == nil {
		return
	}

	r.all.Record(step, d, e)
	l, ok := r.sub[name]
	if !ok {
		l = NewLatency(r.cfg, r.descs)
		r.sub[name] = l
	}
	l.Record(step, d, e)
}

func (r *MultiLatency) WriteTo(w MultiLogWriter) error {
	if r == nil {
		return nil
	}
	if err := w.WriteAll(r.all.WriteTo); err != nil {
		return err
	}
	for name, l := range r.sub {
		if err := w.Write(name, l.WriteTo); err != nil {
			return err
		}
	}

	return nil
}

// Latency records latencies. It is NOT safe for concurrent use.
type Latency struct {
	recs  []hdrhist.Hist
	errs  []int32
	descs []string
}

func (l *Latency) init(cfg hdrhist.Config, steps []string) {
	l.recs = make([]hdrhist.Hist, len(steps))
	l.errs = make([]int32, len(steps))
	l.descs = steps
	for i := range l.recs {
		l.recs[i].Init(cfg)
	}
}

func NewLatency(cfg hdrhist.Config, steps []string) *Latency {
	var l Latency
	l.init(cfg, steps)
	return &l
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
		err := w.WriteComment("fabbench: desc for previous: " + r.descs[i])
		if err != nil {
			return err
		}
		err = w.WriteComment("fabbench: error count for previous: " + strconv.Itoa(int(r.errs[i])))
		if err != nil {
			return err
		}
	}

	return nil
}
