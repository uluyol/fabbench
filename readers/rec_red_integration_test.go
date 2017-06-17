package readers

import (
	"bytes"
	"errors"
	"math/rand"
	"testing"
	"time"

	"github.com/uluyol/fabbench/recorders"
	"github.com/uluyol/hdrhist"
)

var badRec = errors.New("bad request")

func TestLatencyRecorderRecordsNRecords(t *testing.T) {
	tests := []struct {
		nrec int
		perr float32
	}{
		{1000, 0.3},
		{10000, 0.5},
		{1000, 1},
		{1000, 0},
	}

	for i, test := range tests {
		rec := recorders.NewLatency(hdrhist.Config{
			LowestDiscernible: int64(time.Nanosecond),
			HighestTrackable:  int64(time.Second),
			SigFigs:           3,
			AutoResize:        true,
		}, []string{"ZZZ"})
		rng := rand.New(rand.NewSource(int64(0)))
		for r := 0; r < test.nrec; r++ {
			if rng.Float32() < test.perr {
				rec.Record(0, 0, badRec)
			} else {
				rec.Record(0, 100, nil)
			}
		}
		rd := readerOf(t, rec)

		nreq := numReqs(rd)
		wantreq := int64(test.nrec)
		if nreq != wantreq {
			t.Errorf("case %d: have recorded %d requests, want %d", i, nreq, wantreq)
		}
	}
}

func numReqs(rd *Latency) int64 {
	var c int64
	for _, h := range rd.Hists {
		c += h.TotalCount()
	}
	for _, nerr := range rd.Errs {
		c += int64(nerr)
	}
	return c
}

func readerOf(t *testing.T, rec *recorders.Latency) *Latency {
	var buf bytes.Buffer
	lw := hdrhist.NewLogWriter(&buf)
	if err := rec.WriteTo(lw); err != nil {
		t.Fatalf("unexpected error while writing recorder: %v", err)
	}
	rd, err := ReadLatency(&buf)
	if err != nil {
		t.Fatalf("unexpected error while populating reader: %v", err)
	}
	return rd
}

func TestLatencyRecorderReaderRoundTrip(t *testing.T) {
	t.Parallel()
	rec := recorders.NewLatency(hdrhist.Config{
		LowestDiscernible: int64(time.Microsecond),
		HighestTrackable:  int64(100 * time.Second),
		SigFigs:           3,
		AutoResize:        true,
	}, []string{"zd=2"})
	rec.Start(0)
	rec.Record(0, 1, nil)
	rec.Record(0, time.Nanosecond, nil)
	rec.Record(0, time.Microsecond, nil)
	rec.Record(0, time.Millisecond, nil)
	rec.Record(0, time.Second, nil)
	rec.Record(0, 2000*time.Millisecond, nil)
	rec.Record(0, 100000*time.Second, nil)
	rec.Record(0, 123123123123, errors.New("dummy0"))
	rec.Record(0, 123129993123, errors.New("dummy2"))
	rec.Record(0, 0xffffffaaaf, errors.New("dummy3"))
	rec.Record(0, 12317773, errors.New("dummy4"))
	rec.End(0)

	var buf bytes.Buffer
	lw := hdrhist.NewLogWriter(&buf)
	if err := rec.WriteTo(lw); err != nil {
		t.Fatalf("unexpected error while writing: %v", err)
	}

	res, err := ReadLatency(&buf)
	if err != nil {
		t.Fatalf("unexpected error while reading: %v", err)
	}

	if len(res.Hists) != 1 || len(res.Errs) != 1 {
		t.Fatalf("want 1 hist and err count, got %d and %d", len(res.Hists), len(res.Errs))
	}

	if res.Errs[0] != 4 {
		t.Fatalf("want 4 errors, got %d", res.Errs[0])
	}
}

func TestLatencyRecorderReaderMulti(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	rec := recorders.NewLatency(hdrhist.Config{
		LowestDiscernible: int64(time.Microsecond),
		HighestTrackable:  int64(100 * time.Second),
		SigFigs:           3,
		AutoResize:        true,
	}, []string{"rw=1", "ttt", ";;+99"})

	rec.Start(0)
	rec.Record(0, 0, nil)
	rec.Record(0, time.Nanosecond, nil)
	rec.Record(0, time.Microsecond, nil)
	rec.Record(0, time.Millisecond, nil)
	rec.Record(0, time.Second, nil)
	rec.Record(0, 2000*time.Millisecond, nil)
	rec.Record(0, 100000*time.Second, nil)
	rec.Record(0, 123123123123, errors.New("dummy0"))
	rec.End(0)

	rec.Start(1)
	rec.Record(1, time.Microsecond, nil)
	rec.Record(1, time.Millisecond, nil)
	rec.Record(1, time.Second, nil)
	rec.Record(1, 2000*time.Millisecond, nil)
	rec.Record(1, 100000*time.Second, nil)
	rec.End(1)

	rec.Start(2)
	rec.Record(2, 0, nil)
	rec.Record(2, time.Nanosecond, nil)
	rec.Record(1, 123123123123, errors.New("dummy0"))
	rec.Record(2, time.Millisecond, nil)
	rec.Record(2, 100000*time.Second, nil)
	rec.Record(2, 123123123123, errors.New("dummy0"))
	rec.Record(2, 123123123123, errors.New("dummy2"))
	rec.End(2)

	lw := hdrhist.NewLogWriter(&buf)
	if err := rec.WriteTo(lw); err != nil {
		t.Fatalf("unexpected error while writing 3: %v", err)
	}

	// check written data

	res, err := ReadLatency(&buf)
	if err != nil {
		t.Fatalf("unexpected error while reading: %v", err)
	}

	if len(res.Hists) != 3 || len(res.Errs) != 3 {
		t.Fatalf("want 3 hists and err counts, got %d and %d", len(res.Hists), len(res.Errs))
	}

	resTests := []struct {
		good int64
		errs int32
	}{
		{7, 1},
		{5, 1},
		{4, 2},
	}

	for i, test := range resTests {
		if res.Errs[i] != test.errs {
			t.Errorf("case %d: want %d errs, got %d", i, test.errs, res.Errs[i])
		}
		if res.Hists[i].TotalCount() != test.good {
			t.Errorf("case %d: want %d good, got %d", i, test.good, res.Hists[i].TotalCount())
		}
	}
}
