package readers

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/uluyol/fabbench/recorders"
	"github.com/uluyol/hdrhist"
)

func TestLatencyRecorderReaderRoundTrip(t *testing.T) {
	rec := recorders.NewLatency(hdrhist.Config{
		LowestDiscernible: int64(time.Microsecond),
		HighestTrackable:  int64(100 * time.Second),
		SigFigs:           3,
		AutoResize:        true,
	})
	rec.Reset()
	rec.Record(1, nil)
	rec.Record(time.Nanosecond, nil)
	rec.Record(time.Microsecond, nil)
	rec.Record(time.Millisecond, nil)
	rec.Record(time.Second, nil)
	rec.Record(2000*time.Millisecond, nil)
	rec.Record(100000*time.Second, nil)
	rec.Record(123123123123, errors.New("dummy0"))
	rec.Record(123129993123, errors.New("dummy2"))
	rec.Record(0xffffffaaaf, errors.New("dummy3"))
	rec.Record(12317773, errors.New("dummy4"))

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
	var buf bytes.Buffer
	rec := recorders.NewLatency(hdrhist.Config{
		LowestDiscernible: int64(time.Microsecond),
		HighestTrackable:  int64(100 * time.Second),
		SigFigs:           3,
		AutoResize:        true,
	})

	rec.Reset()
	rec.Record(0, nil)
	rec.Record(time.Nanosecond, nil)
	rec.Record(time.Microsecond, nil)
	rec.Record(time.Millisecond, nil)
	rec.Record(time.Second, nil)
	rec.Record(2000*time.Millisecond, nil)
	rec.Record(100000*time.Second, nil)
	rec.Record(123123123123, errors.New("dummy0"))

	lw := hdrhist.NewLogWriter(&buf)
	if err := rec.WriteTo(lw); err != nil {
		t.Fatalf("unexpected error while writing 1: %v", err)
	}

	rec.Reset()
	rec.Record(time.Microsecond, nil)
	rec.Record(time.Millisecond, nil)
	rec.Record(time.Second, nil)
	rec.Record(2000*time.Millisecond, nil)
	rec.Record(100000*time.Second, nil)
	rec.Record(123123123123, errors.New("dummy0"))

	if err := rec.WriteTo(lw); err != nil {
		t.Fatalf("unexpected error while writing 2: %v", err)
	}

	rec.Reset()
	rec.Record(0, nil)
	rec.Record(time.Nanosecond, nil)
	rec.Record(time.Millisecond, nil)
	rec.Record(100000*time.Second, nil)
	rec.Record(123123123123, errors.New("dummy0"))
	rec.Record(123123123123, errors.New("dummy2"))

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
