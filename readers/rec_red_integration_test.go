package readers

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/uluyol/fabbench/recorders"
)

func TestLatencyRecorderReaderRoundTrip(t *testing.T) {
	var rec recorders.Latency
	rec.Reset()
	rec.Record(0, nil)
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
	if err := rec.WriteTo(&buf); err != nil {
		t.Fatalf("unexpected error while writing: %v", err)
	}

	res, err := ReadLatency(&buf)
	if err != nil {
		t.Fatalf("unexpected error while reading: %v", err)
	}

	if !res.start.Equal(rec.Start()) {
		t.Errorf("different start times: %v vs %v", rec.Start(), res.start)
	}

	bucketTests := []struct {
		name    string
		wantLen int
		recVal  []uint16
		resVal  []uint16
	}{
		{"us", 4, rec.Micros(), res.us},
		{"ms", 2, rec.Millis(), res.ms},
		{"s", 1, rec.Seconds(), res.s},
	}

	for _, bt := range bucketTests {
		if bt.wantLen != len(bt.recVal) || bt.wantLen != len(bt.resVal) {
			t.Errorf("bucket %s: want %d vals, got rec %d res %d",
				bt.name, bt.wantLen, len(bt.recVal), len(bt.resVal))
		}
	}

	wantDurations := []time.Duration{
		-1,
		0,
		0,
		time.Microsecond,
		time.Millisecond,
		time.Second,
		2 * time.Second,
		time.Duration(^uint16(0)) * time.Second,
	}

	compareVals(t, res.AllVals(), wantDurations)

	if res.errs != 4 {
		t.Errorf("want 4 errors got %d", res.errs)
	}
}

func TestLatencyRecorderReaderMulti(t *testing.T) {
	var buf bytes.Buffer

	var rec recorders.Latency
	rec.Reset()
	start1 := rec.Start()
	rec.Record(0, nil)
	rec.Record(time.Nanosecond, nil)
	rec.Record(time.Microsecond, nil)
	rec.Record(time.Millisecond, nil)
	rec.Record(time.Second, nil)
	rec.Record(2000*time.Millisecond, nil)
	rec.Record(100000*time.Second, nil)
	rec.Record(123123123123, errors.New("dummy0"))

	if err := rec.WriteTo(&buf); err != nil {
		t.Fatalf("unexpected error while writing 1: %v", err)
	}

	if start1 != rec.Start() {
		t.Fatalf("unexpected start time difference after writing 1: first %v then %v", start1, rec.Start)
	}

	rec.Reset()
	start2 := rec.Start()
	rec.Record(time.Microsecond, nil)
	rec.Record(time.Millisecond, nil)
	rec.Record(time.Second, nil)
	rec.Record(2000*time.Millisecond, nil)
	rec.Record(100000*time.Second, nil)
	rec.Record(123123123123, errors.New("dummy0"))

	if err := rec.WriteTo(&buf); err != nil {
		t.Fatalf("unexpected error while writing 2: %v", err)
	}

	if start2 != rec.Start() {
		t.Fatalf("unexpected start time difference after writing 2: first %v then %v", start2, rec.Start)
	}

	rec.Reset()
	start3 := rec.Start()
	rec.Record(0, nil)
	rec.Record(time.Nanosecond, nil)
	rec.Record(time.Microsecond, nil)
	rec.Record(time.Millisecond, nil)
	rec.Record(100000*time.Second, nil)
	rec.Record(123123123123, errors.New("dummy0"))

	if err := rec.WriteTo(&buf); err != nil {
		t.Fatalf("unexpected error while writing 3: %v", err)
	}

	// check written data

	resTests := []struct {
		start time.Time
		len   int
	}{
		{start1, 8},
		{start2, 6},
		{start3, 6},
	}

	got := 0
	for i, test := range resTests {
		res, err := ReadLatency(&buf)
		if err != nil {
			t.Fatalf("unexpected error while reading %d: %v", i, err)
		}

		if !res.start.Equal(test.start) {
			t.Errorf("different start times for %d: want %v got %v", i, test.start, rec.Start())
		}

		if len(res.AllVals()) != test.len {
			t.Errorf("different lengths for %d: want %d got %d", i, test.len, len(res.AllVals()))
		}
		got++
	}

	if got != len(resTests) {
		t.Errorf("wrote %d got %d", got, len(resTests))
	}
}

func compareVals(t *testing.T, vals []HistVal, durations []time.Duration) {
	if len(vals) != len(durations) {
		t.Errorf("different lengths")
	}

	for i := range vals {
		if vals[i].Value != durations[i] {
			t.Errorf("position %d differs: want %s got %s", i, durations[i], vals[i].Value)
		}
	}
}
