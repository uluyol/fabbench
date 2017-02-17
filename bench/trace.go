package bench

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"time"
)

type arrivalDistKind uint8

const (
	adClosed arrivalDistKind = iota + 1
	adUniform
	adPoisson
)

type arrivalDist struct {
	Param1 uint64
	Kind   arrivalDistKind
}

func (d arrivalDist) String() string {
	switch d.Kind {
	case adClosed:
		return fmt.Sprintf("closed-%d", d.clWorkers())
	case adUniform:
		return fmt.Sprintf("uniform-%f", d.uniWidth())
	case adPoisson:
		return "poisson"
	}
	return fmt.Sprintf("unknown(%d)", d)
}

func (d arrivalDist) clWorkers() int {
	if d.Kind != adClosed {
		panic("check arrival dist kind: not closed")
	}
	return int(d.Param1)
}

func (d arrivalDist) uniWidth() float64 {
	if d.Kind != adUniform {
		panic("check arrival dist kind: not uniform")
	}
	return math.Float64frombits(d.Param1)
}

func parseArrivalDist(raw string) (arrivalDist, error) {
	lower := strings.ToLower(raw)
	switch {
	case strings.HasPrefix(lower, "closed"):
		t := strings.TrimPrefix(lower, "closed-")
		w, err := strconv.ParseInt(t, 10, 64)
		if err != nil {
			return arrivalDist{}, fmt.Errorf("bad workers for closed: %v", err)
		}
		return arrivalDist{Kind: adClosed, Param1: uint64(w)}, nil
	case strings.HasPrefix(lower, "uniform"):
		t := strings.TrimPrefix(lower, "uniform-")
		w, err := strconv.ParseFloat(t, 64)
		if err != nil {
			return arrivalDist{}, fmt.Errorf("bad width for uniform: %v", err)
		}
		if w < 0 || w > 1 {
			return arrivalDist{}, errors.New("uniform width must be in [0, 1]")
		}
		return arrivalDist{Kind: adUniform, Param1: math.Float64bits(w)}, nil
	case lower == "poisson":
		return arrivalDist{Kind: adPoisson}, nil
	}
	return arrivalDist{}, fmt.Errorf("unknown arrival distribution: %s", raw)
}

type keyDistKind uint8

const (
	kdUniform keyDistKind = iota + 1
	kdZipfian
	kdLinear
	kdLinStep
)

func (d keyDist) String() string {
	switch d.Kind {
	case kdUniform:
		return "uniform"
	case kdZipfian:
		return fmt.Sprintf("zipfian-%f", d.zfTheta())
	case kdLinear:
		return "linear"
	case kdLinStep:
		return fmt.Sprintf("linstep-%d", d.lsSteps())
	}
	return fmt.Sprintf("unknown(%d)", d)
}

type keyDist struct {
	Param1 uint64
	Kind   keyDistKind
	// have 7 more bytes for params before growing more
}

func (d keyDist) zfTheta() float64 {
	if d.Kind != kdZipfian {
		panic("check key dist kind: not zipfian")
	}
	return math.Float64frombits(d.Param1)
}

func (d keyDist) lsSteps() int64 {
	if d.Kind != kdLinStep {
		panic("check key dist kind: not linstep")
	}
	return int64(d.Param1)
}

func parseKeyDist(raw string) (keyDist, error) {
	lower := strings.ToLower(raw)
	switch {
	case lower == "uniform":
		return keyDist{Kind: kdUniform}, nil
	case lower == "linear":
		return keyDist{Kind: kdLinear}, nil
	case strings.HasPrefix(lower, "zipfian"):
		t := strings.TrimPrefix(lower, "zipfian-")
		theta, err := strconv.ParseFloat(t, 64)
		if err != nil {
			return keyDist{}, fmt.Errorf("bad theta for zipfian: %v", err)
		}
		return keyDist{Kind: kdZipfian, Param1: math.Float64bits(theta)}, nil
	case strings.HasPrefix(lower, "linstep"):
		t := strings.TrimPrefix(lower, "linstep-")
		steps, err := strconv.ParseUint(t, 10, 64)
		if err != nil {
			return keyDist{}, fmt.Errorf("bad step count for linstep: %v", err)
		}
		return keyDist{Kind: kdLinStep, Param1: steps}, nil
	}
	return keyDist{}, fmt.Errorf("unknown key distribution: %s", raw)
}

type TraceStep struct {
	Duration     time.Duration
	ReadKeyDist  keyDist
	WriteKeyDist keyDist
	ArrivalDist  arrivalDist
	RWRatio      float32
	AvgQPS       uint32
}

const (
	durationKey  = "d="
	rwRatioKey   = "rw="
	qpsKey       = "qps="
	distKey      = "ad="
	readDistKey  = "rkd="
	writeDistKey = "wkd="
)

func (t *TraceStep) String() string {
	return fmt.Sprintf("d=%s rw=%f qps=%d ad=%s rkd=%s wkd=%s",
		t.Duration, t.RWRatio, t.AvgQPS, t.ArrivalDist, t.ReadKeyDist, t.WriteKeyDist)
}

func parseTraceStep(data string, step *TraceStep) error {
	fields := strings.Fields(data)
	for _, f := range fields {
		var err error
		switch {
		case strings.HasPrefix(f, durationKey):
			t := strings.TrimPrefix(f, durationKey)
			step.Duration, err = time.ParseDuration(t)
			if err != nil {
				return fmt.Errorf("invalid duration: err")
			}
		case strings.HasPrefix(f, rwRatioKey):
			t := strings.TrimPrefix(f, rwRatioKey)
			r64, err := strconv.ParseFloat(t, 32)
			if err != nil {
				return fmt.Errorf("invalid rw ratio: %v", err)
			}
			step.RWRatio = float32(r64)
		case strings.HasPrefix(f, qpsKey):
			t := strings.TrimPrefix(f, qpsKey)
			u64, err := strconv.ParseUint(t, 10, 32)
			if err != nil {
				return fmt.Errorf("invalid qps: %v", err)
			}
			step.AvgQPS = uint32(u64)
		case strings.HasPrefix(f, distKey):
			t := strings.TrimPrefix(f, distKey)
			step.ArrivalDist, err = parseArrivalDist(t)
			if err != nil {
				return fmt.Errorf("invalid arrival distribution: %v", err)
			}
		case strings.HasPrefix(f, readDistKey):
			t := strings.TrimPrefix(f, readDistKey)
			step.ReadKeyDist, err = parseKeyDist(t)
			if err != nil {
				return fmt.Errorf("invalid read key distribution: %v", err)
			}
		case strings.HasPrefix(f, writeDistKey):
			t := strings.TrimPrefix(f, writeDistKey)
			step.WriteKeyDist, err = parseKeyDist(t)
			if err != nil {
				return fmt.Errorf("invalid write key distribution: %v", err)
			}
		default:
			return fmt.Errorf("unknown key-value: %s", f)
		}
	}
	return nil
}

func ParseTrace(r io.Reader) ([]TraceStep, error) {
	s := bufio.NewScanner(r)
	var steps []TraceStep
	lineno := 0

	// Make sure step retains values across iterations
	// so that lines inherit values from previous ones.
	var step TraceStep

	for s.Scan() {
		lineno++
		if err := parseTraceStep(s.Text(), &step); err != nil {
			return nil, fmt.Errorf("%d: %v", err)
		}
		steps = append(steps, step)
	}
	return steps, nil
}

func PrintTrace(w io.Writer, trace []TraceStep) (n int, err error) {
	for i := range trace {
		t := &trace[i]
		m, err := fmt.Fprintln(w, t)
		n += m
		if err != nil {
			return n, err
		}
	}
	return n, nil
}
