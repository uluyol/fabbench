package recorders

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"strconv"
	"sync"
	"unicode"

	"github.com/uluyol/fabbench/internal/proto"
)

type AsyncTrace struct {
	C chan *proto.TraceInfo

	w *gzip.Writer
}

func NewAsyncTrace(w io.Writer) *AsyncTrace {
	t := &AsyncTrace{
		w: gzip.NewWriter(w),
		C: make(chan *proto.TraceInfo),
	}
	return t
}

// Do not create more than one consumer
func (t *AsyncTrace) Consume(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			break
		case ti := <-t.C:
			if ti == nil {
				break
			}
			proto.WriteDelimitedTo(t.w, ti)
		}
	}
}

// Sigh. gocql only exports trace information as text.
// We need to consume this and produce structured data.

func NewTraceConsumer(c chan<- *proto.TraceInfo) io.Writer {
	return &traceConsumer{c: c, curTrace: new(proto.TraceInfo)}
}

type traceConsumer struct {
	mu  sync.Mutex
	buf bytes.Buffer

	c        chan<- *proto.TraceInfo
	curTrace *proto.TraceInfo
}

var sessionPre = []byte("Tracing session")
var errPre = []byte("Error:")
var bCoordinator = []byte("coordinator")
var bDuration = []byte("duration")
var bSource = []byte("source")

func isNotNumLetterDot(c rune) bool {
	return !unicode.IsNumber(c) && !unicode.IsLetter(c) && c != '.'
}

func (tc *traceConsumer) Write(b []byte) (n int, err error) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	tc.buf.Write(b)

	for {
		nl := bytes.IndexByte(tc.buf.Bytes(), '\n')
		if nl < 0 {
			break
		}
		buf := make([]byte, nl+1)
		tc.buf.Read(buf)
		switch {
		case bytes.HasPrefix(buf, errPre):
			tc.curTrace = nil
		case bytes.HasPrefix(buf, sessionPre):
			if tc.curTrace != nil {
				tc.c <- tc.curTrace
			}
			tc.curTrace = new(proto.TraceInfo)
			fields := bytes.FieldsFunc(buf, isNotNumLetterDot)
			for i := range fields {
				f := fields[i]
				if bytes.HasPrefix(f, bCoordinator) && i+1 < len(fields) {
					tc.curTrace.CoordinatorAddr = fields[i+1]
				} else if bytes.HasPrefix(f, bDuration) && i+1 < len(fields) {
					d, _ := time.ParseDuration(string(fields[i+1]))
					d /= time.Microseconds
					tc.curTrace.DurationMicros = int32(d)
				}
			}
		default:
			if tc.curTrace == nil {
				// skip if we haven't seen a header or got an error
				continue
			}
			// event
			endName := bytes.LastIndexByte(buf, '(') - 1
			if endName < 0 {
				continue
			}
			foundSpace := false
			startName := -1
			for i := range buf {
				if buf[i] == ' ' {
					if found {
						startName = i + 1
						break
					}
					found = true
				}
			}
			if startName < 0 || startName >= len(buf) {
				continue
			}
			ev := proto.Event{
				Desc: string(buf[startName:endName]),
			}

			fields := bytes.FieldsFunc(buf, isNotNumLetterDot)
			for i := range fields {
				f := fields[i]
				if bytes.HasPrefix(f, bSource) && i+1 < len(fields) {
					ev.Source = fields[i+1]
				} else if bytes.HasPrefix(f, bElapsed) && i+1 < len(fields) {
					dus, _ := strconv.Atoi(fields[i+1])
					ev.DurationMicros = dus
				}
			}
			tc.curTrace.Events = append(tc.curTrace.Events, &ev)
		}
	}

	return len(b), nil
}
