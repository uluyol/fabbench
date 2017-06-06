package recorders

import (
	"bytes"
	"compress/gzip"
	"io"
	"net"
	"strconv"
	"sync"
	"time"
	"unicode"

	"github.com/uluyol/fabbench/internal/proto"
)

type AsyncTrace struct {
	C chan *proto.TraceInfo

	closed chan struct{}

	w  *gzip.Writer
	uw io.WriteCloser
}

func NewAsyncTrace(w io.WriteCloser) *AsyncTrace {
	gw, _ := gzip.NewWriterLevel(w, gzip.BestSpeed)
	t := &AsyncTrace{
		uw:     w,
		w:      gw,
		C:      make(chan *proto.TraceInfo, 10),
		closed: make(chan struct{}),
	}
	return t
}

// Do not create more than one consumer
func (t *AsyncTrace) Consume() {
	defer func() { close(t.closed) }()
	defer t.uw.Close()
	defer t.w.Close()
	for ti := range t.C {
		if ti == nil {
			t.w.Flush()
			break
		}
		proto.WriteDelimitedTo(t.w, ti)
	}
}

func (t *AsyncTrace) Close() {
	t.C <- nil
	<-t.closed
}

// Sigh. gocql only exports trace information as text.
// We need to consume this and produce structured data.

func NewTraceConsumer(c chan<- *proto.TraceInfo) io.Writer {
	return &traceConsumer{c: c, curTrace: nil}
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
var bElapsed = []byte("elapsed")

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

			// Use current time for request end.
			// This is not accurate, but we can't do better
			// with gocql's tracing API.
			tc.curTrace.ReqEndTimeMillis = time.Now().Unix() * 1000

			fields := bytes.FieldsFunc(buf, isNotNumLetterDot)
			for i := range fields {
				f := fields[i]
				if bytes.HasPrefix(f, bCoordinator) && i+1 < len(fields) {
					ip := net.ParseIP(string(fields[i+1]))
					if ip == nil {
						tc.curTrace = nil
						break
					}
					if ip.To4() != nil {
						ip = ip.To4()
					}
					tc.curTrace.CoordinatorAddr = []byte(ip)
				} else if bytes.HasPrefix(f, bDuration) && i+1 < len(fields) {
					d, _ := time.ParseDuration(string(fields[i+1]))
					d /= time.Microsecond
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
					if foundSpace {
						startName = i + 1
						break
					}
					foundSpace = true
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
					ip := net.ParseIP(string(fields[i+1]))
					if ip != nil && ip.To4() != nil {
						ip = ip.To4()
					}
					ev.Source = []byte(ip)
				} else if bytes.HasPrefix(f, bElapsed) && i+1 < len(fields) {
					dus, _ := strconv.ParseInt(string(fields[i+1]), 10, 32)
					ev.DurationMicros = int32(dus)
				}
			}
			tc.curTrace.Events = append(tc.curTrace.Events, &ev)
		}
	}

	return len(b), nil
}
