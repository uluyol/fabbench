package recorders

import (
	"bytes"
	"compress/gzip"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/uluyol/hdrhist"
)

type MultiLogWriter interface {
	WriteAll(f func(*hdrhist.LogWriter) error) error
	Write(name string, f func(*hdrhist.LogWriter) error) error
	Err() error

	private()
}

type mLogWriter struct {
	gzLevel int
	out     string
	err     error
	start   time.Time
}

// NewMultiWriter creates a multi-writer that writes data in two locations:
// - out.gz: aggregate latency histograms over time.
// - out-sub: directory containing specific latency histograms.
func NewMultiLogWriter(out string, start time.Time, gzipLevel int) MultiLogWriter {
	return &mLogWriter{gzLevel: gzipLevel, out: out, start: start}
}

// Write creates a file for and writes the data for the aggregate data.
// If an error has already occured, the previous error is returned immediately
// and nothing is written to disk.
func (mw *mLogWriter) WriteAll(f func(*hdrhist.LogWriter) error) error {
	return mw.writeToFile(mw.out+".gz", f)
}

func (mw *mLogWriter) writeToFile(path string, fn func(*hdrhist.LogWriter) error) error {
	if mw.err != nil {
		return mw.err
	}
	f, err := os.Create(path)
	if err != nil {
		mw.err = err
		return mw.err
	}
	defer f.Close()
	gw, err := gzip.NewWriterLevel(f, mw.gzLevel)
	if err != nil {
		mw.err = err
		return mw.err
	}
	defer gw.Close()
	lw := hdrhist.NewLogWriter(gw)
	if err := lw.WriteStartTime(mw.start); err != nil {
		mw.err = err
		return mw.err
	}
	lw.SetBaseTime(mw.start)
	if err := lw.WriteLegend(); err != nil {
		mw.err = err
		return mw.err
	}
	if err := fn(lw); err != nil {
		mw.err = err
		return mw.err
	}
	return nil
}

// Write creates a file for and writes the data for the specified name.
// If an error has already occured, the previous error is returned immediately
// and nothing is written to disk.
func (mw *mLogWriter) Write(name string, f func(*hdrhist.LogWriter) error) error {
	if mw.err != nil {
		return mw.err
	}
	dirname := mw.out + "-sub"
	_, err := os.Stat(dirname)
	if err != nil {
		if err := os.Mkdir(dirname, 0777); err != nil {
			mw.err = err
			return mw.err
		}
	}
	return mw.writeToFile(filepath.Join(dirname, name+".gz"), f)
}

func (mw *mLogWriter) Err() error {
	return mw.err
}

func (mw *mLogWriter) private() {}

type MemoryMultiLogWriter struct {
	start time.Time
	all   bytes.Buffer
	logs  map[string]*bytes.Buffer
	err   error
}

func NewMemoryMultiLogWriter(start time.Time) *MemoryMultiLogWriter {
	return &MemoryMultiLogWriter{start: start, logs: make(map[string]*bytes.Buffer)}
}

func (mw *MemoryMultiLogWriter) WriteAll(f func(*hdrhist.LogWriter) error) error {
	if mw.err != nil {
		return mw.err
	}
	if err := f(mw.logWriter(&mw.all)); err != nil {
		mw.err = err
		return mw.err
	}
	return mw.err
}

func (mw *MemoryMultiLogWriter) logWriter(buf *bytes.Buffer) *hdrhist.LogWriter {
	lw := hdrhist.NewLogWriter(buf)
	if err := lw.WriteStartTime(mw.start); err != nil {
		mw.err = err
	}
	lw.SetBaseTime(mw.start)
	if err := lw.WriteLegend(); err != nil {
		mw.err = err
	}
	return lw
}

func (mw *MemoryMultiLogWriter) Write(name string, f func(*hdrhist.LogWriter) error) error {
	if mw.err != nil {
		return mw.err
	}
	buf, ok := mw.logs[name]
	if !ok {
		buf = new(bytes.Buffer)
		mw.logs[name] = buf
	}
	if err := f(mw.logWriter(buf)); err != nil {
		mw.err = err
		return err
	}
	return nil
}

func (mw *MemoryMultiLogWriter) AllReader() io.Reader {
	return &mw.all
}

func (mw *MemoryMultiLogWriter) Reader(name string) io.Reader {
	return mw.logs[name]
}

func (mw *MemoryMultiLogWriter) Err() error { return mw.err }
func (mw *MemoryMultiLogWriter) private()   {}
