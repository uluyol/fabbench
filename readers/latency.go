package readers

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"strconv"
	"strings"

	"github.com/uluyol/hdrhist"
)

type Latency struct {
	Hists []*hdrhist.Hist
	Errs  []int32
}

const errPrefix = "fabbench: error count for previous: "

func ReadLatency(r io.Reader) (*Latency, error) {
	b, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}

	var l Latency

	s := bufio.NewScanner(bytes.NewReader(b))
	for s.Scan() {
		t := s.Text()
		if strings.HasPrefix(t, "#") {
			// is comment
			t = strings.TrimSpace(t[1:])
			if strings.HasPrefix(t, errPrefix) {
				t = strings.TrimPrefix(t, errPrefix)
				ec, err := strconv.Atoi(t)
				if err != nil {
					return nil, fmt.Errorf("unable to read error count: %v", err)
				}
				l.Errs = append(l.Errs, int32(ec))
			}
		}
	}

	// don't need to check s.Err() since it's reading from a bytes.Reader

	hr := hdrhist.NewLogReader(bytes.NewReader(b))
	for hr.Scan() {
		l.Hists = append(l.Hists, hr.Hist())
	}

	if hr.Err() != nil {
		return nil, fmt.Errorf("unable to read hist: %v", err)
	}

	if len(l.Errs) != len(l.Hists) {
		return nil, errors.New("number of hists and steps for errors do not match")
	}

	return &l, err
}
