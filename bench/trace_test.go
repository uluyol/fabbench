package bench

import (
	"bytes"
	"strings"
	"testing"
)

func TestTraceRoundTrip(t *testing.T) {
	tests := []struct{ in, out string }{
		{
			in: `
d=30m rw=0.5 qps=500 ad=poisson rkd=zipfian-0.9999 wkd=uniform
d=10m
ad=uniform-0.6
qps=200 rkd=linstep-5
			`,
			out: `
d=30m0s rw=0.500000 qps=500 ad=poisson rkd=zipfian-0.999900 wkd=uniform
d=10m0s rw=0.500000 qps=500 ad=poisson rkd=zipfian-0.999900 wkd=uniform
d=10m0s rw=0.500000 qps=500 ad=uniform-0.600000 rkd=zipfian-0.999900 wkd=uniform
d=10m0s rw=0.500000 qps=200 ad=uniform-0.600000 rkd=linstep-5 wkd=uniform
`,
		}, {
			in: `
d=30m rw=0.2 qps=500 ad=closed-50 rkd=zipfian-0.9999 wkd=linear
d=10m
ad=uniform-0.2 wkd=zipfian-0.9
ad=closedtime-10
qps=200 rkd=linstep-5
`,
			out: `
d=30m0s rw=0.200000 qps=500 ad=closed-50 rkd=zipfian-0.999900 wkd=linear
d=10m0s rw=0.200000 qps=500 ad=closed-50 rkd=zipfian-0.999900 wkd=linear
d=10m0s rw=0.200000 qps=500 ad=uniform-0.200000 rkd=zipfian-0.999900 wkd=zipfian-0.900000
d=10m0s rw=0.200000 qps=500 ad=closedtime-10 rkd=zipfian-0.999900 wkd=zipfian-0.900000
d=10m0s rw=0.200000 qps=200 ad=closedtime-10 rkd=linstep-5 wkd=zipfian-0.900000
`,
		},
	}

	for i, test := range tests {
		trace, err := ParseTrace(strings.NewReader(test.in))
		if err != nil {
			t.Errorf("case %d: unable to parse trace: %v", i, err)
			continue
		}
		var buf bytes.Buffer
		_, err = PrintTrace(&buf, trace)
		if err != nil {
			t.Errorf("case %d: error writing trace: %v", i, err)
		}
		if buf.String() != strings.TrimPrefix(test.out, "\n") {
			t.Errorf("case %d:\nwant:\n%s\ngot:\n%s\n", i, strings.TrimPrefix(test.out, "\n"), buf.String())
		}
	}
}