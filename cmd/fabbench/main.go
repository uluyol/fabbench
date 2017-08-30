package main

import (
	"compress/gzip"
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/google/subcommands"
	"github.com/pkg/profile"
	"github.com/uluyol/fabbench/bench"
	"github.com/uluyol/fabbench/internal/ranges"
	"github.com/uluyol/fabbench/recorders"
	"github.com/uluyol/hdrhist"
)

const formatsDoc = `
CONFIG FORMAT
	The config file is a simple JSON file with the following schema:
		{
			"db": {
				"name": NAME_STR,
				"options": DB_SPECIFIC_OPTIONS
			},
			"workload": {
				"recordCount": RECORDS_INT,
				"keySize": KEY_BYTES_INT,
				"valSize": VAL_BYTES_INT,
			}
		}

	Because fabbench is meant to handle multiple databases, db.options is
	db-specific.

	Currently, fabbench supports the following databases:
		eckv:      ECKV
		dummy:     A dummy database useful for testing
		cassandra: Apache Cassandra via gocql

	For eckv, there is no schema for db.options. Use {}

	For dummy, the schema for db.options is
		{
			"maxQPS": INT // optional
		}

	For cassandra, the schema for db.options is
		{
			"clientRetries":        INT,    // optional
			"numRetries":           INT,    // optional
			"numConns":             INT,    // optional
			"readConsistency":      STRING, // default: ONE
			"writeConsistency":     STRING, // default: ONE
			"keyspace":             STRING, // optional
			"table":                STRING, // optional
			"replicationFactor":    INT,
			"keyCaching":           STRING,
			"compactionStrategy":   STRING,
			"leveledSSTableSizeMB": INT,    // default: 160
			"connectTimeout":       STRING, // default: 5s, time.Duration
			"timeout":              STRING, // default: 600ms, time.Duration

			"traceData": STRING, // path to store traces
			"traceRate": INT     // freq for tracing i.e. every N requests
		}

TRACE FORMAT
	The trace consists of a series of lines with property values set.
	Each line is a step of the trace and inherits values in previous lines.
	Empty lines are ignored.

	The properties are listed below:
		d		duration of trace step (e.g. 5m3s)
		rw		frac of requests that are reads (e.g. 0.8 -> 80% are reads)
		qps		avg requests per second during trace step
		ad		request interarrival distribution
		rkd		key distribution for reads
		wkd		key distribution for writes

	Valid values for these properties are below
		d		any valid time.Duration in Go
		rw		any float in [0, 1]
		qps		any non-negative integer
		ad		poisson:    poisson dist with avg qps
				closed-N:   closed-loop workload of qps*d ops with N workers
				uniform-W:  uniform dist with vals in [avg-avg*W, avg+avg*W]
		rkd		zipfian-θ:  zipfian with param of θ in (0, 1)
				linstep-K:  PDF linearly dec in K steps
				linear:     linearly dec PDF
				uniform:    uniform
		wkd		same options as rkd

	For example, a valid trace line might be
		d=10m rw=0.5 qps=500 ad=poisson rkd=zipfian-0.99999 wkd=uniform
`

type nopStop struct{}

func (nopStop) Stop() {}

type baseFlags struct {
	profPath string
	prof     string
}

func (f *baseFlags) setupProfiling() interface {
	Stop()
} {
	if f.profPath != "" {
		opts := []func(*profile.Profile){profile.ProfilePath(f.profPath)}
		switch f.prof {
		case "cpu":
			opts = append(opts, profile.CPUProfile)
		case "mutex":
			opts = append(opts, profile.MutexProfile)
		case "block":
			opts = append(opts, profile.BlockProfile)
		default:
			// ignore
		}
		return profile.Start(opts...)
	}
	return nopStop{}
}

func (f *baseFlags) SetFlags(fs *flag.FlagSet) {
	fs.StringVar(&f.profPath, "profiledir", "", "turn profiling on and write profiles to this directory")
	fs.StringVar(&f.prof, "profile", "cpu", "resource to profile (possible values: cpu, mutex, block)")
}

type formatsCmd struct{}

func (formatsCmd) Name() string           { return "formats" }
func (formatsCmd) Synopsis() string       { return "describes the config and trace formats" }
func (formatsCmd) Usage() string          { return "" }
func (formatsCmd) SetFlags(*flag.FlagSet) {}

func (formatsCmd) Execute(_ context.Context, _ *flag.FlagSet, args ...interface{}) subcommands.ExitStatus {
	fmt.Fprintln(os.Stderr, formatsDoc)
	return subcommands.ExitSuccess
}

type mkTableCmd struct {
	configPath string
	hostsCSV   string
	baseFlags
}

func (*mkTableCmd) Name() string     { return "mktable" }
func (*mkTableCmd) Synopsis() string { return "create the database table" }
func (*mkTableCmd) Usage() string    { return "" }

func (c *mkTableCmd) SetFlags(fs *flag.FlagSet) {
	fs.StringVar(&c.configPath, "config", "", "config file path")
	fs.StringVar(&c.hostsCSV, "hosts", "", "host addresses (comma separated)")
	c.baseFlags.SetFlags(fs)
}

func (c *mkTableCmd) Execute(ctx context.Context, fs *flag.FlagSet, args ...interface{}) subcommands.ExitStatus {
	defer c.setupProfiling().Stop()
	hosts := strings.Split(c.hostsCSV, ",")
	db, _, err := loadConfig(hosts, c.configPath)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	if err := db.Init(ctx); err != nil {
		log.Fatal(err)
	}

	return subcommands.ExitSuccess
}

type loadCmd struct {
	configPath  string
	hostsCSV    string
	workers     int
	maxFailFrac float64

	loadStart int64
	loadCount int64

	nshard int64
	shardi int64

	baseFlags
}

func (*loadCmd) Name() string     { return "load" }
func (*loadCmd) Synopsis() string { return "load data into the database" }
func (*loadCmd) Usage() string {
	return `fabbench load populates the database.

For loading from multiple processes in parallel (potentially across machines),
use either the -start and -count flags, or the -nshard and -shardi flags.
If both pairs of flags are passed, one set will be used arbitrarily.

`
}

func (c *loadCmd) SetFlags(fs *flag.FlagSet) {
	fs.StringVar(&c.configPath, "config", "", "config file path")
	fs.StringVar(&c.hostsCSV, "hosts", "", "host addresses (comma separated)")
	fs.IntVar(&c.workers, "workers", 20, "number of concurrent workers")
	fs.Float64Var(&c.maxFailFrac, "maxfailfrac", 0.01, "fraction of records that are allowed to fail to load")

	fs.Int64Var(&c.loadStart, "start", 0, "index to start loading from (use either start+count, or nshard+shardi)")
	fs.Int64Var(&c.loadCount, "count", -1, "number of records to load (use either start+count, or nshard+shardi)")

	fs.Int64Var(&c.nshard, "nshard", 0, "number of parallel worker processes (use either start+count, or nshard+shardi)")
	fs.Int64Var(&c.shardi, "shardi", 0, "parallel worker process index (use either start+count, or nshard+shardi)")
	c.baseFlags.SetFlags(fs)
}

func (c *loadCmd) Execute(ctx context.Context, fs *flag.FlagSet, args ...interface{}) subcommands.ExitStatus {
	defer c.setupProfiling().Stop()
	hosts := strings.Split(c.hostsCSV, ",")
	db, bcfg, err := loadConfig(hosts, c.configPath)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	loadStart := c.loadStart
	loadCount := c.loadCount
	if c.nshard > 0 {
		shards := ranges.SplitRecords(bcfg.RecordCount, c.nshard)
		if c.shardi < 0 || c.shardi > int64(len(shards)) {
			log.Fatalf("invalid value for -shardi: %d, number of shards: %d", c.shardi, len(shards))
		}
		loadStart = shards[c.shardi].Start
		loadCount = shards[c.shardi].Count
	}

	l := bench.Loader{
		Log:             log.New(os.Stderr, "fabbench: load: ", log.LstdFlags),
		DB:              db,
		Config:          *bcfg,
		Rand:            rand.New(rand.NewSource(rand.Int63())),
		NumWorkers:      c.workers,
		AllowedFailFrac: c.maxFailFrac,
		LoadStart:       loadStart,
		LoadCount:       loadCount,
	}

	if err := l.Run(ctx); err != nil {
		log.Fatal(err)
	}

	return subcommands.ExitSuccess
}

type runCmd struct {
	configPath string
	tracePath  string
	outPre     string
	hostsCSV   string
	baseFlags
}

func (*runCmd) Name() string     { return "run" }
func (*runCmd) Synopsis() string { return "run the workload" }
func (*runCmd) Usage() string    { return "" }

func (c *runCmd) SetFlags(fs *flag.FlagSet) {
	fs.StringVar(&c.configPath, "config", "", "config file path")
	fs.StringVar(&c.tracePath, "trace", "", "trace file path")
	fs.StringVar(&c.outPre, "out", "", "output path prefix (will add -ro.gz and -wo.gz)")
	fs.StringVar(&c.hostsCSV, "hosts", "", "host addresses (comma separated)")
	c.baseFlags.SetFlags(fs)
}

func (c *runCmd) Execute(ctx context.Context, fs *flag.FlagSet, args ...interface{}) subcommands.ExitStatus {
	defer c.setupProfiling().Stop()
	hosts := strings.Split(c.hostsCSV, ",")
	db, bcfg, err := loadConfig(hosts, c.configPath)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	trace, err := loadTrace(c.tracePath)
	if err != nil {
		log.Fatalf("unable to load trace: %v", err)
	}

	benchStart := time.Now()
	readw := recorders.NewMultiLogWriter(c.outPre+"-ro", benchStart, gzip.BestSpeed)
	writew := recorders.NewMultiLogWriter(c.outPre+"-wo", benchStart, gzip.BestSpeed)

	hdrCfg := hdrhist.Config{
		LowestDiscernible: int64(10 * time.Microsecond),
		HighestTrackable:  int64(100 * time.Second),
		SigFigs:           3,
		AutoResize:        true,
	}

	traceDescs := make([]string, len(trace))
	for i := range trace {
		traceDescs[i] = trace[i].String()
	}

	readRec := recorders.NewMultiLatency(hdrCfg, traceDescs)
	writeRec := recorders.NewMultiLatency(hdrCfg, traceDescs)

	r := bench.Runner{
		Log:           log.New(os.Stderr, "fabbench: run: ", log.LstdFlags),
		DB:            db,
		Config:        *bcfg,
		Rand:          rand.New(rand.NewSource(rand.Int63())),
		Trace:         trace,
		ReadRecorder:  readRec,
		ReadWriter:    readw,
		WriteRecorder: writeRec,
		WriteWriter:   writew,
	}

	if err := r.Run(ctx); err != nil {
		log.Print(err)
		return subcommands.ExitFailure
	}

	return subcommands.ExitSuccess
}

func loadTrace(p string) ([]bench.TraceStep, error) {
	f, err := os.Open(p)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	t, err := bench.ParseTrace(f)
	if err != nil {
		return nil, fmt.Errorf("%s:%v", p, err)
	}

	return t, nil
}

func main() {
	log.SetPrefix("fabbench: ")
	log.SetFlags(0)
	subcommands.Register(subcommands.HelpCommand(), "")
	subcommands.Register(subcommands.FlagsCommand(), "")
	subcommands.Register(subcommands.CommandsCommand(), "")
	subcommands.Register(formatsCmd{}, "")
	subcommands.Register(new(mkTableCmd), "")
	subcommands.Register(new(loadCmd), "")
	subcommands.Register(new(runCmd), "")

	flag.Parse()
	os.Exit(int(subcommands.Execute(context.Background())))
}
