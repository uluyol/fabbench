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

	"github.com/google/subcommands"
	"github.com/uluyol/fabbench/bench"
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
			"leveledSSTableSizeMB": INT     // default: 160
			"timeout":              STRING, // default: 5s, time.Duration

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
}

func (*mkTableCmd) Name() string     { return "mktable" }
func (*mkTableCmd) Synopsis() string { return "create the database table" }
func (*mkTableCmd) Usage() string    { return "" }

func (c *mkTableCmd) SetFlags(fs *flag.FlagSet) {
	fs.StringVar(&c.configPath, "config", "", "config file path")
	fs.StringVar(&c.hostsCSV, "hosts", "", "host addresses (comma separated)")
}

func (c *mkTableCmd) Execute(ctx context.Context, fs *flag.FlagSet, args ...interface{}) subcommands.ExitStatus {
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
	configPath string
	hostsCSV   string
	workers    int
	loadStart  int64
	loadCount  int64
}

func (*loadCmd) Name() string     { return "load" }
func (*loadCmd) Synopsis() string { return "load data into the database" }
func (*loadCmd) Usage() string    { return "" }

func (c *loadCmd) SetFlags(fs *flag.FlagSet) {
	fs.StringVar(&c.configPath, "config", "", "config file path")
	fs.StringVar(&c.hostsCSV, "hosts", "", "host addresses (comma separated)")
	fs.IntVar(&c.workers, "workers", 20, "number of concurrent workers")
	fs.Int64Var(&c.loadStart, "start", 0, "index to start loading from")
	fs.Int64Var(&c.loadCount, "count", -1, "records to load, use with start for parallel clients")
}

func (c *loadCmd) Execute(ctx context.Context, fs *flag.FlagSet, args ...interface{}) subcommands.ExitStatus {
	hosts := strings.Split(c.hostsCSV, ",")
	db, bcfg, err := loadConfig(hosts, c.configPath)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	l := bench.Loader{
		Log:        log.New(os.Stderr, "fabbench: load: ", log.LstdFlags),
		DB:         db,
		Config:     *bcfg,
		Rand:       rand.New(rand.NewSource(rand.Int63())),
		NumWorkers: c.workers,
		LoadStart:  c.loadStart,
		LoadCount:  c.loadCount,
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
}

func (*runCmd) Name() string     { return "run" }
func (*runCmd) Synopsis() string { return "run the workload" }
func (*runCmd) Usage() string    { return "" }

func (c *runCmd) SetFlags(fs *flag.FlagSet) {
	fs.StringVar(&c.configPath, "config", "", "config file path")
	fs.StringVar(&c.tracePath, "trace", "", "trace file path")
	fs.StringVar(&c.outPre, "out", "", "output path prefix (will add -ro.gz and -wo.gz)")
	fs.StringVar(&c.hostsCSV, "hosts", "", "host addresses (comma separated)")
}

func (c *runCmd) Execute(ctx context.Context, fs *flag.FlagSet, args ...interface{}) subcommands.ExitStatus {
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

	readF, err := os.Create(c.outPre + "-ro.gz")
	if err != nil {
		log.Fatalf("unable to open read log file: %v", err)
	}
	defer readF.Close()

	writeF, err := os.Create(c.outPre + "-wo.gz")
	if err != nil {
		log.Fatalf("unable to open write log file %v", err)
	}
	defer writeF.Close()

	readW := gzip.NewWriter(readF)
	defer readW.Close()
	writeW := gzip.NewWriter(writeF)
	defer writeW.Close()

	r := bench.Runner{
		Log:         log.New(os.Stderr, "fabbench: run: ", log.LstdFlags),
		DB:          db,
		Config:      *bcfg,
		Rand:        rand.New(rand.NewSource(rand.Int63())),
		Trace:       trace,
		ReadWriter:  readW,
		WriteWriter: writeW,
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
