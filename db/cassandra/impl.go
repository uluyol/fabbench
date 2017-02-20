package cassandra

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gocql/gocql"
	"github.com/uluyol/fabbench/db"
	"github.com/uluyol/fabbench/recorders"
)

type conf struct {
	ClientRetries        *int   `json:"clientRetries",omitempty`
	NumRetries           *int   `json:"numRetries,omitempty"`
	NumConns             *int   `json:"numConns,omitempty"`
	ReadConsistency      string `json:"readConsistency"`
	WriteConsistency     string `json:"writeConsistency"`
	Keyspace             string `json:"keyspace"`
	Table                string `json:"table"`
	ReplicationFactor    *int   `json:"replicationFactor,omitempty"`
	KeyCaching           string `json:"keyCaching"`
	CompactionStrategy   string `json:"compactionStrategy"`
	LeveledSSTableSizeMB *int   `json:"leveledSSTableSizeMB"`
	Timeout              string `json:"timeout"`

	TraceData *string `json:"traceData",omitempty`
	TraceRate *int32  `json:"traceRate",omitempty`
}

func newInt(v int) *int { return &v }

func (c *conf) fillDefaults() {
	intFields := []struct {
		val **int
		def int
	}{
		{&c.ClientRetries, 10},
		{&c.NumRetries, 4},
		{&c.ReplicationFactor, 3},
		{&c.NumConns, 4},
		{&c.LeveledSSTableSizeMB, 160},
	}
	for _, f := range intFields {
		if *f.val == nil {
			*f.val = newInt(f.def)
		}
	}

	strFields := []struct {
		val *string
		def string
	}{
		{&c.ReadConsistency, "ONE"},
		{&c.WriteConsistency, "ONE"},
		{&c.Keyspace, "fabbench"},
		{&c.Table, "udata"},
		{&c.KeyCaching, "ALL"},
		{&c.CompactionStrategy, "LeveledCompactionStrategy"},
		{&c.Timeout, "5s"},
	}
	for _, f := range strFields {
		if *f.val == "" {
			*f.val = f.def
		}
	}
}

func parseConsistency(s string) (c gocql.Consistency, err error) {
	defer func() {
		if e := recover(); e != nil {
			if er, ok := e.(error); ok {
				err = er
				return
			}
			panic(e)
		}
	}()
	return gocql.ParseConsistency(s), nil
}

type client struct {
	cluster          *gocql.ClusterConfig
	readConsistency  gocql.Consistency
	writeConsistency gocql.Consistency
	conf             *conf

	session struct {
		once sync.Once
		s    *gocql.Session
		err  error
	}

	rat     *recorders.AsyncTrace
	wat     *recorders.AsyncTrace
	rtracer gocql.Tracer
	wtracer gocql.Tracer

	getQPool sync.Pool
	putQPool sync.Pool

	opCount int32
}

func (c *client) getSession() (*gocql.Session, error) {
	c.session.once.Do(func() {
		c.session.s, c.session.err = c.cluster.CreateSession()
		c.rtracer = gocql.NewTraceWriter(c.session.s, recorders.NewTraceConsumer(c.rat.C))
		c.wtracer = gocql.NewTraceWriter(c.session.s, recorders.NewTraceConsumer(c.wat.C))
	})

	return c.session.s, c.session.err
}

func (c *client) traceQuery(q *gocql.Query, read bool) *gocql.Query {
	if c.rtracer != nil && c.wtracer != nil {
		tracer := c.rtracer
		if !read {
			tracer = c.wtracer
		}
		if atomic.AddInt32(&c.opCount, 1)%*c.conf.TraceRate == 0 {
			return q.Trace(tracer)
		}
	}
	return q
}

func (c *client) getQuery(s *gocql.Session) *gocql.Query {
	t := c.getQPool.Get()
	if t != nil {
		if q, ok := t.(*gocql.Query); ok {
			return q
		}
	}
	q := s.Query("SELECT vval FROM " + c.conf.Table + " WHERE vkey = ?")
	q.Consistency(c.readConsistency)
	return q
}

func (c *client) cacheGetQuery(q *gocql.Query) { c.getQPool.Put(q) }

func (c *client) putQuery(s *gocql.Session) *gocql.Query {
	t := c.putQPool.Get()
	if t != nil {
		if q, ok := t.(*gocql.Query); ok {
			return q
		}
	}
	q := s.Query("INSERT INTO " + c.conf.Table + " (vkey, vval) VALUES (?, ?)")
	q.Consistency(c.writeConsistency)
	return q
}

func (c *client) cachePutQuery(q *gocql.Query) { c.putQPool.Put(q) }

func newClient(hosts []string, cfg *conf) (db.DB, error) {
	timeout, err := time.ParseDuration(cfg.Timeout)
	if err != nil {
		return nil, fmt.Errorf("error parsing timeout as time.Duration: %v")
	}

	readConsistency, err := parseConsistency(cfg.ReadConsistency)
	if err != nil {
		return nil, fmt.Errorf("invalid read consistency: %v")
	}
	writeConsistency, err := parseConsistency(cfg.WriteConsistency)
	if err != nil {
		return nil, fmt.Errorf("invalid write consistency: %v")
	}

	cluster := gocql.NewCluster(hosts...)
	cluster.ProtoVersion = 4
	cluster.RetryPolicy = &gocql.SimpleRetryPolicy{NumRetries: *cfg.NumRetries}
	cluster.Timeout = timeout
	cluster.SocketKeepalive = 30 * time.Second
	cluster.NumConns = *cfg.NumConns

	// Test that we can create a session.
	// We don't know if the Keyspace exists yet,
	// so we can't actually create the session used for gets/puts now.
	// We'll create it when the first request starts executing.
	//
	// For now, just make sure we can connect so we can give sane errors.
	s, err := cluster.CreateSession()
	if err != nil {
		return nil, fmt.Errorf("unable to connect to db: %v", err)
	}
	s.Close()

	cluster.Keyspace = cfg.Keyspace

	var rat, wat *recorders.AsyncTrace
	if cfg.TraceRate != nil && cfg.TraceData != nil {
		rf, err := os.Create(*cfg.TraceData + "-ro.gz")
		if err != nil {
			return nil, fmt.Errorf("unable to open ro trace file: %v", err)
		}
		wf, err := os.Create(*cfg.TraceData + "-wo.gz")
		if err != nil {
			return nil, fmt.Errorf("unable to open wo trace file: %v", err)
		}
		rat = recorders.NewAsyncTrace(rf)
		wat = recorders.NewAsyncTrace(wf)
		go rat.Consume()
		go wat.Consume()
	}

	c := &client{
		cluster:          cluster,
		readConsistency:  readConsistency,
		writeConsistency: writeConsistency,
		conf:             cfg,
		rat:              rat,
		wat:              wat,
	}
	return c, nil
}

func (c *client) Init(ctx context.Context) error {
	ks := c.cluster.Keyspace
	c.cluster.Keyspace = ""

	defer func() {
		c.cluster.Keyspace = ks
	}()

	session, err := c.cluster.CreateSession()
	if err != nil {
		return fmt.Errorf("unable to connect to db to make table: %v", err)
	}
	defer session.Close()

	q := session.Query(fmt.Sprintf(
		"CREATE KEYSPACE %s WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': %d}",
		ks, *c.conf.ReplicationFactor))
	if err := q.WithContext(ctx).Exec(); err != nil {
		return fmt.Errorf("unable to create keyspace %s: %v", ks, err)
	}
	q = session.Query(fmt.Sprintf(
		"CREATE TABLE %s.%s (vkey varchar primary key, vval varchar) WITH compaction = {'class': '%s', 'sstable_size_in_mb': %d} AND caching = {'keys': '%s'}",
		ks, c.conf.Table, c.conf.CompactionStrategy, *c.conf.LeveledSSTableSizeMB, c.conf.KeyCaching))
	if err := q.WithContext(ctx).Exec(); err != nil {
		return fmt.Errorf("unable to create table %s: %v", c.conf.Table, err)
	}
	return nil
}

func exponentialRetry(maxTries int, f func() error) error {
	err := f()
	if err == nil {
		return nil
	}
	sleep := 5 * time.Millisecond
	maxSleep := time.Second
	for retry := 0; retry < maxTries; retry++ {
		time.Sleep(sleep)
		err = f()
		if err == nil {
			return nil
		}
		sleep *= 5
		if sleep > maxSleep {
			sleep = maxSleep
		}
	}
	return err
}

func (c *client) Get(ctx context.Context, key string) (string, error) {
	s, err := c.getSession()
	if err != nil {
		return "", fmt.Errorf("unable to connect to db: %v", err)
	}
	q := c.getQuery(s)
	var v string
	err = exponentialRetry(*c.conf.ClientRetries, func() error {
		return c.traceQuery(q.Bind(key).WithContext(ctx), true).Scan(&v)
	})
	c.cacheGetQuery(q)
	return v, err
}

func (c *client) Put(ctx context.Context, key, val string) error {
	s, err := c.getSession()
	if err != nil {
		return fmt.Errorf("unable to connect to db: %v", err)
	}
	q := c.putQuery(s)
	err = exponentialRetry(*c.conf.ClientRetries, func() error {
		return c.traceQuery(q.Bind(key, val).WithContext(ctx), false).Exec()
	})
	c.cachePutQuery(q)
	return err
}

func (c *client) Close() error {
	c.rat.Close()
	c.wat.Close()
	return nil
}

func makeDB(hosts []string, data []byte) (db.DB, error) {
	var cfg conf
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("invalid cassandra config: %v", err)
	}
	cfg.fillDefaults()
	return newClient(hosts, &cfg)
}

func init() {
	db.Register("cassandra", makeDB)
}
