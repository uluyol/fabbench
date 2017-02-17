package cassandra

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/gocql/gocql"
	"github.com/uluyol/fabbench/db"
)

type conf struct {
	ClientRetries      *int   `json:"clientRetries",omitempty`
	NumRetries         *int   `json:"numRetries,omitempty"`
	NumConns           *int   `json:"numConns,omitempty"`
	ReadConsistency    string `json:"readConsistency"`
	WriteConsistency   string `json:"writeConsistency"`
	Keyspace           string `json:"keyspace"`
	Table              string `json:"table"`
	ReplicationFactor  *int   `json:"replicationFactor,omitempty"`
	KeyCaching         string `json:"keyCaching"`
	CompactionStrategy string `json:"compactionStrategy"`
	Timeout            string `json:"timeout"`
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
		{&c.NumConns, 20},
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

	session *gocql.Session

	getQPool sync.Pool
	putQPool sync.Pool
}

func (c *client) getQuery() *gocql.Query {
	t := c.getQPool.Get()
	if t != nil {
		if q, ok := t.(*gocql.Query); ok {
			return q
		}
	}
	q := c.session.Query("SELECT vval FROM " + c.conf.Table + " WHERE vkey = ?")
	q.SetConsistency(c.readConsistency)
	return q
}

func (c *client) cacheGetQuery(q *gocql.Query) { c.getQPool.Put(q) }

func (c *client) putQuery() *gocql.Query {
	t := c.putQPool.Get()
	if t != nil {
		if q, ok := t.(*gocql.Query); ok {
			return q
		}
	}
	q := c.session.Query("INSERT INTO " + c.conf.Table + " (vkey, vval) VALUES (?, ?)")
	q.SetConsistency(c.writeConsistency)
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
	cluster.NumConns = *cfg.NumConns
	cluster.Keyspace = cfg.Keyspace

	s, err := cluster.NewSession()
	if err != nil {
		return nil, fmt.Errorf("unable to connect to cluster: %v", err)
	}

	c := &client{
		cluster:          cluster,
		readConsistency:  readConsistency,
		writeConsistency: writeConsistency,
		conf:             cfg,
		session:          s,
	}
	return c, nil
}

func (c *client) Init(ctx context.Context) error {
	session, err := c.cluster.CreateSession()
	if err != nil {
		return fmt.Errorf("unable to connect to cluster: %v", err)
	}
	defer session.Close()

	ks := c.cluster.Keyspace
	c.cluster.Keyspace = ""
	defer func() {
		c.cluster.Keyspace = ks
	}()

	q := session.Query(fmt.Sprintf(
		"CREATE KEYSPACE %s WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': %d}",
		ks, *c.conf.ReplicationFactor))
	if err := q.WithContext(ctx).Exec(); err != nil {
		return fmt.Errorf("unable to create keyspace %s: %v", ks, err)
	}
	q := session.Query(fmt.Sprintf(
		"CREATE TABLE %s.%s (vkey varchar primary key, vval varchar)",
		ks, c.conf.Table))
	if err := q.WithContext(ctx).Exec(); err != nil {
		return fmt.Errorf("unable to create table %s: %v", c.conf.Table, err)
	}
}

func (c *client) Get(ctx context.Context, key string) (string, error) {
	q := c.getQuery()
	var v string
	for retry := 0; retry < *c.conf.ClientRetries; retry++ {
		err := q.Bind(key).WithContext(ctx).Scan(&v)
		switch err {
		case gocql.ErrNoConnections:
			// retry
		default:
			c.cacheGetQuery(q)
			return v, err
		}
	}
	c.cacheGetQuery(q)
	return "", gocql.ErrNoConnections
}

func (c *client) Put(ctx context.Context, key, val string) error {
	q := c.putQuery()

	for retry := 0; retry < *c.conf.ClientRetries; retry++ {
		err := q.Bind(key, val).WithContext(ctx).Exec()
		switch err {
		case gocql.ErrNoConnections:
			// retry
		default:
			c.cachePutQuery(q)
			return err
		}
	}
	c.cachePutQuery(q)
	return gocql.ErrNoConnections
}

func makeDB(hosts []string, data []byte) (db.DB, error) {
	var cfg conf
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("invalid cassandra config: %v", err)
	}
	cfg.fillDefaults()
	return newClient(hosts, &conf)
}

func init() {
	db.Register("cassandra", makeDB)
}
