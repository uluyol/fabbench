package db

import (
	"context"
	"fmt"
)

type DB interface {
	Init(ctx context.Context) error
	Get(ctx context.Context, key string) (string, error)
	Put(ctx context.Context, key, val string) error
}

var dbs = make(map[string]func(h []string, b []byte) (DB, error))

func Register(name string, mkDB func(hosts []string, data []byte) (DB, error)) {
	db[name] = mkDB
}

func Dial(name string, hosts []string, cfgData []byte) (DB, error) {
	if mkDB, ok := dbs[name]; ok {
		return mkDB(hosts, cdfData)
	}
	return fmt.Errorf("unknown db: %s", name)
}
