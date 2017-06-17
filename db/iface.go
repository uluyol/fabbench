package db

import (
	"context"
	"fmt"
)

type DB interface {
	Init(ctx context.Context) error
	Get(ctx context.Context, key string) (string, Meta, error)
	Put(ctx context.Context, key, val string) (Meta, error)
	Close() error
}

var dbs = make(map[string]func(h []string, b []byte) (DB, error))

func Register(name string, mkDB func(hosts []string, data []byte) (DB, error)) {
	dbs[name] = mkDB
}

func Dial(name string, hosts []string, cfgData []byte) (DB, error) {
	if mkDB, ok := dbs[name]; ok {
		return mkDB(hosts, cfgData)
	}
	return nil, fmt.Errorf("unknown db: %s", name)
}

type Meta interface {
	Value(key interface{}) interface{}
}

type meta struct {
	key    interface{}
	val    interface{}
	parent Meta
}

func (m *meta) Value(k interface{}) interface{} {
	if m.key == k {
		return m.val
	}
	return m.parent.Value(k)
}

func MetaWithValue(parent Meta, key interface{}, val interface{}) Meta {
	return &meta{
		key:    key,
		val:    val,
		parent: parent,
	}
}

type emptyMeta struct{}

func (m emptyMeta) Value(_ interface{}) interface{} { return nil }

func EmptyMeta() Meta { return emptyMeta{} }

type HostInfo interface {
	ID() string
}

type hostInfoKey struct{}

func MetaWithHostInfo(parent Meta, hi HostInfo) Meta {
	return MetaWithValue(parent, hostInfoKey{}, hi)
}

func GetHostInfo(m Meta) (HostInfo, bool) {
	v := m.Value(hostInfoKey{})
	if v == nil {
		return nil, false
	}
	return v.(HostInfo), true
}
