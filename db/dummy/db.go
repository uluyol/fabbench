package dummy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/uluyol/fabbench/db"
)

type client struct {
	_closed int32
}

var errClosed = errors.New("db is closed")

func (c *client) isClosed() bool { return atomic.LoadInt32(&c._closed) != 0 }

func (c *client) Close() error {
	atomic.StoreInt32(&c._closed, 1)
	return nil
}

func (c *client) Init(_ context.Context) error {
	if c.isClosed() {
		return errClosed
	}
	return nil
}

func (c *client) Get(_ context.Context, key string) (string, error) {
	if c.isClosed() {
		return "", errClosed
	}
	return key + "-value", nil
}

func (c *client) Put(_ context.Context, key, val string) error {
	if c.isClosed() {
		return errClosed
	}
	return nil
}

func init() {
	db.Register("dummy", func(_ []string, data []byte) (db.DB, error) {
		type conf struct {
			MaxQPS *int `json:"maxQPS",omitempty`
		}
		var cfg conf
		if err := json.Unmarshal(data, &cfg); err != nil {
			return nil, fmt.Errorf("invalid dummy config: %v", err)
		}
		if cfg.MaxQPS == nil {
			return &client{}, nil
		} else {
			c := &rtClient{
				Latency: time.Second / time.Duration(*cfg.MaxQPS),
			}
			return c, nil
		}
	})
}
