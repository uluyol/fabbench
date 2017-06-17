package dummy

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/uluyol/fabbench/db"
)

type rtClient struct {
	Latency time.Duration
	reqs    chan<- struct{}
	resps   <-chan struct{}
	_closed int32
	sinit   sync.Once
}

func (c *rtClient) isClosed() bool { return atomic.LoadInt32(&c._closed) != 0 }

func (c *rtClient) Close() error {
	atomic.StoreInt32(&c._closed, 1)
	return nil
}

// TODO: remove garbage goroutine.
func (c *rtClient) doReq(ctx context.Context) error {
	if c.isClosed() {
		return errClosed
	}
	c.sinit.Do(func() {
		reqs := make(chan struct{})
		resps := make(chan struct{})
		c.reqs = reqs
		c.resps = resps
		go func() {
			for range reqs {
				time.Sleep(c.Latency)
				resps <- struct{}{}
			}
			close(resps)
		}()
	})
	select {
	case <-ctx.Done():
		return ctx.Err()
	case c.reqs <- struct{}{}:
		<-c.resps
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	}
}

func (c *rtClient) Init(ctx context.Context) error {
	return c.doReq(ctx)
}

func (c *rtClient) Get(ctx context.Context, key string) (string, db.Meta, error) {
	if err := c.doReq(ctx); err != nil {
		return "", db.EmptyMeta(), err
	}
	return key + "-value", db.EmptyMeta(), nil
}

func (c *rtClient) Put(ctx context.Context, key, val string) (db.Meta, error) {
	return db.EmptyMeta(), c.doReq(ctx)
}
