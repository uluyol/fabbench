package dummy

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
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
	case c.reqs <- struct{}{}:
		<-c.resps
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *rtClient) Init(ctx context.Context) error {
	return c.doReq(ctx)
}

func (c *rtClient) Get(ctx context.Context, key string) (string, error) {
	if err := c.doReq(ctx); err != nil {
		return "", err
	}
	return key + "-value", nil
}

func (c *rtClient) Put(ctx context.Context, key, val string) error {
	return c.doReq(ctx)
}
