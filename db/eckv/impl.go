package eckv

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/grpc"

	"github.com/uluyol/fabbench/db"
	"github.com/uluyol/fabbench/db/eckv/internal/pb"
)

type client struct {
	cc *grpc.ClientConn
	c  pb.ECKVClient
}

func (c *client) Init(_ context.Context) error { return nil }

func (c *client) Get(ctx context.Context, key string) (string, db.Meta, error) {
	resp, err := c.c.Get(ctx, &pb.GetReq{Key: key})
	if err != nil {
		return "", db.EmptyMeta(), err
	}
	return string(resp.Val), db.EmptyMeta(), err
}

func (c *client) Put(ctx context.Context, key, val string) (db.Meta, error) {
	m := db.EmptyMeta()
	_, err := c.c.Put(ctx, &pb.PutReq{Key: key, Val: []byte(val)})
	return m, err
}

func (c *client) Close() error { return c.cc.Close() }

func makeClient(hosts []string, data []byte) (db.DB, error) {
	if len(hosts) == 0 {
		return nil, errors.New("no host")
	}

	cc, err := grpc.Dial(hosts[0], grpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("unable to connect to host: %v", err)
	}
	return &client{cc: cc, c: pb.NewECKVClient(cc)}, nil
}

func init() {
	db.Register("eckv", makeClient)
}
