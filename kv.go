package consul

import "context"

type kv struct {
}

func (c *Client) readKey(ctx context.Context, key string) (kv kv, err error) {
	return
}

func (c *Client) acquireKey(ctx context.Context, key string, modifyIndex uint64) (err error) {
	err = c.Put(ctx, "/v1/kv/"+key, Query{
		{},
	}, nil, nil)
	return
}

func (c *Client) releaseKey(ctx context.Context, key string, modifyIndex uint64) (err error) {
	return
}
