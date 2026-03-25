package mackerelcache

import (
	"context"

	pb "github.com/chadjefferies/go-mackerelcache/api/mackerelcachepb"
	"github.com/chadjefferies/go-mackerelcache/encoding"
	"github.com/chadjefferies/go-mackerelcache/routing"
)

type Cache[T any] interface {
	Put(ctx context.Context, partition, key string, value T) error
	Get(ctx context.Context, partition, key string) (T, error)
	Delete(ctx context.Context, partition, key string) error
}

type cache[T any] struct {
	client *Client
	router routing.Router
	codec  encoding.Codec[T]
	nodes  []string
}

func (c *cache[T]) Put(ctx context.Context, partition, key string, value T) error {
	ctx, cancel := c.withContext(ctx, partition, key)
	defer cancel()

	encodedVal, err := c.codec.Encode(value)
	if err != nil {
		return err
	}

	req := &pb.PutRequest{
		PartitionKey: partition,
		Key:          key,
		Value:        encodedVal,
	}

	node := c.router.Route(ctx, c.nodes)
	resp, err := c.client.clients[node].Put(ctx, req, defaultCallOpts...)
	if err != nil {
		return err
	}
	if resp.Result != pb.WriteResult_SUCCESS {
		return err
	}
	return err
}

func (c *cache[T]) Get(ctx context.Context, partition, key string) (T, error) {
	ctx, cancel := c.withContext(ctx, partition, key)
	defer cancel()

	req := &pb.GetRequest{
		PartitionKey: partition,
		Key:          key,
	}

	node := c.router.Route(ctx, c.nodes)
	resp, err := c.client.clients[node].Get(ctx, req, defaultCallOpts...)
	if err != nil {
		var zero T
		return zero, err
	}

	val, err := c.codec.Decode(resp.Value)
	if err != nil {
		var zero T
		return zero, err
	}

	return val, nil
}

func (c *cache[T]) Delete(ctx context.Context, partition, key string) error {
	ctx, cancel := c.withContext(ctx, partition, key)
	defer cancel()

	req := &pb.DeleteRequest{
		PartitionKey: partition,
		Key:          key,
	}

	node := c.router.Route(ctx, c.nodes)
	resp, err := c.client.clients[node].Delete(ctx, req, defaultCallOpts...)
	if err != nil {
		return err
	}
	if resp.Result != pb.WriteResult_SUCCESS {
		return err
	}
	return err
}

func (c *cache[T]) withContext(ctx context.Context, partition, key string) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(ctx, c.client.Cfg.Timeout)
	ctx = context.WithValue(ctx, routing.ContextKeyRoutePartition, partition)
	ctx = context.WithValue(ctx, routing.ContextKeyRouteKey, key)
	return ctx, cancel
}

func NewCache[T any](c *Client, router routing.Router, codec encoding.Codec[T]) Cache[T] {
	return &cache[T]{client: c, router: router, codec: codec, nodes: c.Cfg.Endpoints}
}

func NewStringCache(c *Client) Cache[string] {
	return &cache[string]{client: c, router: routing.NewKeyRouter(&routing.ConsistentHashFunction{}), codec: &encoding.StringCodec[string]{}, nodes: c.Cfg.Endpoints}
}
