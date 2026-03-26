package mackerelcache

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	pb "github.com/chadjefferies/go-mackerelcache/api/mackerelcachepb"
)

type Cache[T any] interface {
	Put(ctx context.Context, partition, key string, value T) error
	PutMany(ctx context.Context, partition string, items map[string]T) error
	Get(ctx context.Context, partition, key string) (T, error)
	GetMany(ctx context.Context, partition string, key []string) (map[string]T, error)
	Delete(ctx context.Context, partition, key string) error
	DeleteMany(ctx context.Context, partition string, keys []string) (int32, error)
}

type cache[T any] struct {
	client *Client
	router Router
	codec  Codec[T]
	nodes  []string
}

func (c *cache[T]) Put(ctx context.Context, partition, key string, value T) error {
	ctx, cancel := context.WithTimeout(ctx, c.client.Cfg.Timeout)
	defer cancel()
	ctx = withRoutingValues(ctx, partition, key)

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
		return errors.New("failed to put value: " + resp.Result.String())
	}
	return err
}

func (c *cache[T]) PutMany(ctx context.Context, partition string, items map[string]T) error {
	ctx, cancel := context.WithTimeout(ctx, c.client.Cfg.Timeout)
	defer cancel()

	itemMap := make(map[string]map[string][]byte)
	for k, v := range items {
		ctx := withRoutingValues(ctx, partition, k)
		node := c.router.Route(ctx, c.nodes)
		val, err := c.codec.Encode(v)
		if err != nil {
			return err
		}

		if _, ok := itemMap[node]; !ok {
			itemMap[node] = make(map[string][]byte)
		}

		itemMap[node][k] = val
	}

	var wg sync.WaitGroup
	l := len(itemMap)
	errc := make(chan error, l)
	wg.Add(l)
	for node, entries := range itemMap {
		go func() {
			defer wg.Done()
			req := &pb.PutManyRequest{
				PartitionKey: partition,
				Entries:      entries,
			}
			resp, err := c.client.clients[node].PutMany(ctx, req, defaultCallOpts...)
			if err != nil {
				errc <- err
			} else {
				if resp.Result != pb.WriteResult_SUCCESS {
					errc <- errors.New("failed to put values: " + resp.Result.String())
				}
			}
		}()
	}

	wg.Wait()
	close(errc)

	errs := []error{}
	for e := range errc {
		errs = append(errs, e)
	}
	if len(errs) != 0 {
		return errors.Join(errs...)
	}

	return nil
}

func (c *cache[T]) Get(ctx context.Context, partition, key string) (T, error) {
	ctx, cancel := context.WithTimeout(ctx, c.client.Cfg.Timeout)
	defer cancel()
	ctx = withRoutingValues(ctx, partition, key)

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

func (c *cache[T]) GetMany(ctx context.Context, partition string, keys []string) (map[string]T, error) {
	ctx, cancel := context.WithTimeout(ctx, c.client.Cfg.Timeout)
	defer cancel()

	var mu sync.Mutex
	results := make(map[string]T)
	addItemToResults := func(k string, v T) {
		mu.Lock()
		defer mu.Unlock()
		results[k] = v
	}

	keyMap := make(map[string][]string)
	for _, k := range keys {
		ctx := withRoutingValues(ctx, partition, k)
		node := c.router.Route(ctx, c.nodes)
		keyMap[node] = append(keyMap[node], k)
	}

	var wg sync.WaitGroup
	l := len(keyMap)
	errc := make(chan error, l)
	wg.Add(l)
	for node, keys := range keyMap {
		go func() {
			defer wg.Done()
			req := &pb.GetManyRequest{
				PartitionKey: partition,
				Keys:         keys,
			}
			resp, err := c.client.clients[node].GetMany(ctx, req, defaultCallOpts...)
			if err != nil {
				errc <- err
				// TODO: we don't want to return partial results if an error occurs here.
			} else {
				for k, v := range resp.Entries {
					val, err := c.codec.Decode(v)
					if err != nil {
						errc <- err
						// TODO: we can return partial results if some keys fail to decode
					} else {
						addItemToResults(k, val)
					}
				}
			}
		}()
	}

	wg.Wait()
	close(errc)

	errs := []error{}
	for e := range errc {
		errs = append(errs, e)
	}
	if len(errs) != 0 {
		return results, errors.Join(errs...)
	}

	return results, nil
}

func (c *cache[T]) Delete(ctx context.Context, partition, key string) error {
	ctx, cancel := context.WithTimeout(ctx, c.client.Cfg.Timeout)
	defer cancel()
	ctx = withRoutingValues(ctx, partition, key)

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
		return errors.New("failed to delete value: " + resp.Result.String())
	}
	return err
}

func (c *cache[T]) DeleteMany(ctx context.Context, partition string, keys []string) (int32, error) {
	ctx, cancel := context.WithTimeout(ctx, c.client.Cfg.Timeout)
	defer cancel()

	var result atomic.Int32
	keyMap := make(map[string][]string)
	for _, k := range keys {
		ctx := withRoutingValues(ctx, partition, k)
		node := c.router.Route(ctx, c.nodes)
		keyMap[node] = append(keyMap[node], k)
	}

	var wg sync.WaitGroup
	l := len(keyMap)
	errc := make(chan error, l)
	wg.Add(l)
	for node, keys := range keyMap {
		go func() {
			defer wg.Done()
			req := &pb.DeleteManyRequest{
				PartitionKey: partition,
				Keys:         keys,
			}
			resp, err := c.client.clients[node].DeleteMany(ctx, req, defaultCallOpts...)
			if err != nil {
				errc <- err
			} else {
				result.Add(resp.Deleted)
			}
		}()
	}

	wg.Wait()
	close(errc)

	errs := []error{}
	for e := range errc {
		errs = append(errs, e)
	}
	if len(errs) != 0 {
		return result.Load(), errors.Join(errs...)
	}

	return result.Load(), nil
}

func withRoutingValues(ctx context.Context, partition, key string) context.Context {
	ctx = context.WithValue(ctx, ContextKeyRoutePartition, partition)
	ctx = context.WithValue(ctx, ContextKeyRouteKey, key)
	return ctx
}

func NewCache[T any](c *Client, router Router, codec Codec[T]) Cache[T] {
	return &cache[T]{client: c, router: router, codec: codec, nodes: c.Cfg.Endpoints}
}

func NewStringCache(c *Client) Cache[string] {
	return &cache[string]{client: c, router: NewKeyRouter(), codec: &StringCodec[string]{}, nodes: c.Cfg.Endpoints}
}

func NewBinaryCache(c *Client) Cache[[]byte] {
	return &cache[[]byte]{client: c, router: NewKeyRouter(), codec: &BinaryCodec[[]byte]{}, nodes: c.Cfg.Endpoints}
}
