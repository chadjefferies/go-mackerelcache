package mackerelcache

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/chadjefferies/go-mackerelcache/api/mackerelcachepb"
)

type Cache[T any] interface {
	// Put sets a key-value in the cache. If it already exists,
	// it's updated, if it doesn't exist, a new entry is created.
	Put(ctx context.Context, partition, key string, value T) error
	// PutMany sets a collection of key-value pairs in the cache. If they already exist,
	// they are updated, if they don't exist, new entries are created.
	PutMany(ctx context.Context, partition string, items map[string]T) error
	// Get gets the value in the cache at the specified key.
	Get(ctx context.Context, partition, key string) (T, error)
	// GetMany gets the values in the cache at the specified keys.
	GetMany(ctx context.Context, partition string, key []string) (map[string]T, error)
	// Delete removes the value from the cache.
	Delete(ctx context.Context, partition, key string) error
	// DeleteMany removes the values from the cache, returning the number of keys successfully deleted.
	DeleteMany(ctx context.Context, partition string, keys []string) (int32, error)
	// Ttl returns the remaining TTL for the given key. If the key does not exist, it returns a negative duration.
	Ttl(ctx context.Context, partition, key string) (time.Duration, error)
	// TtlMany returns the remaining TTL for the given keys. For keys that do not exist, the value will be a negative duration.
	TtlMany(ctx context.Context, partition string, key []string) (map[string]time.Duration, error)
	// Touch updates the last access time of a key. Only supports partitions with sliding expiration.
	Touch(ctx context.Context, partition, key string) error
	// TouchMany updates the last access time of the specified keys. Only supports partitions with sliding expiration.
	// Returns the number of keys successfully touched.
	TouchMany(ctx context.Context, partition string, keys []string) (int32, error)
	// Increment atomically increments the integer value at the given key by 1, returning the new value.
	// If the key does not exist, it's initialized to 0 before incrementing.
	Increment(ctx context.Context, partition, key string) (int64, error)
	// IncrementBy atomically increments the integer value at the given key by the specified value, returning the new value.
	// If the key does not exist, it's initialized to 0 before incrementing.
	IncrementBy(ctx context.Context, partition, key string, value int64) (int64, error)
	// Decrement atomically decrements the integer value at the given key by 1, returning the new value.
	// If the key does not exist, it's initialized to 0 before decrementing.
	Decrement(ctx context.Context, partition, key string) (int64, error)
	// DecrementBy atomically decrements the integer value at the given key by the specified value, returning the new value.
	// If the key does not exist, it's initialized to 0 before decrementing.
	DecrementBy(ctx context.Context, partition, key string, value int64) (int64, error)
}

type cache[T any] struct {
	client *Client
	codec  Codec[T]
	router Router
	nodes  []string
}

func (c *cache[T]) Put(ctx context.Context, partition, key string, value T) error {
	ctx, cancel := context.WithTimeout(ctx, c.client.Cfg.Timeout)
	defer cancel()

	ctx = withRoutingValues(ctx, partition, key)
	node := c.router.Route(ctx, c.nodes)
	encodedVal, err := c.codec.Encode(value)
	if err != nil {
		return err
	}
	req := &pb.PutRequest{
		PartitionKey: partition,
		Key:          key,
		Value:        encodedVal,
	}
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

	itemMap, err := c.routeItemsToNode(ctx, partition, items)
	if err != nil {
		return err
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
	node := c.router.Route(ctx, c.nodes)
	req := &pb.GetRequest{
		PartitionKey: partition,
		Key:          key,
	}
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

	keyMap := c.routeKeysToNode(ctx, partition, keys)
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
	node := c.router.Route(ctx, c.nodes)
	req := &pb.DeleteRequest{
		PartitionKey: partition,
		Key:          key,
	}
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

	keyMap := c.routeKeysToNode(ctx, partition, keys)
	var result atomic.Int32
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

func (c *cache[T]) Ttl(ctx context.Context, partition, key string) (time.Duration, error) {
	ctx, cancel := context.WithTimeout(ctx, c.client.Cfg.Timeout)
	defer cancel()

	ctx = withRoutingValues(ctx, partition, key)
	node := c.router.Route(ctx, c.nodes)
	req := &pb.TtlRequest{
		PartitionKey: partition,
		Key:          key,
	}
	resp, err := c.client.clients[node].Ttl(ctx, req, defaultCallOpts...)
	if err != nil {
		return 0, err
	}
	return time.Duration(resp.ValueMs) * time.Millisecond, nil
}

func (c *cache[T]) TtlMany(ctx context.Context, partition string, keys []string) (map[string]time.Duration, error) {
	ctx, cancel := context.WithTimeout(ctx, c.client.Cfg.Timeout)
	defer cancel()

	var mu sync.Mutex
	results := make(map[string]time.Duration)
	addItemToResults := func(k string, v time.Duration) {
		mu.Lock()
		defer mu.Unlock()
		results[k] = v
	}

	keyMap := c.routeKeysToNode(ctx, partition, keys)
	var wg sync.WaitGroup
	l := len(keyMap)
	errc := make(chan error, l)
	wg.Add(l)
	for node, keys := range keyMap {
		go func() {
			defer wg.Done()
			req := &pb.TtlManyRequest{
				PartitionKey: partition,
				Keys:         keys,
			}
			resp, err := c.client.clients[node].TtlMany(ctx, req, defaultCallOpts...)
			if err != nil {
				errc <- err
				// TODO: we don't want to return partial results if an error occurs here.
			} else {
				for k, v := range resp.Entries {
					val := time.Duration(v) * time.Millisecond
					addItemToResults(k, val)
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

func (c *cache[T]) Touch(ctx context.Context, partition, key string) error {
	ctx, cancel := context.WithTimeout(ctx, c.client.Cfg.Timeout)
	defer cancel()

	ctx = withRoutingValues(ctx, partition, key)
	node := c.router.Route(ctx, c.nodes)
	req := &pb.TouchRequest{
		PartitionKey: partition,
		Key:          key,
	}
	resp, err := c.client.clients[node].Touch(ctx, req, defaultCallOpts...)
	if err != nil {
		return err
	}
	if resp.Result != pb.WriteResult_SUCCESS {
		return errors.New("failed to touch value: " + resp.Result.String())
	}
	return err
}

func (c *cache[T]) TouchMany(ctx context.Context, partition string, keys []string) (int32, error) {
	ctx, cancel := context.WithTimeout(ctx, c.client.Cfg.Timeout)
	defer cancel()

	keyMap := c.routeKeysToNode(ctx, partition, keys)
	var result atomic.Int32
	var wg sync.WaitGroup
	l := len(keyMap)
	errc := make(chan error, l)
	wg.Add(l)
	for node, keys := range keyMap {
		go func() {
			defer wg.Done()
			req := &pb.TouchManyRequest{
				PartitionKey: partition,
				Keys:         keys,
			}
			resp, err := c.client.clients[node].TouchMany(ctx, req, defaultCallOpts...)
			if err != nil {
				errc <- err
			} else {
				result.Add(resp.Touched)
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

func (c *cache[T]) Increment(ctx context.Context, partition, key string) (int64, error) {
	ctx, cancel := context.WithTimeout(ctx, c.client.Cfg.Timeout)
	defer cancel()

	ctx = withRoutingValues(ctx, partition, key)
	node := c.router.Route(ctx, c.nodes)
	req := &pb.IncrementRequest{
		PartitionKey: partition,
		Key:          key,
	}
	resp, err := c.client.clients[node].Increment(ctx, req, defaultCallOpts...)
	if err != nil {
		return 0, err
	}
	return resp.Value, nil
}

func (c *cache[T]) IncrementBy(ctx context.Context, partition, key string, value int64) (int64, error) {
	ctx, cancel := context.WithTimeout(ctx, c.client.Cfg.Timeout)
	defer cancel()

	ctx = withRoutingValues(ctx, partition, key)
	node := c.router.Route(ctx, c.nodes)
	req := &pb.IncrementByRequest{
		PartitionKey: partition,
		Key:          key,
		Value:        value,
	}
	resp, err := c.client.clients[node].IncrementBy(ctx, req, defaultCallOpts...)
	if err != nil {
		return 0, err
	}
	return resp.Value, nil
}

func (c *cache[T]) Decrement(ctx context.Context, partition, key string) (int64, error) {
	ctx, cancel := context.WithTimeout(ctx, c.client.Cfg.Timeout)
	defer cancel()

	ctx = withRoutingValues(ctx, partition, key)
	node := c.router.Route(ctx, c.nodes)
	req := &pb.DecrementRequest{
		PartitionKey: partition,
		Key:          key,
	}
	resp, err := c.client.clients[node].Decrement(ctx, req, defaultCallOpts...)
	if err != nil {
		return 0, err
	}
	return resp.Value, nil
}

func (c *cache[T]) DecrementBy(ctx context.Context, partition, key string, value int64) (int64, error) {
	ctx, cancel := context.WithTimeout(ctx, c.client.Cfg.Timeout)
	defer cancel()

	ctx = withRoutingValues(ctx, partition, key)
	node := c.router.Route(ctx, c.nodes)
	req := &pb.DecrementByRequest{
		PartitionKey: partition,
		Key:          key,
		Value:        value,
	}
	resp, err := c.client.clients[node].DecrementBy(ctx, req, defaultCallOpts...)
	if err != nil {
		return 0, err
	}
	return resp.Value, nil
}

func withRoutingValues(ctx context.Context, partition, key string) context.Context {
	ctx = context.WithValue(ctx, ContextKeyRoutePartition, partition)
	ctx = context.WithValue(ctx, ContextKeyRouteKey, key)
	return ctx
}

func (c *cache[T]) routeKeysToNode(ctx context.Context, partition string, keys []string) map[string][]string {
	keyMap := make(map[string][]string)
	for _, k := range keys {
		ctx := withRoutingValues(ctx, partition, k)
		node := c.router.Route(ctx, c.nodes)
		keyMap[node] = append(keyMap[node], k)
	}
	return keyMap
}

func (c *cache[T]) routeItemsToNode(ctx context.Context, partition string, items map[string]T) (map[string]map[string][]byte, error) {
	itemMap := make(map[string]map[string][]byte)
	for k, v := range items {
		ctx := withRoutingValues(ctx, partition, k)
		node := c.router.Route(ctx, c.nodes)
		val, err := c.codec.Encode(v)
		if err != nil {
			return nil, err
		}

		if _, ok := itemMap[node]; !ok {
			itemMap[node] = make(map[string][]byte)
		}

		itemMap[node][k] = val
	}
	return itemMap, nil
}

func NewTypedCache[T any](c *Client, codec Codec[T]) Cache[T] {
	return &cache[T]{client: c, codec: codec, router: NewKeyRouter(), nodes: c.Cfg.Endpoints}
}

func NewTypedCacheWithRouter[T any](c *Client, codec Codec[T], router Router) Cache[T] {
	return &cache[T]{client: c, codec: codec, router: router, nodes: c.Cfg.Endpoints}
}

func NewStringCache(c *Client) Cache[string] {
	return &cache[string]{client: c, router: NewKeyRouter(), codec: &StringCodec[string]{}, nodes: c.Cfg.Endpoints}
}

func NewBinaryCache(c *Client) Cache[[]byte] {
	return &cache[[]byte]{client: c, router: NewKeyRouter(), codec: &BinaryCodec[[]byte]{}, nodes: c.Cfg.Endpoints}
}
