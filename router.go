package mackerelcache

import (
	"context"
	"fmt"
	"os"
	"os/user"
)

// TODO: Possibly use grpc's Balancer interface instead of defining our own Router interface.

const (
	ContextKeyRouteKey       = "cache-key"
	ContextKeyRoutePartition = "cache-partition-key"
)

type Router interface {
	Route(ctx context.Context, nodes []string) string
	HashFunction
}

// KeyRouter uses the cache key to route requests to nodes.
// As a result, keys in a partition will be spread evenly across cache nodes.
type KeyRouter struct {
	HashFunction
}

func (r *KeyRouter) Route(ctx context.Context, nodes []string) string {
	routeKey := ctx.Value(ContextKeyRouteKey).(string)
	idx := r.Hash(routeKey, int32(len(nodes)))
	return nodes[idx]
}

// NewKeyRouterWithHash creates a new KeyRouter with the provided hash function.
func NewKeyRouterWithHash(hashFunction HashFunction) Router {
	return &KeyRouter{
		hashFunction,
	}
}

// NewKeyRouter creates a new KeyRouter with the default consistent hash function.
func NewKeyRouter() Router {
	return NewKeyRouterWithHash(&ConsistentHashFunction{})
}

// PartitionRouter uses the partition key to route requests to nodes.
// As a result, all keys in a partition will reside on the same cache node.
type PartitionRouter struct {
	HashFunction
}

func (r *PartitionRouter) Route(ctx context.Context, nodes []string) string {
	routeKey := ctx.Value(ContextKeyRoutePartition).(string)
	node := r.Hash(routeKey, int32(len(nodes)))
	return nodes[node]
}

// NewPartitionRouterWithHash creates a new PartitionRouter with the provided hash function.
func NewPartitionRouterWithHash(hashFunction HashFunction) Router {
	return &PartitionRouter{
		hashFunction,
	}
}

// NewPartitionRouter creates a new PartitionRouter with the default consistent hash function.
func NewPartitionRouter() Router {
	return NewPartitionRouterWithHash(&ConsistentHashFunction{})
}

// ClientAccountRouter routes based on the client account, which is determined by the USERDOMAIN and USERNAME environment variables.
// As a result, a single client will always talk to the same cache node.
type ClientAccountRouter struct {
	HashFunction
}

func (r *ClientAccountRouter) Route(ctx context.Context, nodes []string) string {
	domain := os.Getenv("USERDOMAIN")

	u, err := user.Current()
	username := ""
	if err == nil {
		username = u.Username
	}

	routeKey := fmt.Sprintf("%s-%s", domain, username)
	node := r.Hash(routeKey, int32(len(nodes)))
	return nodes[node]
}

// NewClientAccountRouterWithHash creates a new ClientAccountRouter with the provided hash function.
func NewClientAccountRouterWithHash(hashFunction HashFunction) Router {
	return &ClientAccountRouter{
		hashFunction,
	}
}

// NewClientAccountRouter creates a new ClientAccountRouter with the default consistent hash function.
func NewClientAccountRouter() Router {
	return NewClientAccountRouterWithHash(&ConsistentHashFunction{})
}
