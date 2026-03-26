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

type KeyRouter struct {
	HashFunction
}

func (r *KeyRouter) Route(ctx context.Context, nodes []string) string {
	routeKey := ctx.Value(ContextKeyRouteKey).(string)
	idx := r.Hash(routeKey, int32(len(nodes)))
	return nodes[idx]
}

func NewKeyRouterWithHash(hashFunction HashFunction) Router {
	return &KeyRouter{
		hashFunction,
	}
}

func NewKeyRouter() Router {
	return NewKeyRouterWithHash(&ConsistentHashFunction{})
}

type PartitionRouter struct {
	HashFunction
}

func (r *PartitionRouter) Route(ctx context.Context, nodes []string) string {
	routeKey := ctx.Value(ContextKeyRoutePartition).(string)
	node := r.Hash(routeKey, int32(len(nodes)))
	return nodes[node]
}

func NewPartitionRouterWithHash(hashFunction HashFunction) Router {
	return &PartitionRouter{
		hashFunction,
	}
}

func NewPartitionRouter() Router {
	return NewPartitionRouterWithHash(&ConsistentHashFunction{})
}

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

func NewClientAccountRouterWithHash(hashFunction HashFunction) Router {
	return &ClientAccountRouter{
		hashFunction,
	}
}

func NewClientAccountRouter() Router {
	return NewClientAccountRouterWithHash(&ConsistentHashFunction{})
}
