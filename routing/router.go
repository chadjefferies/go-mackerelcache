package routing

import (
	"context"
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

func NewKeyRouter(hashFunction HashFunction) Router {
	return &KeyRouter{
		hashFunction,
	}
}

type PartitionRouter struct {
	HashFunction
}

func (r *PartitionRouter) Route(ctx context.Context, nodes []string) string {
	routeKey := ctx.Value(ContextKeyRoutePartition).(string)
	node := r.Hash(routeKey, int32(len(nodes)))
	return nodes[node]
}

func NewPartitionRouter(hashFunction HashFunction) Router {
	return &PartitionRouter{
		hashFunction,
	}
}
