package mackerelcache

import (
	jump "github.com/lithammer/go-jump-consistent-hash"
	"github.com/spaolacci/murmur3"
)

type HashFunction interface {
	Hash(routeKey string, nodeCount int32) int32
}

type ConsistentHashFunction struct {
}

func (f *ConsistentHashFunction) Hash(routeKey string, nodeCount int32) int32 {
	// TODO: this cast assumes utf-8 encoding, while the C# client uses UTF-16 encoding. This may lead to different hash values for the same string between the two clients.
	h1, h2 := murmur3.Sum128([]byte(routeKey))
	idx := jump.Hash(h1+h2, nodeCount)
	return idx
}
