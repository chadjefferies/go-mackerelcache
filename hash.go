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
	routeKeyBytes := []byte(routeKey)
	// routeKeyBytes := stringToUTF16Bytes(routeKey)
	h1, h2 := murmur3.Sum128(routeKeyBytes)
	idx := jump.Hash(h1+h2, nodeCount)
	return idx
}

// func stringToUTF16Bytes(s string) []byte {
// 	runes := []rune(s)
// 	uint16s := utf16.Encode(runes)
// 	buf := new(bytes.Buffer)
// 	binary.Write(buf, binary.LittleEndian, uint16s)
// 	return buf.Bytes()
// }
