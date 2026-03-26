package main

import (
	"context"
	"fmt"
	"time"

	"github.com/chadjefferies/go-mackerelcache"
)

func main() {
	ctx := context.Background()

	// Create a MackerelCache client assuming three cache nodes running on localhost with ports 11211, 11212, and 11213.
	client, err := mackerelcache.NewClient(&mackerelcache.Config{
		Endpoints: []string{"localhost:11211", "localhost:11212", "localhost:11213"},
		Timeout:   1 * time.Minute,
	})
	defer client.Close()

	// Create a simple string cache using the built-in helper method.
	stringCache := mackerelcache.NewStringCache(client)

	// Notice we don't need to create a partition explicitly,
	// it will be created implicitly when we put a value with a new partition key.

	// First, let's delete any existing value for the key.
	err = stringCache.Delete(ctx, "string_go_client", "abc123")
	if err != nil {
		fmt.Printf("failed to delete value: %v\n", err)
	}

	// Write the value to the cache
	err = stringCache.Put(ctx, "string_go_client", "abc123", "val")
	if err != nil {
		fmt.Printf("failed to create client: %v\n", err)
	}

	// Retrieve the value from the cache
	value, err := stringCache.Get(ctx, "string_go_client", "abc123")
	if err != nil {
		fmt.Printf("failed to get value: %v\n", err)
	}
	fmt.Printf("Retrieved value: %s\n", value)
}
