package main

import (
	"context"
	"fmt"
	"time"

	"github.com/chadjefferies/go-mackerelcache"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	// Create client
	client, err := mackerelcache.New(mackerelcache.Config{
		Endpoints: []string{"localhost:11211", "localhost:11212", "localhost:11213"},
		Timeout:   10 * time.Second,
	})
	defer func() {
		if err := client.Close(); err != nil {
			fmt.Printf("error closing client: %v", err)
		}
	}()

	stringCache := mackerelcache.NewStringCache(client)

	err = stringCache.Delete(ctx, "go_client", "abc123")
	if err != nil {
		fmt.Printf("failed to delete value: %v", err)
	}

	err = stringCache.Put(ctx, "go_client", "abc123", "val")

	if err != nil {
		fmt.Printf("failed to create client: %v", err)
	}

	value, err := stringCache.Get(ctx, "go_client", "abc123")
	if err != nil {
		fmt.Printf("failed to get value: %v", err)
	}

	fmt.Printf("Retrieved value: %s", value)
}
