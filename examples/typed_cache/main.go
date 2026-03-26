package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/chadjefferies/go-mackerelcache"
	"github.com/chadjefferies/go-mackerelcache/api/mackerelcachepb"
)

func main() {
	ctx := context.Background()

	// Create a MackerelCache client assuming three cache nodes running on localhost with ports 11211, 11212, and 11213.
	client, err := mackerelcache.NewClient(&mackerelcache.Config{
		Endpoints: []string{"localhost:11211", "localhost:11212", "localhost:11213"},
		Timeout:   1 * time.Minute,
	})
	defer client.Close()

	// Test the connection
	_, err = client.Ping(ctx)
	if err != nil {
		fmt.Printf("failed to ping clients: %v\n", err)
	}

	// Explicitly create a partition for the cache (optional, as partitions can be created on demand)
	err = client.PutPartition(ctx, "typed_go_client", 5*time.Minute, mackerelcachepb.ExpirationType_SLIDING, false, mackerelcachepb.EvictionPolicy_LRU, 1024)
	if err != nil {
		fmt.Printf("failed to put partition: %v\n", err)
	}

	// Create a typed cache for User objects, using a partition router and a custom json codec
	userCache := mackerelcache.NewCache(client,
		mackerelcache.NewPartitionRouter(),
		&UserCodec{})

	// First, let's delete any existing value for the key.
	err = userCache.Delete(ctx, "typed_go_client", "abc123")
	if err != nil {
		fmt.Printf("failed to delete user: %v\n", err)
	}

	// Write the value to the cache
	err = userCache.Put(ctx, "typed_go_client", "abc123", User{ID: "abc123", Name: "Alice", Age: 30})
	if err != nil {
		fmt.Printf("failed to put user: %v\n", err)
	}

	// Retrieve the value from the cache
	value, err := userCache.Get(ctx, "typed_go_client", "abc123")
	if err != nil {
		fmt.Printf("failed to get user: %v\n", err)
	}

	fmt.Printf("Retrieved user: %v\n", value)
}

type User struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Age  int    `json:"age"`
}

type UserCodec struct {
}

func (c *UserCodec) Encode(user User) ([]byte, error) {
	return json.Marshal(user)
}

func (c *UserCodec) Decode(data []byte) (User, error) {
	var user User
	err := json.Unmarshal(data, &user)
	if err != nil {
		return user, err
	}
	return user, nil
}
