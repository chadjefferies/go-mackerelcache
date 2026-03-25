package main

import (
	"context"
	"fmt"
	"time"

	"github.com/chadjefferies/go-mackerelcache"
	"github.com/chadjefferies/go-mackerelcache/api/mackerelcachepb"
	"github.com/chadjefferies/go-mackerelcache/routing"
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

	err = client.PutPartition(ctx, "go_client", 5*time.Minute, false, mackerelcachepb.EvictionPolicy_LRU, 1024)
	if err != nil {
		fmt.Printf("failed to put partition: %v", err)
	}

	stringCache := mackerelcache.NewCache(client,
		routing.NewPartitionRouter(&routing.ConsistentHashFunction{}),
		&UserCodec{})

	err = stringCache.Delete(ctx, "go_client", "abc123")
	if err != nil {
		fmt.Printf("failed to delete value: %v", err)
	}

	err = stringCache.Put(ctx, "go_client", "abc123", User{ID: "abc123", Name: "Alice"})

	if err != nil {
		fmt.Printf("failed to create client: %v", err)
	}

	value, err := stringCache.Get(ctx, "go_client", "abc123")
	if err != nil {
		fmt.Printf("failed to get value: %v", err)
	}

	fmt.Printf("Retrieved value: %s", value)
}

type User struct {
	ID   string
	Name string
}

type UserCodec struct {
}

func (c *UserCodec) Encode(user User) ([]byte, error) {
	return []byte(fmt.Sprintf("%s:%s", user.ID, user.Name)), nil
}

func (c *UserCodec) Decode(data []byte) (User, error) {
	parts := string(data)
	var user User
	fmt.Sscanf(parts, "%s:%s", &user.ID, &user.Name)
	return user, nil
}
