package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/chadjefferies/go-mackerelcache"
)

func main() {
	ctx := context.Background()

	// Create a MackerelCache client assuming three cache nodes running on
	// localhost with ports 11211, 11212, and 11213.
	client, err := mackerelcache.NewClient(&mackerelcache.Config{
		Endpoints: []string{"localhost:11211", "localhost:11212", "localhost:11213"},
		Timeout:   1 * time.Minute,
	})
	defer client.Close()

	// Retrieve the stats for each cache node.
	stats, err := client.GetStats(ctx)
	if err != nil {
		fmt.Printf("failed to retrieve cache stats: %v\n", err)
	} else {
		// Call .Aggregate() to get the combined stats across all cache nodes.
		aggStats := stats.Aggregate()
		prettyJSON, err := json.MarshalIndent(aggStats, "", "  ")
		if err != nil {
			fmt.Printf("failed to parse cache stats: %v\n", err)
		} else {
			fmt.Printf("Cache stats: %v\n", string(prettyJSON))
		}
	}

	// Retrieve the configuration settings for each cache node.
	conf, err := client.GetConf(ctx)
	if err != nil {
		fmt.Printf("failed to retrieve cache configuration: %v\n", err)
	} else {
		prettyJSON, err := json.MarshalIndent(conf, "", "  ")
		if err != nil {
			fmt.Printf("failed to parse cache configuration: %v\n", err)
		} else {
			fmt.Printf("Cache configuration: %v\n", string(prettyJSON))
		}
	}
}
