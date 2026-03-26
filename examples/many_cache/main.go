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

	employees := map[string]Employee{
		"1": {ID: "1", Name: "Alice", Age: 30},
		"2": {ID: "2", Name: "John", Age: 22},
		"3": {ID: "3", Name: "Natasha", Age: 53},
	}
	var employeeIds []string
	for id := range employees {
		employeeIds = append(employeeIds, id)
	}

	// Explicitly create a partition for the cache (optional, as partitions can be created on demand)
	err = client.PutPartition(ctx, "many_go_client", 5*time.Minute, mackerelcachepb.ExpirationType_SLIDING, false, mackerelcachepb.EvictionPolicy_LRU, 1024)
	if err != nil {
		fmt.Printf("failed to put partition: %v\n", err)
	}

	// Create a typed cache for User objects, using a partition router and a custom json codec
	employeeCache := mackerelcache.NewCache(client, mackerelcache.NewKeyRouter(), &EmployeeCodec{})

	// First, let's delete any existing value for the key.
	r, err := employeeCache.DeleteMany(ctx, "many_go_client", employeeIds)
	if err != nil {
		fmt.Printf("failed to delete employees: %v\n", err)
	}

	fmt.Printf("Deleted %v employees\n", r)

	// Write the value to the cache
	err = employeeCache.PutMany(ctx, "many_go_client", employees)
	if err != nil {
		fmt.Printf("failed to put employees: %v\n", err)
	}

	fmt.Printf("Put %v employees\n", len(employees))

	// Retrieve the value from the cache
	value, err := employeeCache.GetMany(ctx, "many_go_client", employeeIds)
	if err != nil {
		fmt.Printf("failed to get employees: %v\n", err)
	}

	fmt.Printf("Retrieved employees: %v\n", value)
}

type Employee struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Age  int    `json:"age"`
}

type EmployeeCodec struct {
}

func (c *EmployeeCodec) Encode(emp Employee) ([]byte, error) {
	return json.Marshal(emp)
}

func (c *EmployeeCodec) Decode(data []byte) (Employee, error) {
	var emp Employee
	err := json.Unmarshal(data, &emp)
	if err != nil {
		return emp, err
	}
	return emp, nil
}
