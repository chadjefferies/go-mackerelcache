package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/chadjefferies/go-mackerelcache"
	"github.com/chadjefferies/go-mackerelcache/api/mackerelcachepb"
)

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

func GetEmployees() []Employee {
	return []Employee{
		{ID: "e1244f4b", Name: "Alice", Age: 30},
		{ID: "337d6823", Name: "John", Age: 22},
		{ID: "b2193120", Name: "Natasha", Age: 53},
	}
}

func main() {
	ctx := context.Background()

	// Create a MackerelCache client assuming three cache nodes running on
	// localhost with ports 11211, 11212, and 11213.
	client, err := mackerelcache.NewClient(&mackerelcache.Config{
		Endpoints: []string{"localhost:11211", "localhost:11212", "localhost:11213"},
		Timeout:   1 * time.Minute,
	})
	defer client.Close()

	var employeeIds []string
	var employeeMap = make(map[string]Employee)
	for _, emp := range GetEmployees() {
		employeeIds = append(employeeIds, emp.ID)
		employeeMap[emp.ID] = emp
	}

	// Explicitly create a partition for the cache
	err = client.PutPartition(ctx, "many_go_client", mackerelcache.PartitionOptions{
		Expiration:     5 * time.Minute,
		ExpirationType: mackerelcachepb.ExpirationType_SLIDING,
		EvictionPolicy: mackerelcachepb.EvictionPolicy_LRU,
		MaxCacheSize:   1024,
	})
	if err != nil {
		fmt.Printf("failed to put partition: %v\n", err)
	}

	// Create a typed cache for Employee objects, using a partition router and a custom json codec
	employeeCache := mackerelcache.NewTypedCache(client, &EmployeeCodec{})

	// Let's delete any existing value for the key.
	r, err := employeeCache.DeleteMany(ctx, "many_go_client", employeeIds)
	if err != nil {
		fmt.Printf("failed to delete employees: %v\n", err)
	}
	fmt.Printf("Deleted %v employees\n", r)

	// Write the values to the cache
	err = employeeCache.PutMany(ctx, "many_go_client", employeeMap)
	if err != nil {
		fmt.Printf("failed to put employees: %v\n", err)
	}
	fmt.Printf("Put %v employees\n", len(employeeMap))

	// Retrieve the value from the cache
	values, err := employeeCache.GetMany(ctx, "many_go_client", employeeIds)
	if err != nil {
		fmt.Printf("failed to get employees: %v\n", err)
	}
	fmt.Printf("Retrieved employees: %v\n", values)
}
