package mackerelcache

import (
	"context"
	"errors"
	"sync"

	pb "github.com/chadjefferies/go-mackerelcache/api/mackerelcachepb"
)

type Maintenance interface {
	// Ping checks the health of the cache nodes.
	Ping(ctx context.Context) (map[string]string, error)
	// InvokeGC forces a GC run on the cache node. This is not recommended.
	InvokeGC(ctx context.Context) error
}

type maintenance struct {
	clients map[string]pb.MaintenanceServiceClient
}

func (m *maintenance) Ping(ctx context.Context) (map[string]string, error) {
	// ctx, cancel := context.WithTimeout(ctx, mc.client.Cfg.Timeout)
	// defer cancel()

	var mu sync.Mutex
	results := make(map[string]string)
	addItemToResults := func(k string, v string) {
		mu.Lock()
		defer mu.Unlock()
		results[k] = v
	}

	var wg sync.WaitGroup
	l := len(m.clients)
	errc := make(chan error, l)
	wg.Add(l)

	for node, client := range m.clients {
		go func() {
			defer wg.Done()
			resp, err := client.Ping(ctx, &pb.PingRequest{}, defaultCallOpts...)
			if err != nil {
				errc <- err
				addItemToResults(node, "error: "+err.Error())
			} else {
				addItemToResults(node, resp.Result)
			}
		}()
	}

	wg.Wait()
	close(errc)

	errs := []error{}
	for e := range errc {
		errs = append(errs, e)
	}
	if len(errs) != 0 {
		return results, errors.Join(errs...)
	}
	return results, nil
}

func (m *maintenance) InvokeGC(ctx context.Context) error {
	var wg sync.WaitGroup
	l := len(m.clients)
	errc := make(chan error, l)
	wg.Add(l)

	for _, client := range m.clients {
		go func() {
			defer wg.Done()
			_, err := client.InvokeGC(ctx, &pb.InvokeGCRequest{}, defaultCallOpts...)
			if err != nil {
				errc <- err
			}
		}()
	}

	wg.Wait()
	close(errc)

	errs := []error{}
	for e := range errc {
		errs = append(errs, e)
	}
	if len(errs) != 0 {
		return errors.Join(errs...)
	}
	return nil
}
