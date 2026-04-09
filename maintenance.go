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
	// GetStats returns the stats for the cache nodes such as hits, misses, etc.
	GetStats(ctx context.Context) (*NodeStats, error)
	// GetConf returns the configuration settings for the cache node.
	GetConf(ctx context.Context) (map[string]*pb.CacheConfiguration, error)
}

type maintenance struct {
	clients map[string]pb.MaintenanceServiceClient
	cfg     *Config
}

func (m *maintenance) Ping(ctx context.Context) (map[string]string, error) {
	ctx, cancel := context.WithTimeout(ctx, m.cfg.Timeout)
	defer cancel()

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
	ctx, cancel := context.WithTimeout(ctx, m.cfg.Timeout)
	defer cancel()

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

func (m *maintenance) GetStats(ctx context.Context) (*NodeStats, error) {
	ctx, cancel := context.WithTimeout(ctx, m.cfg.Timeout)
	defer cancel()

	var stats = NewStats()
	var wg sync.WaitGroup
	l := len(m.clients)
	errc := make(chan error, l)
	wg.Add(l)

	for node, client := range m.clients {
		go func() {
			defer wg.Done()
			resp, err := client.GetStats(ctx, &pb.GetStatsRequest{}, defaultCallOpts...)
			if err != nil {
				errc <- err
			} else {
				stats.AddNodeStats(node, resp)
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
		return stats, errors.Join(errs...)
	}
	return stats, nil
}

func (m *maintenance) GetConf(ctx context.Context) (map[string]*pb.CacheConfiguration, error) {
	ctx, cancel := context.WithTimeout(ctx, m.cfg.Timeout)
	defer cancel()

	var mu sync.Mutex
	results := make(map[string]*pb.CacheConfiguration)
	addItemToResults := func(k string, v *pb.CacheConfiguration) {
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
			resp, err := client.GetConf(ctx, &pb.GetConfRequest{}, defaultCallOpts...)
			if err != nil {
				errc <- err
			} else {
				addItemToResults(node, resp)
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
