package mackerelcache

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	pb "github.com/chadjefferies/go-mackerelcache/api/mackerelcachepb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/durationpb"
)

var (
	defaultWaitForReady       = grpc.WaitForReady(false)
	defaultMaxCallSendMsgSize = grpc.MaxCallSendMsgSize(2 * 1024 * 1024)
	defaultMaxCallRecvMsgSize = grpc.MaxCallRecvMsgSize(math.MaxInt32)
)

// defaultCallOpts defines a list of default "gRPC.CallOption".
var defaultCallOpts = []grpc.CallOption{
	defaultWaitForReady,
	defaultMaxCallSendMsgSize,
	defaultMaxCallRecvMsgSize,
}

// Client is a MackerelCache client representing a pool of zero or more underlying connections.
// It's safe for concurrent use by multiple goroutines.
type Client struct {
	Watcher
	Maintenance
	Cfg     *Config
	clients map[string]pb.MackerelCacheServiceClient
	conns   map[string]*grpc.ClientConn
}

type PartitionOptions struct {
	Expiration     time.Duration
	ExpirationType pb.ExpirationType
	Persist        bool
	EvictionPolicy pb.EvictionPolicy
	MaxCacheSize   int64
}

// PutPartition creates a partition in the cache. If it already exists,
// / it's updated, if it doesn't exist, a new partition is created.
func (c *Client) PutPartition(ctx context.Context, partition string, opts PartitionOptions) error {
	var wg sync.WaitGroup
	l := len(c.clients)
	errc := make(chan error, l)
	wg.Add(l)

	for _, client := range c.clients {
		go func() {
			defer wg.Done()
			req := &pb.PutPartitionRequest{
				PartitionKey:   partition,
				Expiration:     durationpb.New(opts.Expiration),
				Persist:        opts.Persist,
				EvictionPolicy: opts.EvictionPolicy,
				MaxCacheSize:   opts.MaxCacheSize,
				ExpirationType: opts.ExpirationType,
			}
			resp, err := client.PutPartition(ctx, req, defaultCallOpts...)
			if err != nil {
				errc <- err
				return
			}
			if resp.Result != pb.WriteResult_SUCCESS {
				errc <- errors.New("failed to put partition: " + resp.Result.String())
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

// FlushPartition flushes all cache entries in the specified partition.
func (c *Client) FlushPartition(ctx context.Context, partition string) error {
	var wg sync.WaitGroup
	l := len(c.clients)
	errc := make(chan error, l)
	wg.Add(l)

	for _, client := range c.clients {
		go func() {
			defer wg.Done()
			req := &pb.FlushPartitionRequest{
				PartitionKey: partition,
			}
			_, err := client.FlushPartition(ctx, req, defaultCallOpts...)
			if err != nil {
				errc <- err
				return
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

// DeletePartition deletes the specified partition and all cache entries in it.
func (c *Client) DeletePartition(ctx context.Context, partition string) error {
	var wg sync.WaitGroup
	l := len(c.clients)
	errc := make(chan error, l)
	wg.Add(l)

	for _, client := range c.clients {
		go func() {
			defer wg.Done()
			req := &pb.DeletePartitionRequest{
				PartitionKey: partition,
			}
			resp, err := client.DeletePartition(ctx, req, defaultCallOpts...)
			if err != nil {
				errc <- err
				return
			}
			if resp.Result != pb.WriteResult_SUCCESS {
				errc <- errors.New("failed to delete partition: " + resp.Result.String())
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

// Close closes the client, releasing any open resources.
// It is rare to Close a Client, as the Client is meant to be
// long-lived and shared between many goroutines.
func (c *Client) Close() error {
	// c.cancel()
	// if c.Watcher != nil {
	// 	c.Watcher.Close()
	// }
	for _, conn := range c.conns {
		if err := conn.Close(); err != nil {
			//fmt.Printf("error closing connection: %v", err)
			// TODO: log error?
		}
	}
	// return c.ctx.Err()
	return nil
}

// NewClient creates a new MackerelCache client instance
func NewClient(conf *Config) (*Client, error) {
	conns := make(map[string]*grpc.ClientConn)
	clients := make(map[string]pb.MackerelCacheServiceClient)
	maintClients := make(map[string]pb.MaintenanceServiceClient)
	watchClients := make(map[string]pb.WatchServiceClient)

	for _, endpoint := range conf.Endpoints {
		if endpoint == "" {
			return nil, fmt.Errorf("endpoint cannot be empty")
		}

		var opts []grpc.DialOption
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))

		opts = append(opts, grpc.WithDefaultCallOptions(
			defaultMaxCallRecvMsgSize,
			defaultMaxCallSendMsgSize,
		))

		// retry, err := config.RetryDialOption("mackerelcachepb.MackerelCacheService", conf.RetryPolicy)
		// if err == nil {
		// 	opts = append(opts, retry)
		// }

		conn, err := grpc.NewClient(
			endpoint,
			opts...,
		)

		if err != nil {
			return nil, err
		}
		conns[endpoint] = conn
		clients[endpoint] = pb.NewMackerelCacheServiceClient(conn)
		maintClients[endpoint] = pb.NewMaintenanceServiceClient(conn)
		watchClients[endpoint] = pb.NewWatchServiceClient(conn)
	}

	return newClient(conns, clients, maintClients, watchClients, conf), nil
}

func newClient(conns map[string]*grpc.ClientConn, clients map[string]pb.MackerelCacheServiceClient, maintClients map[string]pb.MaintenanceServiceClient, watchClients map[string]pb.WatchServiceClient, conf *Config) *Client {
	return &Client{
		conns:       conns,
		clients:     clients,
		Cfg:         conf,
		Maintenance: &maintenance{clients: maintClients},
		Watcher:     &watcher{clients: watchClients},
	}
}
