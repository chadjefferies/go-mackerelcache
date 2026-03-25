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

// Client represents a gRPC client for the MackerelCache service.
type Client struct {
	//Watcher
	//Maintenance

	clients map[string]pb.MackerelCacheServiceClient
	conns   map[string]*grpc.ClientConn

	Cfg Config
}

func (c *Client) PutPartition(ctx context.Context, partition string, expiration time.Duration, persist bool, evictionPolicy pb.EvictionPolicy, maxCacheSize int64) error {
	var wg sync.WaitGroup
	l := len(c.clients)
	errc := make(chan error, l)
	wg.Add(l)

	for _, client := range c.clients {
		go func() {
			defer wg.Done()
			req := &pb.PutPartitionRequest{
				PartitionKey:   partition,
				Expiration:     durationpb.New(expiration),
				Persist:        persist,
				EvictionPolicy: evictionPolicy,
				MaxCacheSize:   maxCacheSize,
			}
			resp, err := client.PutPartition(ctx, req, defaultCallOpts...)
			if err != nil {
				// TODO: log errors
				errc <- err
			}
			if resp.Result != pb.WriteResult_SUCCESS {
				// TODO: create more specific error types
				errc <- fmt.Errorf("failed to put partition: %v", resp.Result)
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

// New creates a new client instance
func New(conf Config) (*Client, error) {
	conns := make(map[string]*grpc.ClientConn)
	clients := make(map[string]pb.MackerelCacheServiceClient)

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
	}

	return newClient(conns, clients, conf), nil
}

// newClient creates a new client instance
func newClient(conns map[string]*grpc.ClientConn, clients map[string]pb.MackerelCacheServiceClient, conf Config) *Client {
	return &Client{
		conns:   conns,
		clients: clients,
		Cfg:     conf,
	}
}
