package mackerelcache

import (
	"context"
	"time"

	//"go.uber.org/zap"
	"google.golang.org/grpc"
)

type Config struct {
	// Endpoints is a list of URLs.
	Endpoints []string `json:"endpoints"`

	Timeout time.Duration `json:"timeout"`

	// // DialTimeout is the timeout for failing to establish a connection.
	// DialTimeout time.Duration `json:"dial-timeout"`

	// // DialKeepAliveTimeout is the time that the client waits for a response for the
	// // keep-alive probe. If the response is not received in this time, the connection is closed.
	// DialKeepAliveTimeout time.Duration `json:"dial-keep-alive-timeout"`

	// MaxCallSendMsgSize is the client-side request send limit in bytes.
	// If 0, it defaults to 2.0 MiB (2 * 1024 * 1024).
	// Make sure that "MaxCallSendMsgSize" < server-side default send/recv limit.
	MaxCallSendMsgSize int

	// MaxCallRecvMsgSize is the client-side response receive limit.
	// If 0, it defaults to "math.MaxInt32", because range response can
	// easily exceed request send limits.
	// Make sure that "MaxCallRecvMsgSize" >= server-side default send/recv limit.
	MaxCallRecvMsgSize int

	// DialOptions is a list of dial options for the grpc client (e.g., for interceptors).
	// Note that grpc.NewClient ignores options that are specific to grpc.Dial such as
	// "grpc.WithBlock()". Use DialTimeout to bound client initialization time.
	DialOptions []grpc.DialOption

	// Context is the default client context; it can be used to cancel grpc dial out and
	// other operations that do not have an explicit context.
	Context context.Context

	// // Logger sets client-side logger.
	// // If nil, fallback to building LogConfig.
	// Logger *zap.Logger

	// // LogConfig configures client-side logger.
	// // If nil, use the default logger.
	// // TODO: configure gRPC logger
	// LogConfig *zap.Config

	// // MaxUnaryRetries is the maximum number of retries for unary RPCs.
	// MaxUnaryRetries uint `json:"max-unary-retries"`

	// // BackoffWaitBetween is the wait time before retrying an RPC.
	// BackoffWaitBetween time.Duration `json:"backoff-wait-between"`

	// // BackoffJitterFraction is the jitter fraction to randomize backoff wait time.
	// BackoffJitterFraction float64 `json:"backoff-jitter-fraction"`

	// TODO: support custom balancer picker
}
