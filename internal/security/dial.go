package security

import (
	"context"
	"errors"
	"os"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"godfs/internal/config"
	"godfs/internal/observability"
)

// ClientTransport returns transport credentials: TLS when GODFS_TLS_ENABLED, else insecure.
func ClientTransport() (credentials.TransportCredentials, error) {
	cfg := LoadTLSConfigFromEnv()
	if !cfg.Enabled {
		return insecure.NewCredentials(), nil
	}
	if cfg.CAFile == "" {
		return nil, errors.New("GODFS_TLS_ENABLED requires GODFS_TLS_CA_FILE for clients")
	}
	return ClientTransportCredentials(cfg)
}

// ClientDialOptions returns dial options for internal components (master↔chunk, chunk↔chunk).
// Adds GODFS_CLUSTER_KEY as Bearer on every RPC when set.
func ClientDialOptions() ([]grpc.DialOption, error) {
	tc, err := ClientTransport()
	if err != nil {
		return nil, err
	}
	opts := []grpc.DialOption{grpc.WithTransportCredentials(tc)}
	key := strings.TrimSpace(os.Getenv("GODFS_CLUSTER_KEY"))
	if key != "" {
		opts = append(opts,
			grpc.WithUnaryInterceptor(bearerUnaryInterceptor(key)),
			grpc.WithStreamInterceptor(bearerStreamInterceptor(key)),
		)
	}
	opts = append(opts, observability.GRPCClientDialOptions()...)
	opts = append(opts, config.GRPCDialOptions()...)
	return opts, nil
}

func bearerUnaryInterceptor(secret string) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+secret)
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

func bearerStreamInterceptor(secret string) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+secret)
		return streamer(ctx, desc, cc, method, opts...)
	}
}

// UserClientDialOptions builds options for end-user clients: TLS + optional API key / JWT in metadata.
func UserClientDialOptions(apiKey string) ([]grpc.DialOption, error) {
	tc, err := ClientTransport()
	if err != nil {
		return nil, err
	}
	opts := []grpc.DialOption{grpc.WithTransportCredentials(tc)}
	tok := strings.TrimSpace(apiKey)
	if tok == "" {
		tok = strings.TrimSpace(os.Getenv("GODFS_CLIENT_API_KEY"))
	}
	if tok != "" {
		opts = append(opts,
			grpc.WithUnaryInterceptor(bearerUnaryInterceptor(tok)),
			grpc.WithStreamInterceptor(bearerStreamInterceptor(tok)),
		)
	}
	opts = append(opts, observability.GRPCClientDialOptions()...)
	opts = append(opts, config.GRPCDialOptions()...)
	return opts, nil
}

// UserClientDialOptionsForGateway returns TLS + client options for a multi-tenant HTTP gateway:
// it does not attach GODFS_CLIENT_API_KEY from the environment. Send Bearer tokens per request
// via metadata on each RPC (e.g. metadata.AppendToOutgoingContext).
func UserClientDialOptionsForGateway() ([]grpc.DialOption, error) {
	tc, err := ClientTransport()
	if err != nil {
		return nil, err
	}
	opts := []grpc.DialOption{grpc.WithTransportCredentials(tc)}
	opts = append(opts, observability.GRPCClientDialOptions()...)
	opts = append(opts, config.GRPCDialOptions()...)
	return opts, nil
}
