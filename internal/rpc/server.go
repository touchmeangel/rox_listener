package rpc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/touchmeangel/rox_listener/internal/containerd"
	"github.com/touchmeangel/rox_listener/internal/tasks"
	taskpb "github.com/touchmeangel/rox_proto/rox/task/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	taskpb.UnimplementedTaskServiceServer

	client   *containerd.Client
	runtime  string
	s3Client *s3.Client
	s3Bucket string
}

func NewServer(client *containerd.Client, runtime string, s3Client *s3.Client, s3Bucket string) *Server {
	return &Server{
		client:   client,
		runtime:  runtime,
		s3Client: s3Client,
		s3Bucket: s3Bucket,
	}
}

func ConcurrencyLimiter(sem chan struct{}) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		select {
		case sem <- struct{}{}:
			defer func() { <-sem }()
		case <-ctx.Done():
			return nil, status.FromContextError(ctx.Err()).Err()
		}
		return handler(ctx, req)
	}
}

func LoggingInterceptor(logger *slog.Logger) grpc.UnaryServerInterceptor {
	if logger == nil {
		logger = slog.Default()
	}
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		dur := time.Since(start)

		code := status.Code(err)
		attrs := []any{
			"method", info.FullMethod,
			"duration_ms", dur.Milliseconds(),
			"code", code.String(),
		}
		if err != nil {
			logger.ErrorContext(ctx, "rpc failed", append(attrs, "error", err)...)
		} else {
			logger.InfoContext(ctx, "rpc completed", attrs...)
		}
		return resp, err
	}
}

type namedField struct {
	Name  string
	Value string
}

func requireNonEmpty(fields ...namedField) error {
	for _, f := range fields {
		if f.Value == "" {
			return fmt.Errorf("missing required field: %s", f.Name)
		}
	}
	return nil
}

func toStatus(err error) error {
	var permErr *tasks.PermanentError
	if errors.As(err, &permErr) {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	return status.Errorf(codes.Internal, "%v", err)
}
