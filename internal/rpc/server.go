package rpc

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"

	"github.com/touchmeangel/rox_listener/internal/containerd"
	"github.com/touchmeangel/rox_listener/internal/tasks"
	taskpb "github.com/touchmeangel/rox_proto/rox/task/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	taskpb.UnimplementedTaskServiceServer

	client  *containerd.Client
	runtime string
	logger  *slog.Logger
	sem     chan struct{}
}

func NewServer(client *containerd.Client, runtime string, concurrency int, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	return &Server{
		client:  client,
		runtime: runtime,
		logger:  logger,
		sem:     make(chan struct{}, concurrency),
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

func toStatus(err error) error {
	var permErr *tasks.PermanentError
	if errors.As(err, &permErr) {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	return status.Errorf(codes.Internal, "%v", err)
}

func execute[TTask any](ctx context.Context, s *Server, run tasks.Runner, task TTask) (tasks.ContainerResult, error) {
	var zero tasks.ContainerResult

	payload, err := json.Marshal(task)
	if err != nil {
		return zero, status.Errorf(codes.Internal, "encoding task: %v", err)
	}

	raw, err := run(ctx, s.client, s.runtime, payload)
	if err != nil {
		return zero, toStatus(err)
	}

	var result tasks.ContainerResult
	if err := json.Unmarshal(raw, &result); err != nil {
		return zero, status.Errorf(codes.Internal, "decoding task result: %v", err)
	}
	return result, nil
}
