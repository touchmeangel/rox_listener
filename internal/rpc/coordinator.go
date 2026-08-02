package rpc

import (
	"context"

	"github.com/touchmeangel/rox_listener/internal/tasks"
	taskpb "github.com/touchmeangel/rox_proto/rox/task/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Server) RunCoordinator(ctx context.Context, req *taskpb.RunCoordinatorRequest) (*taskpb.RunCoordinatorResponse, error) {
	if req.GetRunId() == "" {
		return nil, status.Error(codes.InvalidArgument, "run_id is required")
	}

	result, err := tasks.RunCoordinator(ctx, s.client, s.runtime, req)
	if err != nil {
		return nil, toStatus(err)
	}

	return result, nil
}
