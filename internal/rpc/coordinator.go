package rpc

import (
	"context"

	taskpb "github.com/touchmeangel/rox_proto/rox/task/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Server) RunCoordinator(ctx context.Context, req *taskpb.RunCoordinatorRequest) (*taskpb.RunCoordinatorResponse, error) {
	if err := requireNonEmpty(
		namedField{"run_id", req.GetRunId()},
		namedField{"workspace_name", req.GetWorkspaceName()},
		namedField{"coordinator_id", req.GetCoordinatorId()},
	); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	result, err := s.runner.RunCoordinator(ctx, req)
	if err != nil {
		return nil, toStatus(err)
	}

	return result, nil
}
