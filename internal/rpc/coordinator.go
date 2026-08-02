package rpc

import (
	"context"

	"github.com/touchmeangel/rox_listener/tasks"
	taskpb "github.com/touchmeangel/rox_proto/rox/task/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Server) RunCoordinator(ctx context.Context, req *taskpb.RunCoordinatorRequest) (*taskpb.RunCoordinatorResponse, error) {
	if req.GetRunId() == "" {
		return nil, status.Error(codes.InvalidArgument, "run_id is required")
	}

	result, err := execute(ctx, s, tasks.RunCoordinator, tasks.CoordinatorTask{
		RunID: req.GetRunId(),
	})
	if err != nil {
		return nil, err
	}

	return &taskpb.RunCoordinatorResponse{
		RunId:    result.RunID,
		ExitCode: result.ExitCode,
		Output:   result.Output,
		Error:    result.Error,
	}, nil
}
