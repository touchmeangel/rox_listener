package rpc

import (
	"context"
	"encoding/json"

	"github.com/touchmeangel/rox_listener/internal/tasks"
	taskpb "github.com/touchmeangel/rox_proto/rox/task/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Server) RunWorker(ctx context.Context, req *taskpb.RunWorkerRequest) (*taskpb.RunWorkerResponse, error) {
	if req.GetRunId() == "" || req.GetMissionId() == "" {
		return nil, status.Error(codes.InvalidArgument, "run_id and mission_id are required")
	}
	if len(req.GetMission()) == 0 || !json.Valid(req.GetMission()) {
		return nil, status.Error(codes.InvalidArgument, "mission must be valid, non-empty JSON")
	}

	result, err := tasks.RunWorker(ctx, s.client, s.runtime, req)
	if err != nil {
		return nil, toStatus(err)
	}

	return result, nil
}
