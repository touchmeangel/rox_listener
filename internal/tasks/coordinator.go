package tasks

import (
	"context"
	"fmt"

	taskpb "github.com/touchmeangel/rox_proto/rox/task/v1"
)

func (r *Runner) RunCoordinator(ctx context.Context, req *taskpb.RunCoordinatorRequest) (*taskpb.RunCoordinatorResponse, error) {
	runID := req.GetRunId()
	userID := req.GetUserId()
	workspaceName := req.GetWorkspaceName()

	out, errMsg, err := r.run(ctx, params{
		Kind: "coordinator", UserID: userID, Workspace: workspaceName,
		ResultKey: fmt.Sprintf("results/%s/%s/coordinator.json", userID, runID),
		Args: []string{"coordinator", "--work-path", "/work",
			"--output", "/out/coordinator_results.json", "--debug", "/out/debug.log"},
	})
	if err != nil {
		return nil, err
	}
	return &taskpb.RunCoordinatorResponse{RunId: req.GetRunId(), Output: out, Error: errMsg}, nil
}
