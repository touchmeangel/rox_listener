package tasks

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/touchmeangel/rox_listener/internal/kube"
	taskpb "github.com/touchmeangel/rox_proto/rox/task/v1"
)

func (r *Runner) RunWorker(ctx context.Context, req *taskpb.RunWorkerRequest) (*taskpb.RunWorkerResponse, error) {
	runID := req.GetRunId()
	userID := req.GetUserId()
	missionID := req.GetMissionId()

	missionsJSON, err := json.Marshal(struct {
		Missions []json.RawMessage `json:"missions"`
	}{Missions: []json.RawMessage{json.RawMessage(req.GetMission())}})
	if err != nil {
		return nil, Permanent(fmt.Errorf("wrapping mission for worker CLI: %w", err))
	}

	outputFilename := fmt.Sprintf("worker_%s.json", missionID)

	out, errMsg, err := r.run(ctx, params{
		Kind: "worker", UserID: userID, Workspace: req.GetWorkspaceName(),
		ResultKey: fmt.Sprintf("results/%s/%s/%s_worker.json", userID, runID, missionID),
		Args: []string{
			"worker",
			"--work-path", "/work",
			"--output", "/app/" + outputFilename,
			"--debug", "/app/debug.log",
			"--missions-file", "/app/coordinator_results.json",
			"--mission-id", missionID,
		},
		Files: []kube.File{{Path: "/app/coordinator_results.json", Content: missionsJSON}},
	})
	if err != nil {
		return nil, err
	}
	return &taskpb.RunWorkerResponse{RunId: runID, Output: out, Error: errMsg}, nil
}
