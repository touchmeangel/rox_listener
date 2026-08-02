package tasks

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/touchmeangel/rox_listener/internal/containerd"
	taskpb "github.com/touchmeangel/rox_proto/rox/task/v1"
)

func RunWorker(ctx context.Context, client *containerd.Client, runtime string, req *taskpb.RunWorkerRequest) (*taskpb.RunWorkerResponse, error) {
	runID := req.GetRunId()
	missionID := req.GetMissionId()
	mission := json.RawMessage(req.GetMission())

	workDir, cleanup, err := newWorkspace()
	if err != nil {
		return nil, err
	}
	defer cleanup()

	repoDir := filepath.Join(workDir, "repo")
	if err := os.MkdirAll(repoDir, 0o755); err != nil {
		return nil, fmt.Errorf("preparing placeholder repo dir: %w", err)
	}

	configFile := filepath.Join(workDir, "config.json")
	if err := os.WriteFile(configFile, []byte("{}"), 0o644); err != nil {
		return nil, fmt.Errorf("preparing placeholder config: %w", err)
	}

	missionsFileContent, err := json.Marshal(struct {
		Missions []json.RawMessage `json:"missions"`
	}{Missions: []json.RawMessage{mission}})
	if err != nil {
		return nil, fmt.Errorf("wrapping mission for worker CLI: %w", err)
	}
	missionsFile := filepath.Join(workDir, "coordinator_results.json")
	if err := os.WriteFile(missionsFile, missionsFileContent, 0o644); err != nil {
		return nil, fmt.Errorf("writing missions file: %w", err)
	}

	debugLog := filepath.Join(workDir, "debug.log")
	if err := touchEmpty(debugLog); err != nil {
		return nil, fmt.Errorf("preparing debug log: %w", err)
	}

	outputFilename := fmt.Sprintf("worker_%s.json", missionID)
	outputHostPath := filepath.Join(workDir, outputFilename)
	if err := touchEmpty(outputHostPath); err != nil {
		return nil, fmt.Errorf("preparing output file: %w", err)
	}

	cmd := []string{
		"worker",
		"--repo-path", "/repo",
		"--work-path", "/work",
		"--output", "/work/" + outputFilename,
		"--debug", "/app/debug.log",
		"--missions-file", "/app/coordinator_results.json",
		"--mission-id", missionID,
	}

	mounts := []containerd.Mount{
		{Source: repoDir, Target: "/repo", ReadOnly: true},
		{Source: workDir, Target: "/work", ReadOnly: false},
		{Source: configFile, Target: "/app/config.json", ReadOnly: true},
		{Source: debugLog, Target: "/app/debug.log", ReadOnly: false},
		{Source: missionsFile, Target: "/app/coordinator_results.json", ReadOnly: true},
	}

	name := fmt.Sprintf("rox-worker-%s-%s-%s", runID, missionID, randomID())
	result, err := run(ctx, client, runtime, name, cmd, mounts, outputHostPath)
	if err != nil {
		return nil, err
	}

	return &taskpb.RunWorkerResponse{
		RunId:    runID,
		ExitCode: result.ExitCode,
		Output:   result.Output,
		Error:    result.Error,
	}, nil
}
