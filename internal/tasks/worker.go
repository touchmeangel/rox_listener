package tasks

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/touchmeangel/rox_listener/internal/containerd"
	"github.com/touchmeangel/rox_listener/internal/storage"
	taskpb "github.com/touchmeangel/rox_proto/rox/task/v1"
)

func RunWorker(ctx context.Context, client *containerd.Client, runtime string, s3Client *s3.Client, bucket, workDir string, appConfig json.RawMessage, agentEnv []string, req *taskpb.RunWorkerRequest) (*taskpb.RunWorkerResponse, error) {
	runID := req.GetRunId()
	workspaceName := req.GetWorkspaceName()
	missionID := req.GetMissionId()
	mission := json.RawMessage(req.GetMission())

	scratchDir, cleanup, err := newWorkspace(workDir)
	if err != nil {
		return nil, err
	}
	defer cleanup()

	configFile := filepath.Join(scratchDir, "config.json")
	if err := os.WriteFile(configFile, appConfig, 0o644); err != nil {
		return nil, fmt.Errorf("writing app config: %w", err)
	}

	missionsFileContent, err := json.Marshal(struct {
		Missions []json.RawMessage `json:"missions"`
	}{Missions: []json.RawMessage{mission}})
	if err != nil {
		return nil, fmt.Errorf("wrapping mission for worker CLI: %w", err)
	}
	missionsFile := filepath.Join(scratchDir, "coordinator_results.json")
	if err := os.WriteFile(missionsFile, missionsFileContent, 0o644); err != nil {
		return nil, fmt.Errorf("writing missions file: %w", err)
	}

	debugLog := filepath.Join(scratchDir, "debug.log")
	if err := touchEmpty(debugLog); err != nil {
		return nil, fmt.Errorf("preparing debug log: %w", err)
	}

	outputFilename := fmt.Sprintf("worker_%s.json", missionID)
	outputHostPath := filepath.Join(scratchDir, outputFilename)
	if err := touchEmpty(outputHostPath); err != nil {
		return nil, fmt.Errorf("preparing output file: %w", err)
	}

	repoHostDir := filepath.Join(scratchDir, "repo")
	if err := os.MkdirAll(repoHostDir, 0o755); err != nil {
		return nil, fmt.Errorf("creating local repo dir: %w", err)
	}
	if err := storage.DownloadWorkspace(ctx, s3Client, bucket, workspaceName, repoHostDir); err != nil {
		return nil, fmt.Errorf("downloading workspace: %w", err)
	}

	workHostDir := filepath.Join(scratchDir, "work")
	if err := os.MkdirAll(workHostDir, 0o755); err != nil {
		return nil, fmt.Errorf("creating local work dir: %w", err)
	}

	cmd := []string{
		"worker",
		"--repo-path", "/project",
		"--work-path", "/work",
		"--output", "/app/" + outputFilename,
		"--debug", "/app/debug.log",
		"--missions-file", "/app/coordinator_results.json",
		"--mission-id", missionID,
	}

	mounts := []containerd.Mount{
		{Source: configFile, Target: "/app/config.json", ReadOnly: true},
		{Source: debugLog, Target: "/app/debug.log", ReadOnly: false},
		{Source: missionsFile, Target: "/app/coordinator_results.json", ReadOnly: true},
		{Source: outputHostPath, Target: "/app/" + outputFilename, ReadOnly: false},
		{Source: repoHostDir, Target: "/project", ReadOnly: false},
		{Source: workHostDir, Target: "/work", ReadOnly: false},
	}

	name := fmt.Sprintf("rox-worker-%s", randomID())
	result, err := run(ctx, client, runtime, name, cmd, mounts, agentEnv, outputHostPath, nil)
	if err != nil {
		return nil, err
	}

	errMsg := result.Error
	if result.ExitCode == 0 {
		errMsg = ""
	}

	return &taskpb.RunWorkerResponse{
		RunId:  runID,
		Output: result.Output,
		Error:  errMsg,
	}, nil
}
