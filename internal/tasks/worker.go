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

func RunWorker(ctx context.Context, client *containerd.Client, runtime string, s3Client *s3.Client, bucket string, req *taskpb.RunWorkerRequest) (*taskpb.RunWorkerResponse, error) {
	runID := req.GetRunId()
	workerID := req.GetWorkerId()
	missionID := req.GetMissionId()
	workspaceName := req.GetWorkspaceName()
	mission := json.RawMessage(req.GetMission())

	scratchDir, cleanup, err := newWorkspace()
	if err != nil {
		return nil, err
	}
	defer cleanup()

	configFile := filepath.Join(scratchDir, "config.json")
	if err := os.WriteFile(configFile, []byte("{}"), 0o644); err != nil {
		return nil, fmt.Errorf("preparing placeholder config: %w", err)
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

	cmd := []string{
		"worker",
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
	}

	populate := func(rootfs string) error {
		workDir := filepath.Join(rootfs, "work")
		if err := os.MkdirAll(workDir, 0o755); err != nil {
			return fmt.Errorf("creating /work in container rootfs: %w", err)
		}
		return storage.DownloadWorkspace(ctx, s3Client, bucket, workspaceName, workDir)
	}

	name := fmt.Sprintf("rox-worker-%s-%s-%s-%s", runID, workerID, missionID, randomID())
	result, err := run(ctx, client, runtime, name, cmd, mounts, outputHostPath, populate)
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
