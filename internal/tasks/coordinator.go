package tasks

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/touchmeangel/rox_listener/internal/containerd"
	"github.com/touchmeangel/rox_listener/internal/storage"
	taskpb "github.com/touchmeangel/rox_proto/rox/task/v1"
)

func RunCoordinator(ctx context.Context, client *containerd.Client, runtime string, s3Client *s3.Client, bucket string, req *taskpb.RunCoordinatorRequest) (*taskpb.RunCoordinatorResponse, error) {
	runID := req.GetRunId()
	workspaceName := req.GetWorkspaceName()
	coordinatorID := req.GetCoordinatorId()

	scratchDir, cleanup, err := newWorkspace()
	if err != nil {
		return nil, err
	}
	defer cleanup()

	configFile := filepath.Join(scratchDir, "config.json")
	if err := os.WriteFile(configFile, []byte("{}"), 0o644); err != nil {
		return nil, fmt.Errorf("preparing placeholder config: %w", err)
	}

	debugLog := filepath.Join(scratchDir, "debug.log")
	if err := touchEmpty(debugLog); err != nil {
		return nil, fmt.Errorf("preparing debug log: %w", err)
	}

	outputHostPath := filepath.Join(scratchDir, "coordinator_results.json")
	if err := touchEmpty(outputHostPath); err != nil {
		return nil, fmt.Errorf("preparing output file: %w", err)
	}

	cmd := []string{
		"coordinator",
		"--work-path", "/work",
		"--output", "/app/coordinator_results.json",
		"--debug", "/app/debug.log",
	}

	mounts := []containerd.Mount{
		{Source: configFile, Target: "/app/config.json", ReadOnly: true},
		{Source: debugLog, Target: "/app/debug.log", ReadOnly: false},
		{Source: outputHostPath, Target: "/app/coordinator_results.json", ReadOnly: false},
	}

	populate := func(rootfs string) error {
		workDir := filepath.Join(rootfs, "work")
		if err := os.MkdirAll(workDir, 0o755); err != nil {
			return fmt.Errorf("creating /work in container rootfs: %w", err)
		}
		return storage.DownloadWorkspace(ctx, s3Client, bucket, workspaceName, workDir)
	}

	name := fmt.Sprintf("rox-coordinator-%s-%s-%s", runID, coordinatorID, randomID())
	result, err := run(ctx, client, runtime, name, cmd, mounts, outputHostPath, populate)
	if err != nil {
		return nil, err
	}

	errMsg := result.Error
	if result.ExitCode == 0 {
		errMsg = ""
	}

	return &taskpb.RunCoordinatorResponse{
		RunId:  runID,
		Output: result.Output,
		Error:  errMsg,
	}, nil
}
