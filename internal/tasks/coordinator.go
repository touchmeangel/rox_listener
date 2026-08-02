package tasks

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/touchmeangel/rox_listener/internal/containerd"
)

func RunCoordinator(ctx context.Context, client *containerd.Client, runtime string, task CoordinatorTask) (ContainerResult, error) {
	if task.RunID == "" {
		task.RunID = randomID()
	}

	workDir, cleanup, err := newWorkspace()
	if err != nil {
		return ContainerResult{}, err
	}
	defer cleanup()

	repoDir := filepath.Join(workDir, "repo")
	if err := os.MkdirAll(repoDir, 0o755); err != nil {
		return ContainerResult{}, fmt.Errorf("preparing placeholder repo dir: %w", err)
	}

	configFile := filepath.Join(workDir, "config.json")
	if err := os.WriteFile(configFile, []byte("{}"), 0o644); err != nil {
		return ContainerResult{}, fmt.Errorf("preparing placeholder config: %w", err)
	}

	debugLog := filepath.Join(workDir, "debug.log")
	if err := touchEmpty(debugLog); err != nil {
		return ContainerResult{}, fmt.Errorf("preparing debug log: %w", err)
	}

	outputHostPath := filepath.Join(workDir, "coordinator_results.json")
	if err := touchEmpty(outputHostPath); err != nil {
		return ContainerResult{}, fmt.Errorf("preparing output file: %w", err)
	}

	cmd := []string{
		"coordinator",
		"--repo-path", "/repo",
		"--work-path", "/work",
		"--output", "/work/coordinator_results.json",
		"--debug", "/app/debug.log",
	}

	mounts := []containerd.Mount{
		{Source: repoDir, Target: "/repo", ReadOnly: true},
		{Source: workDir, Target: "/work", ReadOnly: false},
		{Source: configFile, Target: "/app/config.json", ReadOnly: true},
		{Source: debugLog, Target: "/app/debug.log", ReadOnly: false},
	}

	name := fmt.Sprintf("rox-coordinator-%s-%s", task.RunID, randomID())
	result, err := run(ctx, client, runtime, name, cmd, mounts, outputHostPath)
	if err != nil {
		return ContainerResult{}, err
	}
	result.RunID = task.RunID

	return result, nil
}
