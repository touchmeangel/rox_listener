package taskrunner

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/touchmeangel/rox_listener/containerd"
	"github.com/touchmeangel/rox_listener/rabbitworker"
)

const Image = "touchmeangel/rox_agent:latest"

type ContainerResult struct {
	RunID    string          `json:"run_id"`
	ExitCode int64           `json:"exit_code"`
	Output   json.RawMessage `json:"output,omitempty"`
	Error    string          `json:"error,omitempty"`
}

type CoordinatorTask struct {
	RunID     string `json:"run_id"`
	RepoRef   string `json:"repo_ref"`
	SkipBuild bool   `json:"skip_build"`
}

type WorkerTask struct {
	RunID       string `json:"run_id"`
	RepoRef     string `json:"repo_ref"`
	MissionID   string `json:"mission_id"`
	MissionsRef string `json:"missions_ref"`
}

func randomID() string {
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func touchEmpty(path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	return f.Close()
}

func newWorkspace() (dir string, cleanup func(), err error) {
	dir, err = os.MkdirTemp("", "rox-work-*")
	if err != nil {
		return "", nil, fmt.Errorf("creating workspace: %w", err)
	}
	return dir, func() { _ = os.RemoveAll(dir) }, nil
}

func run(ctx context.Context, client *containerd.Client, runtime, name string, cmd []string, mounts []containerd.Mount, outputHostPath string) (ContainerResult, error) {
	result := ContainerResult{RunID: name}

	exitCode, runErr := client.Run(ctx, containerd.RunSpec{
		Image:   Image,
		Name:    name,
		Cmd:     cmd,
		Runtime: runtime,
		Mounts:  mounts,
		Quiet:   true,
	})
	result.ExitCode = exitCode
	if runErr != nil {
		return result, fmt.Errorf("running container: %w", runErr)
	}

	data, readErr := os.ReadFile(outputHostPath)
	if readErr != nil {
		result.Error = fmt.Sprintf("could not read output file: %v", readErr)
		return result, nil
	}
	if len(data) > 0 {
		result.Output = json.RawMessage(data)
	}
	return result, nil
}

func RunCoordinator(ctx context.Context, client *containerd.Client, runtime string, data json.RawMessage) (json.RawMessage, error) {
	var task CoordinatorTask
	if err := json.Unmarshal(data, &task); err != nil {
		return nil, rabbitworker.Permanent(fmt.Errorf("invalid coordinator task payload: %w", err))
	}
	if task.RunID == "" {
		task.RunID = randomID()
	}

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

	debugLog := filepath.Join(workDir, "debug.log")
	if err := touchEmpty(debugLog); err != nil {
		return nil, fmt.Errorf("preparing debug log: %w", err)
	}

	outputHostPath := filepath.Join(workDir, "coordinator_results.json")
	if err := touchEmpty(outputHostPath); err != nil {
		return nil, fmt.Errorf("preparing output file: %w", err)
	}

	cmd := []string{
		"coordinator",
		"--repo-path", "/repo",
		"--work-path", "/work",
		"--output", "/work/coordinator_results.json",
		"--debug", "/app/debug.log",
	}
	if task.SkipBuild {
		cmd = append(cmd, "--skip-build")
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
		return nil, err
	}
	result.RunID = task.RunID

	return json.Marshal(result)
}

func RunWorker(ctx context.Context, client *containerd.Client, runtime string, data json.RawMessage) (json.RawMessage, error) {
	var task WorkerTask
	if err := json.Unmarshal(data, &task); err != nil {
		return nil, rabbitworker.Permanent(fmt.Errorf("invalid worker task payload: %w", err))
	}
	if task.RunID == "" {
		task.RunID = randomID()
	}
	if task.MissionID == "" {
		return nil, rabbitworker.Permanent(fmt.Errorf("worker task missing mission_id"))
	}

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

	missionsFile := filepath.Join(workDir, "coordinator_results.json")
	if err := os.WriteFile(missionsFile, []byte(`{"missions":[]}`), 0o644); err != nil {
		return nil, fmt.Errorf("preparing placeholder missions file: %w", err)
	}

	debugLog := filepath.Join(workDir, "debug.log")
	if err := touchEmpty(debugLog); err != nil {
		return nil, fmt.Errorf("preparing debug log: %w", err)
	}

	outputFilename := fmt.Sprintf("worker_%s.json", task.MissionID)
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
		"--mission-id", task.MissionID,
	}

	mounts := []containerd.Mount{
		{Source: repoDir, Target: "/repo", ReadOnly: true},
		{Source: workDir, Target: "/work", ReadOnly: false},
		{Source: configFile, Target: "/app/config.json", ReadOnly: true},
		{Source: debugLog, Target: "/app/debug.log", ReadOnly: false},
		{Source: missionsFile, Target: "/app/coordinator_results.json", ReadOnly: true},
	}

	name := fmt.Sprintf("rox-worker-%s-%s-%s", task.RunID, task.MissionID, randomID())
	result, err := run(ctx, client, runtime, name, cmd, mounts, outputHostPath)
	if err != nil {
		return nil, err
	}
	result.RunID = task.RunID

	return json.Marshal(result)
}
