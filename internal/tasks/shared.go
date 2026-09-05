package tasks

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"

	"github.com/touchmeangel/rox_listener/internal/containerd"
)

const Image = "touchmeangel/rox_agent:latest"

type PermanentError struct{ Err error }

func (e *PermanentError) Error() string { return e.Err.Error() }
func (e *PermanentError) Unwrap() error { return e.Err }

func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &PermanentError{Err: err}
}

type Runner[TTask any] func(ctx context.Context, client *containerd.Client, runtime string, task TTask) (containerResult, error)

type containerResult struct {
	RunID    string
	ExitCode int64
	Output   json.RawMessage
	Error    string
}

func randomID() string {
	b := make([]byte, 8)
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

func run(ctx context.Context, client *containerd.Client, runtime, name string, cmd []string, mounts []containerd.Mount, outputHostPath string, populate containerd.PopulateFunc) (containerResult, error) {
	result := containerResult{RunID: name}

	exitCode, runErr := client.Run(ctx, containerd.RunSpec{
		Image:    Image,
		Name:     name,
		Cmd:      cmd,
		Runtime:  runtime,
		Mounts:   mounts,
		Populate: populate,
		Quiet:    false,
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
