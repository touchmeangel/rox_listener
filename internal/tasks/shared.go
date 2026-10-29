package tasks

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/touchmeangel/rox_listener/internal/kube"
	"github.com/touchmeangel/rox_listener/internal/storage"
)

type PermanentError struct{ Err error }

func (e *PermanentError) Error() string { return e.Err.Error() }
func (e *PermanentError) Unwrap() error { return e.Err }

func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &PermanentError{Err: err}
}

type Runner struct {
	Kube          *kube.Client
	S3            *s3.Client
	Bucket        string
	AgentImage    string
	ListenerImage string
	AppConfig     json.RawMessage
	AgentEnv      []string
	S3Env         []string
	CPU, Memory   string
	Timeout       time.Duration
}

type params struct {
	Kind, UserID, Workspace, ResultKey string
	Args                               []string
	Files                              []kube.File
}

func randomID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func (r *Runner) run(ctx context.Context, p params) (output []byte, errMsg string, err error) {
	putURL, err := storage.PresignPut(ctx, r.S3, r.Bucket, p.ResultKey, r.Timeout+10*time.Minute)
	if err != nil {
		return nil, "", fmt.Errorf("presigning result upload: %w", err)
	}

	res, err := r.Kube.Run(ctx, kube.RunSpec{
		Name:    fmt.Sprintf("rox-%s-%s", p.Kind, randomID()),
		Image:   r.AgentImage,
		Args:    p.Args,
		Env:     append(append([]string{}, r.AgentEnv...), "ROX_RESULT_PUT_URL="+putURL),
		Files:   append([]kube.File{{Path: "/app/config.json", Content: r.AppConfig}}, p.Files...),
		Scratch: []string{"/work", "/out"},
		Init: []kube.InitStep{{
			Name:  "fetch-workspace",
			Image: r.ListenerImage,
			Args: []string{"fetch-workspace", "--bucket", r.Bucket,
				"--user", p.UserID, "--workspace", p.Workspace, "--dest", "/work"},
			Env: r.S3Env,
		}},
		Timeout: r.Timeout, CPU: r.CPU, Memory: r.Memory,
	})
	if err != nil {
		return nil, "", err // infrastructure failure: retry-able
	}

	output, err = storage.GetObject(ctx, r.S3, r.Bucket, p.ResultKey) // nil if the agent never uploaded
	if err != nil {
		return nil, "", fmt.Errorf("reading result: %w", err)
	}
	if res.ExitCode == 0 {
		return output, "", nil
	}
	return output, fmt.Sprintf("agent exited %d (%s)\n%s", res.ExitCode, res.Reason, res.Logs), nil
}
