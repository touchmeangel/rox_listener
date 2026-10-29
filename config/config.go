package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

// S3 is everything needed to talk to object storage. It is shared by the
// listener and by the fetch-workspace init container.
type S3 struct {
	Endpoint        string
	Region          string
	AccessKeyID     string
	SecretAccessKey string
	Bucket          string
}

func (s S3) Env() []string {
	return []string{
		"S3_ENDPOINT=" + s.Endpoint,
		"S3_REGION=" + s.Region,
		"S3_ACCESS_KEY_ID=" + s.AccessKeyID,
		"S3_SECRET_ACCESS_KEY=" + s.SecretAccessKey,
		"S3_BUCKET=" + s.Bucket,
	}
}

type Config struct {
	ListenAddr         string
	MaxConcurrentTasks int

	S3 S3

	Kubeconfig    string
	Namespace     string
	RuntimeClass  string
	AgentImage    string
	ListenerImage string
	TaskCPU       string
	TaskMemory    string
	TaskTimeout   time.Duration

	AppConfig json.RawMessage
	AgentEnv  []string
}

type reader struct {
	missing []string
	invalid []string
}

func (r *reader) req(name string) string {
	v := os.Getenv(name)
	if v == "" {
		r.missing = append(r.missing, name)
	}
	return v
}

func (r *reader) opt(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

func (r *reader) positiveInt(name string) int {
	raw := r.req(name)
	if raw == "" {
		return 0
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		r.invalid = append(r.invalid, fmt.Sprintf("%s must be a positive integer, got %q", name, raw))
		return 0
	}
	return n
}

func (r *reader) duration(name string, def time.Duration) time.Duration {
	raw := os.Getenv(name)
	if raw == "" {
		return def
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		r.invalid = append(r.invalid, fmt.Sprintf("%s must be a positive duration like 2h, got %q", name, raw))
		return def
	}
	return d
}

func (r *reader) err() error {
	var parts []string
	if len(r.missing) > 0 {
		parts = append(parts, "missing required config: "+strings.Join(r.missing, ", "))
	}
	parts = append(parts, r.invalid...)
	if len(parts) == 0 {
		return nil
	}
	return errors.New(strings.Join(parts, "; "))
}

func loadS3(r *reader) S3 {
	return S3{
		Endpoint:        r.req("S3_ENDPOINT"),
		Region:          r.req("S3_REGION"),
		AccessKeyID:     r.req("S3_ACCESS_KEY_ID"),
		SecretAccessKey: r.req("S3_SECRET_ACCESS_KEY"),
		Bucket:          r.req("S3_BUCKET"),
	}
}

func LoadS3() (S3, error) {
	var r reader
	s := loadS3(&r)
	return s, r.err()
}

func LoadConfig() (Config, error) {
	var r reader
	cfg := Config{
		ListenAddr:         r.req("LISTEN_ADDRESS"),
		MaxConcurrentTasks: r.positiveInt("MAX_CONCURRENT_TASKS"),
		S3:                 loadS3(&r),
		Kubeconfig:         os.Getenv("KUBECONFIG"),
		Namespace:          r.opt("KUBE_NAMESPACE", "rox"),
		RuntimeClass:       os.Getenv("RUNTIME_CLASS"),
		AgentImage:         r.req("AGENT_IMAGE"),
		ListenerImage:      r.req("LISTENER_IMAGE"),
		TaskCPU:            os.Getenv("TASK_CPU"),
		TaskMemory:         os.Getenv("TASK_MEMORY"),
		TaskTimeout:        r.duration("TASK_TIMEOUT", 2*time.Hour),
	}
	appConfigPath := r.req("APP_CONFIG_PATH")
	agentEnvPath := r.req("AGENT_ENV_PATH")
	if err := r.err(); err != nil {
		return cfg, err
	}

	data, err := os.ReadFile(appConfigPath)
	if err != nil {
		return cfg, fmt.Errorf("reading APP_CONFIG_PATH %q: %w", appConfigPath, err)
	}
	if !json.Valid(data) {
		return cfg, fmt.Errorf("APP_CONFIG_PATH %q does not contain valid JSON", appConfigPath)
	}
	cfg.AppConfig = json.RawMessage(data)

	agentEnv, err := parseEnvFile(agentEnvPath)
	if err != nil {
		return cfg, fmt.Errorf("reading AGENT_ENV_PATH %q: %w", agentEnvPath, err)
	}
	cfg.AgentEnv = agentEnv
	return cfg, nil
}

func parseEnvFile(path string) ([]string, error) {
	if _, err := os.Stat(path); err != nil {
		return nil, err
	}
	vars, err := godotenv.Read(path)
	if err != nil {
		return nil, fmt.Errorf("parsing env file: %w", err)
	}
	if len(vars) == 0 {
		return nil, fmt.Errorf("no KEY=VALUE entries found")
	}
	env := make([]string, 0, len(vars))
	for k, v := range vars {
		env = append(env, k+"="+v)
	}
	return env, nil
}
