package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	Runtime            string
	ListenAddr         string
	MaxConcurrentTasks int

	S3Endpoint        string
	S3Region          string
	S3AccessKeyID     string
	S3SecretAccessKey string
	S3Bucket          string

	DockerHubUsername string
	DockerHubToken    string

	WorkDir string

	AppConfigPath string
	AppConfig     json.RawMessage

	AgentEnvPath string
	AgentEnv     []string
}

func LoadConfig() (Config, error) {
	cfg := Config{
		Runtime:           os.Getenv("CONTAINER_RUNTIME"),
		ListenAddr:        os.Getenv("LISTEN_ADDRESS"),
		S3Endpoint:        os.Getenv("S3_ENDPOINT"),
		S3Region:          os.Getenv("S3_REGION"),
		S3AccessKeyID:     os.Getenv("S3_ACCESS_KEY_ID"),
		S3SecretAccessKey: os.Getenv("S3_SECRET_ACCESS_KEY"),
		S3Bucket:          os.Getenv("S3_BUCKET"),
		DockerHubUsername: os.Getenv("DOCKERHUB_USERNAME"),
		DockerHubToken:    os.Getenv("DOCKERHUB_TOKEN"),
		WorkDir:           os.Getenv("WORK_DIR"),
		AppConfigPath:     os.Getenv("APP_CONFIG_PATH"),
		AgentEnvPath:      os.Getenv("AGENT_ENV_PATH"),
	}

	var missing []string
	checkReq := func(name, val string) {
		if val == "" {
			missing = append(missing, name)
		}
	}

	checkReq("LISTEN_ADDRESS", cfg.ListenAddr)
	checkReq("S3_ENDPOINT", cfg.S3Endpoint)
	checkReq("S3_REGION", cfg.S3Region)
	checkReq("S3_ACCESS_KEY_ID", cfg.S3AccessKeyID)
	checkReq("S3_SECRET_ACCESS_KEY", cfg.S3SecretAccessKey)
	checkReq("S3_BUCKET", cfg.S3Bucket)
	checkReq("DOCKERHUB_USERNAME", cfg.DockerHubUsername)
	checkReq("DOCKERHUB_TOKEN", cfg.DockerHubToken)
	checkReq("WORK_DIR", cfg.WorkDir)
	checkReq("APP_CONFIG_PATH", cfg.AppConfigPath)
	checkReq("AGENT_ENV_PATH", cfg.AgentEnvPath)

	raw := os.Getenv("MAX_CONCURRENT_TASKS")
	if raw == "" {
		missing = append(missing, "MAX_CONCURRENT_TASKS")
	} else if n, err := strconv.Atoi(raw); err != nil || n <= 0 {
		return cfg, fmt.Errorf("MAX_CONCURRENT_TASKS must be a positive integer, got %q", raw)
	} else {
		cfg.MaxConcurrentTasks = n
	}

	if len(missing) > 0 {
		return cfg, fmt.Errorf("missing required config: %s", strings.Join(missing, ", "))
	}

	data, err := os.ReadFile(cfg.AppConfigPath)
	if err != nil {
		return cfg, fmt.Errorf("reading APP_CONFIG_PATH %q: %w", cfg.AppConfigPath, err)
	}
	if !json.Valid(data) {
		return cfg, fmt.Errorf("APP_CONFIG_PATH %q does not contain valid JSON", cfg.AppConfigPath)
	}
	cfg.AppConfig = json.RawMessage(data)

	agentEnv, err := parseEnvFile(cfg.AgentEnvPath)
	if err != nil {
		return cfg, fmt.Errorf("reading AGENT_ENV_PATH %q: %w", cfg.AgentEnvPath, err)
	}
	cfg.AgentEnv = agentEnv

	return cfg, nil
}

func parseEnvFile(path string) ([]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var env []string
	for i, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, found := strings.Cut(line, "=")
		if !found {
			return nil, fmt.Errorf("line %d: expected KEY=VALUE, got %q", i+1, line)
		}
		key = strings.TrimSpace(key)
		if key == "" {
			return nil, fmt.Errorf("line %d: empty key", i+1)
		}
		value = strings.Trim(strings.TrimSpace(value), `"'`)
		env = append(env, key+"="+value)
	}
	if len(env) == 0 {
		return nil, fmt.Errorf("no KEY=VALUE entries found")
	}
	return env, nil
}
