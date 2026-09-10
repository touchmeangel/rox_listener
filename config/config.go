package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/joho/godotenv"
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
