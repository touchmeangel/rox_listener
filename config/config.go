package config

import (
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
	return cfg, nil
}
