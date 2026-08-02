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
}

func LoadConfig() (Config, error) {
	cfg := Config{
		Runtime:    os.Getenv("CONTAINER_RUNTIME"),
		ListenAddr: os.Getenv("LISTEN_ADDRESS"),
	}

	var missing []string
	if cfg.ListenAddr == "" {
		missing = append(missing, "LISTEN_ADDRESS")
	}

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
