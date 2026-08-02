package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	AMQPURL            string
	QueueName          string
	Runtime            string
	ListenAddr         string
	MaxConcurrentTasks int
}

func LoadConfig() (Config, error) {
	cfg := Config{
		AMQPURL:    os.Getenv("AMQP_URL"),
		QueueName:  os.Getenv("AMQP_QUEUE_NAME"),
		Runtime:    os.Getenv("CONTAINER_RUNTIME"),
		ListenAddr: os.Getenv("LISTEN_ADDRESS"),
	}

	var missing []string
	if cfg.AMQPURL == "" {
		missing = append(missing, "AMQP_URL")
	}
	if cfg.QueueName == "" {
		missing = append(missing, "AMQP_QUEUE_NAME")
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
