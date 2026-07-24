package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/touchmeangel/rox_listener/config"
	"github.com/touchmeangel/rox_listener/containerd"
	"github.com/touchmeangel/rox_listener/rabbitworker"
	"github.com/touchmeangel/rox_listener/taskrunner"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	cfg, err := config.LoadConfig()
	if err != nil {
		logger.Error("config read error", "error", err)
		return
	}

	client, err := containerd.New()
	if err != nil {
		logger.Error("containerd client init failed", "error", err)
		return
	}
	defer client.Close()

	w := rabbitworker.New(
		cfg.AMQPURL,
		cfg.QueueName,
		rabbitworker.WithLogger(logger),
		rabbitworker.WithPrefetch(cfg.MaxConcurrentContainers),
	)

	w.On("coordinator", func(ctx context.Context, data json.RawMessage) (json.RawMessage, error) {
		return taskrunner.RunCoordinator(ctx, client, cfg.Runtime, data)
	})
	w.On("worker", func(ctx context.Context, data json.RawMessage) (json.RawMessage, error) {
		return taskrunner.RunWorker(ctx, client, cfg.Runtime, data)
	})

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := w.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("worker exited with error", "error", err)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := w.Close(shutdownCtx); err != nil {
		logger.Error("shutdown error", "error", err)
	}
}
