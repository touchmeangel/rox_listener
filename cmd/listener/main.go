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
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	cfg, err := config.LoadConfig()
	if err != nil {
		logger.Error("config read error", "error", err)
		return
	}

	w := rabbitworker.New(
		cfg.AMQPURL,
		cfg.QueueName,
		rabbitworker.WithLogger(logger),
	)
	client, err := containerd.New()
	if err != nil {
		logger.Error("containerd creation exited with error", "error", err)
		return
	}
	w.On("coordinator", func(ctx context.Context, data json.RawMessage) error {
		client.Run(ctx, containerd.RunSpec{
			Runtime: ,
		})
		return nil
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
