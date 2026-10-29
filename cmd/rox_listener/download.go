package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/touchmeangel/rox_listener/config"
	"github.com/touchmeangel/rox_listener/internal/storage"
)

func runFetchWorkspace(args []string) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	fs := flag.NewFlagSet("fetch-workspace", flag.ContinueOnError)
	user := fs.String("user", "", "ID of the user that owns the workspace")
	workspace := fs.String("workspace", "", "workspace (run) name")
	dest := fs.String("dest", "/work", "directory to download into")
	if err := fs.Parse(args); err != nil {
		return
	}
	if *user == "" || *workspace == "" {
		logger.Error("--user and --workspace are required")
		return
	}

	s3cfg, err := config.LoadS3()
	if err != nil {
		logger.Error("config read error", "error", err)
		return
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	client, err := storage.NewS3Client(ctx, s3cfg.Endpoint, s3cfg.Region, s3cfg.AccessKeyID, s3cfg.SecretAccessKey)
	if err != nil {
		logger.Error("storage connection failed", "error", err)
		return
	}

	start := time.Now()
	n, err := storage.DownloadWorkspace(ctx, client, s3cfg.Bucket, *user, *workspace, *dest)
	if err != nil {
		logger.Error("workspace download failed", "error", err)
		return
	}
	logger.Info("workspace downloaded", "files", n, "took", time.Since(start).Round(time.Millisecond).String())
	return
}
