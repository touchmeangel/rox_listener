package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/touchmeangel/rox_listener/config"
	"github.com/touchmeangel/rox_listener/internal/storage"
)

func runFetchWorkspace(args []string) error {
	fs := flag.NewFlagSet("fetch-workspace", flag.ContinueOnError)

	user := fs.String("user", "", "ID of the user that owns the workspace")
	workspace := fs.String("workspace", "", "workspace (run) name")
	dest := fs.String("dest", "/work", "directory to download into")

	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("parse fetch-workspace flags: %w", err)
	}

	if *user == "" || *workspace == "" {
		return errors.New("--user and --workspace are required")
	}

	s3cfg, err := config.LoadS3()
	if err != nil {
		return fmt.Errorf("load S3 config: %w", err)
	}

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	client, err := storage.NewS3Client(
		ctx,
		s3cfg.Endpoint,
		s3cfg.Region,
		s3cfg.AccessKeyID,
		s3cfg.SecretAccessKey,
	)
	if err != nil {
		return fmt.Errorf("connect to storage: %w", err)
	}

	start := time.Now()

	n, err := storage.DownloadWorkspace(
		ctx,
		client,
		s3cfg.Bucket,
		*user,
		*workspace,
		*dest,
	)
	if err != nil {
		return fmt.Errorf("download workspace: %w", err)
	}

	slog.Info(
		"workspace downloaded",
		"files", n,
		"took", time.Since(start).Round(time.Millisecond).String(),
	)

	return nil
}
