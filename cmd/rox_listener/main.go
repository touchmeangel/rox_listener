package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/touchmeangel/rox_listener/config"
	"github.com/touchmeangel/rox_listener/internal/kube"
	"github.com/touchmeangel/rox_listener/internal/rpc"
	"github.com/touchmeangel/rox_listener/internal/storage"
	"github.com/touchmeangel/rox_listener/internal/tasks"
	taskpb "github.com/touchmeangel/rox_proto/rox/task/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

// def _upload_result(path: str) -> None:
//     url = os.environ.get("ROX_RESULT_PUT_URL")
//     if not url:
//         return  # local runs: keep writing to disk only
//     req = urllib.request.Request(url, data=Path(path).read_bytes(), method="PUT")
//     urllib.request.urlopen(req, timeout=60).read()

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	var err error

	if len(os.Args) > 1 && os.Args[1] == "fetch-workspace" {
		err = runFetchWorkspace(os.Args[2:])
	} else {
		err = serve()
	}

	if err != nil {
		logger.Error("command failed", "error", err)
		os.Exit(1)
	}
}

func serve() error {
	cfg, err := config.LoadConfig()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	s3cfg, err := config.LoadS3()
	if err != nil {
		return fmt.Errorf("load S3 config: %w", err)
	}

	kubeClient, err := kube.New(kube.Config{
		Kubeconfig:   cfg.Kubeconfig,
		Namespace:    cfg.Namespace,
		RuntimeClass: cfg.RuntimeClass,
	})
	if err != nil {
		return fmt.Errorf("initialize Kubernetes client: %w", err)
	}

	if err := kubeClient.Ping(context.Background()); err != nil {
		return fmt.Errorf("kubernetes ping: %w", err)
	}

	lis, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", cfg.ListenAddr, err)
	}
	defer func() { _ = lis.Close() }()

	sem := make(chan struct{}, cfg.MaxConcurrentTasks)

	grpcServer := grpc.NewServer(
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    30 * time.Second,
			Timeout: 10 * time.Second,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             10 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.ChainUnaryInterceptor(
			rpc.LoggingInterceptor(slog.New(slog.NewTextHandler(os.Stdout, nil))),
			rpc.ConcurrencyLimiter(sem),
		),
	)

	s3, err := storage.NewS3Client(
		context.Background(),
		s3cfg.Endpoint,
		s3cfg.Region,
		s3cfg.AccessKeyID,
		s3cfg.SecretAccessKey,
	)
	if err != nil {
		return fmt.Errorf("connect to storage: %w", err)
	}

	runner := &tasks.Runner{
		Kube:          kubeClient,
		S3:            s3,
		Bucket:        cfg.S3.Bucket,
		AgentImage:    cfg.AgentImage,
		ListenerImage: cfg.ListenerImage,
		AppConfig:     cfg.AppConfig,
		AgentEnv:      cfg.AgentEnv,
		S3Env:         cfg.S3.Env(),
		CPU:           cfg.TaskCPU,
		Memory:        cfg.TaskMemory,
		Timeout:       cfg.TaskTimeout,
	}

	srv := rpc.NewServer(runner)
	taskpb.RegisterTaskServiceServer(grpcServer, srv)

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	serveErr := make(chan error, 1)

	go func() {
		if err := grpcServer.Serve(lis); err != nil &&
			!errors.Is(err, grpc.ErrServerStopped) {
			serveErr <- fmt.Errorf("gRPC server: %w", err)
			return
		}
		serveErr <- nil
	}()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	logger.Info(
		"ready to accept tasks",
		"addr", cfg.ListenAddr,
		"max_concurrent", cfg.MaxConcurrentTasks,
	)

	select {
	case err := <-serveErr:
		return err
	case <-ctx.Done():
		logger.Info("shutdown signal received, draining in-flight tasks")
	}

	done := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-time.After(30 * time.Second):
		logger.Warn("graceful stop timed out, forcing shutdown")
		grpcServer.Stop()
		<-done
		return nil
	}
}
