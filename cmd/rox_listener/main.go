package main

import (
	"context"
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

func main() {
	if len(os.Args) > 1 && os.Args[1] == "fetch-workspace" {
		runFetchWorkspace(os.Args[2:])
		return
	}
	serve()
}

func serve() {

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	cfg, err := config.LoadConfig()
	if err != nil {
		logger.Error("config read error", "error", err)
		return
	}
	s3cfg, err := config.LoadS3()
	if err != nil {
		logger.Error("config read error", "error", err)
		return
	}

	kubeClient, err := kube.New(kube.Config{
		Kubeconfig: cfg.Kubeconfig, Namespace: cfg.Namespace, RuntimeClass: cfg.RuntimeClass,
	})
	err = kubeClient.Ping(context.Background())
	if err != nil {
		logger.Error("kube ping failed", "config", cfg.Kubeconfig, "error", err)
		return
	}

	lis, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		logger.Error("listen failed", "addr", cfg.ListenAddr, "error", err)
		return
	}

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
			rpc.LoggingInterceptor(logger),
			rpc.ConcurrencyLimiter(sem),
		),
	)

	s3, err := storage.NewS3Client(context.Background(), s3cfg.Endpoint, s3cfg.Region, s3cfg.AccessKeyID, s3cfg.SecretAccessKey)
	if err != nil {
		logger.Error("storage connection failed", "error", err)
		return
	}
	runner := &tasks.Runner{
		Kube: kubeClient, S3: s3, Bucket: cfg.S3.Bucket,
		AgentImage: cfg.AgentImage, ListenerImage: cfg.ListenerImage,
		AppConfig: cfg.AppConfig, AgentEnv: cfg.AgentEnv,
		S3Env: cfg.S3.Env(),
		CPU:   cfg.TaskCPU, Memory: cfg.TaskMemory, Timeout: cfg.TaskTimeout,
	}
	srv := rpc.NewServer(runner)
	taskpb.RegisterTaskServiceServer(grpcServer, srv)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		logger.Info("ready to accept tasks", "addr", cfg.ListenAddr, "max_concurrent", cfg.MaxConcurrentTasks)
		if err := grpcServer.Serve(lis); err != nil {
			logger.Error("grpc server exited with error", "error", err)
		}
	}()

	<-ctx.Done()
	logger.Info("shutdown signal received, draining in-flight tasks")

	done := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		logger.Warn("graceful stop timed out, forcing shutdown")
		grpcServer.Stop()
	}
}
