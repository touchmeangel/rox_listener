package containerd

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"

	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/core/content"
	"github.com/containerd/containerd/v2/core/remotes/docker"
	"github.com/containerd/containerd/v2/pkg/cio"
	"github.com/containerd/containerd/v2/pkg/namespaces"
	"github.com/containerd/containerd/v2/pkg/oci"
	"github.com/containerd/errdefs"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	specs "github.com/opencontainers/runtime-spec/specs-go"
)

const (
	defaultSocket    = "/run/containerd/containerd.sock"
	defaultNamespace = "rox"
	defaultSnapshot  = "overlayfs"
)

const cleanupTimeout = 30 * time.Second

type Client struct {
	cli       *containerd.Client
	namespace string
	logger    *slog.Logger
}

type Option func(*Client)

func WithLogger(l *slog.Logger) Option {
	return func(c *Client) { c.logger = l }
}

func New(opts ...Option) (*Client, error) {
	return NewWithOptions(defaultSocket, defaultNamespace, opts...)
}

func NewWithOptions(socket, namespace string, opts ...Option) (*Client, error) {
	if socket == "" {
		socket = defaultSocket
	}
	if namespace == "" {
		namespace = defaultNamespace
	}
	cli, err := containerd.New(socket)
	if err != nil {
		return nil, fmt.Errorf("connecting to containerd at %s: %w", socket, err)
	}
	c := &Client{cli: cli, namespace: namespace, logger: slog.Default()}
	for _, opt := range opts {
		opt(c)
	}
	return c, nil
}

func (c *Client) Close() error { return c.cli.Close() }

func (c *Client) ctx(parent context.Context) context.Context {
	return namespaces.WithNamespace(parent, c.namespace)
}

func (c *Client) Ping(parent context.Context) error {
	serving, err := c.cli.IsServing(c.ctx(parent))
	if err != nil {
		return fmt.Errorf("checking containerd health: %w", err)
	}
	if !serving {
		return fmt.Errorf("containerd is not serving")
	}
	return nil
}

type PullProgressFunc func(status, id string, current, total int64)

func (c *Client) EnsureImage(parent context.Context, ref string, onProgress PullProgressFunc) error {
	ctx := c.ctx(parent)
	if _, err := c.cli.ImageService().Get(ctx, ref); err == nil {
		return nil
	} else if !errdefs.IsNotFound(err) {
		return fmt.Errorf("checking for image %s: %w", ref, err)
	}
	return c.PullImage(parent, ref, onProgress)
}

func (c *Client) PullImage(parent context.Context, ref string, onProgress PullProgressFunc) error {
	ctx := c.ctx(parent)

	resolver := docker.NewResolver(docker.ResolverOptions{
		Hosts: docker.ConfigureDefaultRegistries(
			docker.WithAuthorizer(docker.NewDockerAuthorizer(docker.WithAuthCreds(credentialsFor))),
		),
	})

	done := make(chan error, 1)
	pullCtx, cancelPoll := context.WithCancel(ctx)
	defer cancelPoll()

	go func() {
		_, err := c.cli.Pull(ctx, ref,
			containerd.WithResolver(resolver),
			containerd.WithPullUnpack,
			containerd.WithPullSnapshotter(defaultSnapshot),
		)
		done <- err
	}()

	if onProgress != nil {
		go c.pollPullProgress(pullCtx, onProgress)
	}

	err := <-done
	cancelPoll()
	if err != nil {
		return fmt.Errorf("pulling %s: %w", ref, err)
	}
	if onProgress != nil {
		onProgress("Pull complete", ref, 1, 1)
	}
	return nil
}

func (c *Client) pollPullProgress(ctx context.Context, onProgress PullProgressFunc) {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	seen := map[string]bool{}
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			statuses, err := c.cli.ContentStore().ListStatuses(ctx, "")
			if err != nil {
				continue
			}
			for _, st := range statuses {
				id := shortRef(st.Ref)
				status := "Downloading"
				if st.Offset >= st.Total && st.Total > 0 {
					status = "Download complete"
				}
				onProgress(status, id, st.Offset, st.Total)
				seen[id] = true
			}
		}
	}
}

func shortRef(ref string) string {
	if i := strings.LastIndex(ref, ":"); i != -1 && i+1 < len(ref) {
		short := ref[i+1:]
		if len(short) > 12 {
			return short[:12]
		}
		return short
	}
	if len(ref) > 12 {
		return ref[:12]
	}
	return ref
}

type Mount struct {
	Source   string
	Target   string
	ReadOnly bool
}

type RunSpec struct {
	Image      string
	Name       string
	Cmd        []string
	Env        []string
	Mounts     []Mount
	ExtraHosts []string
	Runtime    string
	LogFile    io.Writer
	LogPrefix  string
	Quiet      bool
	Live       LineWriter
}

func runtimeShimFor(name string) string {
	switch name {
	case "", "runc":
		return ""
	case "kata", "kata-runtime":
		return "io.containerd.kata.v2"
	default:
		return name
	}
}

var stdoutMu sync.Mutex

func (c *Client) Run(parent context.Context, spec RunSpec) (int64, error) {
	ctx := c.ctx(parent)

	if err := c.EnsureImage(parent, spec.Image, nil); err != nil {
		return -1, fmt.Errorf("ensuring image %s is available: %w", spec.Image, err)
	}

	img, err := c.cli.GetImage(ctx, spec.Image)
	if err != nil {
		return -1, fmt.Errorf("resolving pulled image %s: %w", spec.Image, err)
	}

	c.forceRemove(ctx, spec.Name)

	args, err := c.resolveArgs(ctx, img, spec.Cmd)
	if err != nil {
		return -1, fmt.Errorf("resolving entrypoint for %s: %w", spec.Image, err)
	}

	mounts, hostsCleanup, err := buildMounts(spec.Mounts, spec.ExtraHosts)
	if err != nil {
		return -1, fmt.Errorf("preparing mounts: %w", err)
	}
	if hostsCleanup != nil {
		defer hostsCleanup()
	}

	specOpts := []oci.SpecOpts{
		oci.WithImageConfig(img),
		oci.WithProcessArgs(args...),
		oci.WithEnv(spec.Env),
		oci.WithMounts(mounts),
	}

	containerOpts := []containerd.NewContainerOpts{
		containerd.WithImage(img),
		containerd.WithNewSnapshot(spec.Name+"-snapshot", img),
		containerd.WithNewSpec(specOpts...),
	}
	if shim := runtimeShimFor(spec.Runtime); shim != "" {
		containerOpts = append(containerOpts, containerd.WithRuntime(shim, nil))
	}

	cont, err := c.cli.NewContainer(ctx, spec.Name, containerOpts...)
	if err != nil {
		return -1, fmt.Errorf("creating container: %w", err)
	}
	defer func() {
		delCtx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
		defer cancel()
		if err := cont.Delete(delCtx, containerd.WithSnapshotCleanup); err != nil {
			c.logger.Error("cleanup: failed to delete container", "container", spec.Name, "error", err)
		}
	}()

	pw := &Writer{prefix: spec.LogPrefix, quiet: spec.Quiet, live: spec.Live}
	var stdout, stderr io.Writer = pw, pw
	if spec.LogFile != nil {
		stdout = io.MultiWriter(pw, spec.LogFile)
		stderr = stdout
	}

	task, err := cont.NewTask(ctx, cio.NewCreator(cio.WithStreams(nil, stdout, stderr)))
	if err != nil {
		return -1, fmt.Errorf("creating task: %w", err)
	}
	defer func() {
		delCtx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
		defer cancel()
		if _, err := task.Delete(delCtx); err != nil {
			c.logger.Error("cleanup: failed to delete task", "container", spec.Name, "error", err)
		}
	}()

	exitCh, err := task.Wait(ctx)
	if err != nil {
		return -1, fmt.Errorf("waiting on task: %w", err)
	}

	if err := task.Start(ctx); err != nil {
		return -1, fmt.Errorf("starting task: %w", err)
	}

	select {
	case status := <-exitCh:
		if err := status.Error(); err != nil {
			return -1, fmt.Errorf("task exited with error: %w", err)
		}
		return int64(status.ExitCode()), nil
	case <-parent.Done():
		_ = task.Kill(context.Background(), syscall.SIGKILL)
		<-exitCh
		return -1, parent.Err()
	}
}

func (c *Client) ReapOrphans(parent context.Context) error {
	ctx := c.ctx(parent)
	containers, err := c.cli.Containers(ctx)
	if err != nil {
		return fmt.Errorf("listing containers for orphan sweep: %w", err)
	}

	var errs []error
	for _, cont := range containers {
		id := cont.ID()
		if task, err := cont.Task(ctx, nil); err == nil {
			if _, err := task.Delete(ctx, containerd.WithProcessKill); err != nil {
				errs = append(errs, fmt.Errorf("killing orphan task %s: %w", id, err))
			}
		}
		if err := cont.Delete(ctx, containerd.WithSnapshotCleanup); err != nil {
			errs = append(errs, fmt.Errorf("deleting orphan container %s: %w", id, err))
		}
	}
	return errors.Join(errs...)
}

func (c *Client) forceRemove(ctx context.Context, name string) {
	cont, err := c.cli.LoadContainer(ctx, name)
	if err != nil {
		return
	}
	if task, err := cont.Task(ctx, nil); err == nil {
		_, _ = task.Delete(ctx, containerd.WithProcessKill)
	}
	_ = cont.Delete(ctx, containerd.WithSnapshotCleanup)
}

func (c *Client) resolveArgs(ctx context.Context, img containerd.Image, cmd []string) ([]string, error) {
	desc, err := img.Config(ctx)
	if err != nil {
		return nil, err
	}
	blob, err := content.ReadBlob(ctx, c.cli.ContentStore(), desc)
	if err != nil {
		return nil, err
	}

	var conf ocispec.Image
	if err := json.Unmarshal(blob, &conf); err != nil {
		return nil, err
	}

	if len(cmd) == 0 {
		return append(append([]string{}, conf.Config.Entrypoint...), conf.Config.Cmd...), nil
	}
	return append(append([]string{}, conf.Config.Entrypoint...), cmd...), nil
}

func buildMounts(mounts []Mount, extraHosts []string) ([]specs.Mount, func(), error) {
	out := make([]specs.Mount, 0, len(mounts)+1)
	for _, m := range mounts {
		opts := []string{"rbind"}
		if m.ReadOnly {
			opts = append(opts, "ro")
		} else {
			opts = append(opts, "rw")
		}
		out = append(out, specs.Mount{
			Destination: m.Target,
			Type:        "bind",
			Source:      m.Source,
			Options:     opts,
		})
	}

	var cleanup func()
	if len(extraHosts) > 0 {
		hostsPath, err := writeExtraHostsFile(extraHosts)
		if err != nil {
			return nil, nil, fmt.Errorf("writing extra hosts file: %w", err)
		}
		cleanup = func() { _ = os.Remove(hostsPath) }
		out = append(out, specs.Mount{
			Destination: "/etc/hosts",
			Type:        "bind",
			Source:      hostsPath,
			Options:     []string{"rbind", "ro"},
		})
	}

	return out, cleanup, nil
}

func writeExtraHostsFile(extraHosts []string) (string, error) {
	var b strings.Builder
	b.WriteString("127.0.0.1\tlocalhost\n::1\tlocalhost ip6-localhost ip6-loopback\n")
	for _, entry := range extraHosts {
		parts := strings.SplitN(entry, ":", 2)
		if len(parts) != 2 {
			continue
		}
		fmt.Fprintf(&b, "%s\t%s\n", parts[1], parts[0])
	}

	f, err := os.CreateTemp("", "rox-hosts-*")
	if err != nil {
		return "", err
	}
	defer func() { _ = f.Close() }()
	if _, err := f.WriteString(b.String()); err != nil {
		return "", err
	}
	return f.Name(), nil
}

func decodeBasicAuth(auth string) (string, error) {
	if auth == "" {
		return "", fmt.Errorf("empty auth")
	}
	decoded, err := base64.StdEncoding.DecodeString(auth)
	if err != nil {
		return "", err
	}
	return string(decoded), nil
}
