package containerd

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"

	gocni "github.com/containerd/go-cni"
	"github.com/containernetworking/plugins/pkg/ns"
	"golang.org/x/sys/unix"
)

const (
	cniConfDir  = "/etc/cni/net.d"
	cniBinDir   = "/opt/cni/bin"
	cniConfList = cniConfDir + "/10-rox-sandbox.conflist"
	netnsRunDir = "/var/run/netns"
)

// networkManager gives each sandboxed container its own network namespace,
// wired up via CNI (bridge + NAT) so it gets outbound internet with no
// visibility into the host's — or rox_listener's own — network.
//
// containerd's default-generated OCI spec creates a new, isolated network
// namespace for every container but leaves it completely unconfigured
// (loopback only); normally a CRI plugin runs CNI to wire it up. Since we
// call containerd's client API directly, we do that wiring ourselves here.
type networkManager struct {
	cni gocni.CNI
}

func newNetworkManager() (*networkManager, error) {
	cni, err := gocni.New(
		gocni.WithMinNetworkCount(1),
		gocni.WithPluginConfDir(cniConfDir),
		gocni.WithPluginDir([]string{cniBinDir}),
	)
	if err != nil {
		return nil, fmt.Errorf("creating CNI client: %w", err)
	}
	if err := cni.Load(gocni.WithLoNetwork, gocni.WithConfListFile(cniConfList)); err != nil {
		return nil, fmt.Errorf("loading CNI network config from %s: %w", cniConfList, err)
	}
	return &networkManager{cni: cni}, nil
}

// setup creates a fresh network namespace and configures it via CNI. id
// must be unique per call (the container name works well) — CNI uses it to
// key the veth/iptables state it creates, so teardown can find it again.
// The returned path is where the namespace is persisted; pass it to
// oci.WithLinuxNamespace so the container joins it.
func (m *networkManager) setup(ctx context.Context, id string) (nsPath string, err error) {
	nsPath, err = newPersistentNetNS(id)
	if err != nil {
		return "", fmt.Errorf("creating network namespace: %w", err)
	}
	if _, err := m.cni.Setup(ctx, id, nsPath); err != nil {
		_ = removePersistentNetNS(nsPath)
		return "", fmt.Errorf("configuring CNI network for %s: %w", id, err)
	}
	return nsPath, nil
}

// teardown reverses setup.
func (m *networkManager) teardown(ctx context.Context, id, nsPath string) error {
	var errs []error
	if err := m.cni.Remove(ctx, id, nsPath); err != nil {
		errs = append(errs, fmt.Errorf("removing CNI network: %w", err))
	}
	if err := removePersistentNetNS(nsPath); err != nil {
		errs = append(errs, fmt.Errorf("removing network namespace: %w", err))
	}
	return errors.Join(errs...)
}

// newPersistentNetNS creates a new Linux network namespace and bind-mounts
// it to a stable path under /var/run/netns so it survives after this
// function returns — namespaces are normally destroyed once no process or
// mount references them. This is the pattern container runtimes are
// expected to implement themselves; see
// https://github.com/containernetworking/plugins/blob/main/pkg/ns/README.md#creating-network-namespaces.
func newPersistentNetNS(id string) (string, error) {
	if err := os.MkdirAll(netnsRunDir, 0o755); err != nil {
		return "", fmt.Errorf("creating %s: %w", netnsRunDir, err)
	}
	// Make the directory a shared mountpoint: bind mounts created under it
	// (including the one below) need to propagate out to the host, since
	// containerd's shims run there, not inside this container.
	if err := unix.Mount("", netnsRunDir, "none", unix.MS_SHARED|unix.MS_REC, ""); err != nil {
		if err != unix.EINVAL {
			return "", fmt.Errorf("mount --make-rshared %s: %w", netnsRunDir, err)
		}
		if err := unix.Mount(netnsRunDir, netnsRunDir, "none", unix.MS_BIND|unix.MS_REC, ""); err != nil {
			return "", fmt.Errorf("mount --rbind %s %s: %w", netnsRunDir, netnsRunDir, err)
		}
		if err := unix.Mount("", netnsRunDir, "none", unix.MS_SHARED|unix.MS_REC, ""); err != nil {
			return "", fmt.Errorf("mount --make-rshared %s: %w", netnsRunDir, err)
		}
	}

	suffix := make([]byte, 8)
	if _, err := rand.Read(suffix); err != nil {
		return "", fmt.Errorf("generating netns name: %w", err)
	}
	nsPath := filepath.Join(netnsRunDir, fmt.Sprintf("%s-%x", id, suffix))

	f, err := os.Create(nsPath)
	if err != nil {
		return "", fmt.Errorf("creating netns mountpoint %s: %w", nsPath, err)
	}
	_ = f.Close()

	var wg sync.WaitGroup
	var nsErr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		runtime.LockOSThread()

		origNS, err := ns.GetNS(currentThreadNetNSPath())
		if err != nil {
			nsErr = fmt.Errorf("getting current netns: %w", err)
			return
		}
		defer func() { _ = origNS.Close() }()

		if err := unix.Unshare(unix.CLONE_NEWNET); err != nil {
			nsErr = fmt.Errorf("unshare CLONE_NEWNET: %w", err)
			return
		}
		defer func() { _ = origNS.Set() }()

		if err := unix.Mount(currentThreadNetNSPath(), nsPath, "none", unix.MS_BIND, ""); err != nil {
			nsErr = fmt.Errorf("bind-mounting new netns at %s: %w", nsPath, err)
		}
	}()
	wg.Wait()

	if nsErr != nil {
		_ = os.Remove(nsPath)
		return "", nsErr
	}
	return nsPath, nil
}

func removePersistentNetNS(nsPath string) error {
	if err := unix.Unmount(nsPath, unix.MNT_DETACH); err != nil {
		return fmt.Errorf("unmounting %s: %w", nsPath, err)
	}
	return os.Remove(nsPath)
}

func currentThreadNetNSPath() string {
	return fmt.Sprintf("/proc/%d/task/%d/ns/net", os.Getpid(), unix.Gettid())
}
