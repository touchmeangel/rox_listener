//go:build integration

package kube

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

var envCS kubernetes.Interface

func TestMain(m *testing.M) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		os.Exit(m.Run()) // integration tests skip, or fail if REQUIRE_INTEGRATION is set
	}

	env := &envtest.Environment{}
	cfg, err := env.Start()
	if err != nil {
		fmt.Fprintln(os.Stderr, "starting envtest:", err)
		os.Exit(1)
	}
	envCS = kubernetes.NewForConfigOrDie(cfg)
	_, err = envCS.CoreV1().Namespaces().Create(context.Background(),
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}, metav1.CreateOptions{})
	if err != nil {
		fmt.Fprintln(os.Stderr, "creating namespace:", err)
		_ = env.Stop()
		os.Exit(1)
	}

	code := m.Run()
	_ = env.Stop()
	os.Exit(code)
}

func requireEnvtest(t *testing.T) kubernetes.Interface {
	t.Helper()
	if envCS == nil {
		if os.Getenv("REQUIRE_INTEGRATION") != "" {
			t.Fatal("KUBEBUILDER_ASSETS is not set, but envtest binaries are required in CI")
		}
		t.Skip(`KUBEBUILDER_ASSETS not set; try: eval "$(setup-envtest use -p env)"`)
	}
	return envCS
}

func fullSpec(name string) RunSpec {
	return RunSpec{
		Name:    name,
		Image:   "agent:test",
		Args:    []string{"coordinator"},
		Env:     []string{"ROX_RESULT_PUT_URL=http://storage.invalid/result"},
		CPU:     "500m",
		Memory:  "256Mi",
		Timeout: time.Minute,
		Scratch: []string{"/work"},
		Files:   []File{{Path: "/app/config.json", Content: []byte("{}")}},
		Init: []InitStep{{
			Name: "fetch-workspace", Image: "listener:test",
			Args: []string{"fetch-workspace"}, Env: []string{"S3_BUCKET=test"},
		}},
	}
}

func createOnAPIServer(cs kubernetes.Interface, c *Client, spec RunSpec) error {
	secret, job, err := c.build(spec)
	if err != nil {
		return nil
	}
	ctx := context.Background()
	if _, err := cs.CoreV1().Secrets(ns).Create(ctx, secret, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("secret: %w", err)
	}
	if _, err := cs.BatchV1().Jobs(ns).Create(ctx, job, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("job: %w", err)
	}
	return nil
}

func TestIntegration_BuildOutputIsAcceptedByAPIServer(t *testing.T) {
	cs := requireEnvtest(t)
	c := NewWithClientset(cs, Config{Namespace: ns})

	require.NoError(t, createOnAPIServer(cs, c, fullSpec("it-accepted")))
}

func TestIntegration_BuildNeverEmitsInvalidObjects(t *testing.T) {
	cs := requireEnvtest(t)
	c := NewWithClientset(cs, Config{Namespace: ns})

	cases := []struct {
		name   string
		mutate func(*RunSpec)
	}{
		{"uppercase name", func(s *RunSpec) { s.Name = "it-case-0-Upper" }},
		{"underscore in name", func(s *RunSpec) { s.Name = "it-case_1" }},
		{"env var name has space", func(s *RunSpec) { s.Env = []string{"BAD NAME=1"} }},
		{"duplicate mount path", func(s *RunSpec) { s.Scratch = []string{"/work", "/work"} }},
		{"init name not a DNS label", func(s *RunSpec) { s.Init[0].Name = "Fetch_Workspace" }},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			spec := fullSpec(fmt.Sprintf("it-case-%d", i))
			tc.mutate(&spec)
			require.NoError(t, createOnAPIServer(cs, c, spec),
				"build() accepted a spec the API server rejects; validate it in build()")
		})
	}
}
