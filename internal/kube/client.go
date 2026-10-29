package kube

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/utils/ptr"
)

const (
	labelTask    = "rox.io/task"
	agentName    = "rox"
	maxSecretLen = 900 << 10 // Secrets are capped at 1 MiB, in future we wont need very long config.json
)

type Config struct {
	Kubeconfig     string
	Namespace      string
	RuntimeClass   string
	PullPolicy     corev1.PullPolicy
	PollInterval   time.Duration
	DefaultTimeout time.Duration
}

type Client struct {
	cs  kubernetes.Interface
	cfg Config
}

func New(cfg Config) (*Client, error) {
	rc, err := restConfig(cfg.Kubeconfig)
	if err != nil {
		return nil, fmt.Errorf("loading kubernetes config: %w", err)
	}
	cs, err := kubernetes.NewForConfig(rc)
	if err != nil {
		return nil, fmt.Errorf("building kubernetes client: %w", err)
	}
	return NewWithClientset(cs, cfg), nil
}

func NewWithClientset(cs kubernetes.Interface, cfg Config) *Client {
	if cfg.Namespace == "" {
		cfg.Namespace = "rox"
	}
	if cfg.PullPolicy == "" {
		cfg.PullPolicy = corev1.PullIfNotPresent
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 2 * time.Second
	}
	if cfg.DefaultTimeout <= 0 {
		cfg.DefaultTimeout = 2 * time.Hour
	}
	return &Client{cs: cs, cfg: cfg}
}

func restConfig(kubeconfig string) (*rest.Config, error) {
	if kubeconfig == "" {
		if rc, err := rest.InClusterConfig(); err == nil {
			return rc, nil
		}
	}
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	rules.ExplicitPath = kubeconfig
	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		rules, &clientcmd.ConfigOverrides{}).ClientConfig()
}

func (c *Client) Ping(ctx context.Context) error {
	_, err := c.cs.BatchV1().Jobs(c.cfg.Namespace).List(ctx, metav1.ListOptions{Limit: 1})
	return err
}

func (c *Client) Run(ctx context.Context, spec RunSpec) (*Result, error) {
	if spec.Timeout <= 0 {
		spec.Timeout = c.cfg.DefaultTimeout
	}
	secret, job, err := c.build(spec)
	if err != nil {
		return nil, err
	}

	secrets := c.cs.CoreV1().Secrets(c.cfg.Namespace)
	jobs := c.cs.BatchV1().Jobs(c.cfg.Namespace)

	createdSecret, err := secrets.Create(ctx, secret, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("creating secret: %w", err)
	}
	defer c.cleanup(ctx, spec.Name)

	createdJob, err := jobs.Create(ctx, job, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("creating job: %w", err)
	}

	createdSecret.OwnerReferences = []metav1.OwnerReference{{
		APIVersion: "batch/v1", Kind: "Job",
		Name: createdJob.Name, UID: createdJob.UID,
	}}
	if _, err := secrets.Update(ctx, createdSecret, metav1.UpdateOptions{}); err != nil {
		return nil, fmt.Errorf("attaching secret to job: %w", err)
	}

	failedReason, err := c.wait(ctx, spec.Name)
	if err != nil {
		return nil, err
	}
	return c.collect(ctx, spec.Name, failedReason)
}

func (c *Client) build(spec RunSpec) (*corev1.Secret, *batchv1.Job, error) {
	if n := len(spec.Name); n == 0 || n > 63 {
		return nil, nil, fmt.Errorf("task name %q must be 1-63 characters", spec.Name)
	}
	if spec.Image == "" {
		return nil, nil, fmt.Errorf("image is required")
	}
	res, err := resources(spec.CPU, spec.Memory)
	if err != nil {
		return nil, nil, err
	}

	labels := map[string]string{
		"app.kubernetes.io/managed-by": "rox-listener",
		"rox.io/role":                  "agent",
		labelTask:                      spec.Name,
	}

	data := map[string][]byte{}
	var volumes []corev1.Volume
	var scratchMounts, fileMounts []corev1.VolumeMount

	for i, path := range spec.Scratch {
		vol := fmt.Sprintf("scratch-%d", i)
		volumes = append(volumes, corev1.Volume{Name: vol,
			VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}})
		scratchMounts = append(scratchMounts, corev1.VolumeMount{Name: vol, MountPath: path})
	}

	if len(spec.Files) > 0 {
		volumes = append(volumes, corev1.Volume{Name: "files",
			VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: spec.Name}}})
	}
	for i, f := range spec.Files {
		key := fmt.Sprintf("file-%d", i)
		data[key] = f.Content
		fileMounts = append(fileMounts, corev1.VolumeMount{
			Name: "files", MountPath: f.Path, SubPath: key, ReadOnly: true})
	}

	var inits []corev1.Container
	for _, s := range spec.Init {
		inits = append(inits, corev1.Container{
			Name: s.Name, Image: s.Image, ImagePullPolicy: c.cfg.PullPolicy,
			Args:         s.Args,
			Env:          secretEnv(data, spec.Name, "init."+s.Name, s.Env),
			VolumeMounts: scratchMounts,
		})
	}

	agent := corev1.Container{
		Name: agentName, Image: spec.Image, ImagePullPolicy: c.cfg.PullPolicy,
		Args:         spec.Args,
		Env:          secretEnv(data, spec.Name, agentName, spec.Env),
		VolumeMounts: append(append([]corev1.VolumeMount{}, scratchMounts...), fileMounts...),
		Resources:    res,
	}

	total := 0
	for _, v := range data {
		total += len(v)
	}
	if total > maxSecretLen {
		return nil, nil, fmt.Errorf("payload is %d bytes, over the ~1 MiB Secret limit", total)
	}

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: spec.Name, Labels: labels},
		Type:       corev1.SecretTypeOpaque,
		Data:       data,
	}
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: spec.Name, Labels: labels},
		Spec: batchv1.JobSpec{
			BackoffLimit:            ptr.To[int32](0),
			ActiveDeadlineSeconds:   ptr.To(int64(spec.Timeout.Seconds())),
			TTLSecondsAfterFinished: ptr.To[int32](600),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					RestartPolicy:                corev1.RestartPolicyNever,
					AutomountServiceAccountToken: ptr.To(false),
					EnableServiceLinks:           ptr.To(false),
					InitContainers:               inits,
					Containers:                   []corev1.Container{agent},
					Volumes:                      volumes,
				},
			},
		},
	}
	if c.cfg.RuntimeClass != "" {
		job.Spec.Template.Spec.RuntimeClassName = ptr.To(c.cfg.RuntimeClass)
	}
	return secret, job, nil
}

// secretEnv stores each KEY=VALUE in the Secret and returns env entries that
// reference it, so values never appear in the Job or Pod spec. The scope
// prefix keeps the init step's env separate from the agent's.
func secretEnv(data map[string][]byte, secretName, scope string, kv []string) []corev1.EnvVar {
	out := make([]corev1.EnvVar, 0, len(kv))
	for _, entry := range kv {
		name, value, ok := strings.Cut(entry, "=")
		if !ok || name == "" {
			continue
		}
		key := "env." + scope + "." + name
		data[key] = []byte(value)
		out = append(out, corev1.EnvVar{Name: name, ValueFrom: &corev1.EnvVarSource{
			SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: secretName},
				Key:                  key,
			},
		}})
	}
	return out
}

func resources(cpu, mem string) (corev1.ResourceRequirements, error) {
	list := corev1.ResourceList{}
	for name, val := range map[corev1.ResourceName]string{corev1.ResourceCPU: cpu, corev1.ResourceMemory: mem} {
		if val == "" {
			continue
		}
		q, err := resource.ParseQuantity(val)
		if err != nil {
			return corev1.ResourceRequirements{}, fmt.Errorf("invalid %s %q: %w", name, val, err)
		}
		list[name] = q
	}
	return corev1.ResourceRequirements{Requests: list, Limits: list.DeepCopy()}, nil
}

func (c *Client) wait(ctx context.Context, name string) (failedReason string, err error) {
	jobs := c.cs.BatchV1().Jobs(c.cfg.Namespace)
	tick := time.NewTicker(c.cfg.PollInterval)
	defer tick.Stop()

	apiErrs := 0
	for {
		job, err := jobs.Get(ctx, name, metav1.GetOptions{})
		switch {
		case err == nil:
			apiErrs = 0
			if done, reason := finished(job); done {
				return reason, nil
			}
			if msg := c.stuck(ctx, name); msg != "" {
				return "", fmt.Errorf("task %s cannot start: %s", name, msg)
			}
		case ctx.Err() != nil:
			return "", ctx.Err()
		case apierrors.IsNotFound(err):
			return "", fmt.Errorf("job %s was deleted while running", name)
		default: // transient API error: keep polling, but not forever
			if apiErrs++; apiErrs > 10 {
				return "", fmt.Errorf("polling job %s: %w", name, err)
			}
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-tick.C:
		}
	}
}

func finished(job *batchv1.Job) (bool, string) {
	for _, cond := range job.Status.Conditions {
		if cond.Status != corev1.ConditionTrue {
			continue
		}
		switch cond.Type {
		case batchv1.JobComplete:
			return true, ""
		case batchv1.JobFailed:
			return true, cond.Reason
		}
	}
	return false, ""
}

var fatalWaiting = map[string]bool{
	"ImagePullBackOff": true, "InvalidImageName": true, "CreateContainerConfigError": true,
}

func (c *Client) stuck(ctx context.Context, name string) string {
	pods, err := c.pods(ctx, name)
	if err != nil {
		return ""
	}
	for _, p := range pods {
		all := append(append([]corev1.ContainerStatus{}, p.Status.InitContainerStatuses...),
			p.Status.ContainerStatuses...)
		for _, st := range all {
			if w := st.State.Waiting; w != nil && fatalWaiting[w.Reason] {
				return fmt.Sprintf("container %s: %s: %s", st.Name, w.Reason, w.Message)
			}
		}
	}
	return ""
}

func (c *Client) pods(ctx context.Context, name string) ([]corev1.Pod, error) {
	list, err := c.cs.CoreV1().Pods(c.cfg.Namespace).List(ctx,
		metav1.ListOptions{LabelSelector: labelTask + "=" + name})
	if err != nil {
		return nil, err
	}
	items := list.Items
	sort.Slice(items, func(i, j int) bool {
		return items[i].CreationTimestamp.Time.After(items[j].CreationTimestamp.Time)
	})
	return items, nil
}

func (c *Client) collect(ctx context.Context, name, failedReason string) (*Result, error) {
	pods, err := c.pods(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("listing pods for %s: %w", name, err)
	}
	if len(pods) == 0 {
		if failedReason != "" {
			return &Result{ExitCode: -1, Reason: failedReason}, nil
		}
		return nil, fmt.Errorf("job %s finished but left no pod to inspect", name)
	}
	pod := &pods[0]

	for _, st := range pod.Status.InitContainerStatuses {
		if t := st.State.Terminated; t != nil && t.ExitCode != 0 {
			return nil, fmt.Errorf("init step %q failed (exit %d, %s):\n%s",
				st.Name, t.ExitCode, t.Reason, c.logs(ctx, pod.Name, st.Name))
		}
	}

	res := &Result{ExitCode: -1, Reason: failedReason}
	for _, st := range pod.Status.ContainerStatuses {
		if t := st.State.Terminated; st.Name == agentName && t != nil {
			res.ExitCode = t.ExitCode
			if t.Reason != "" {
				res.Reason = t.Reason
			}
		}
	}
	if failedReason == "DeadlineExceeded" {
		res.Reason = failedReason
	}
	res.Logs = c.logs(ctx, pod.Name, agentName)
	return res, nil
}

func (c *Client) logs(ctx context.Context, pod, container string) string {
	req := c.cs.CoreV1().Pods(c.cfg.Namespace).GetLogs(pod, &corev1.PodLogOptions{
		Container: container, TailLines: ptr.To(int64(200)),
	})
	rc, err := req.Stream(ctx)
	if err != nil {
		return fmt.Sprintf("(could not read logs: %v)", err)
	}
	defer func() { _ = rc.Close() }()
	b, _ := io.ReadAll(io.LimitReader(rc, 64<<10))
	return string(b)
}

func (c *Client) cleanup(parent context.Context, name string) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), 30*time.Second)
	defer cancel()

	policy := metav1.DeletePropagationBackground
	err := c.cs.BatchV1().Jobs(c.cfg.Namespace).Delete(ctx, name, metav1.DeleteOptions{PropagationPolicy: &policy})
	if err != nil && !apierrors.IsNotFound(err) {
		slog.Error("deleting job", "name", name, "error", err)
	}
	err = c.cs.CoreV1().Secrets(c.cfg.Namespace).Delete(ctx, name, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		slog.Error("deleting secret", "name", name, "error", err)
	}
}
