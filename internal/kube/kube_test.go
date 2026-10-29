package kube

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

const ns = "rox"

func validSpec() RunSpec {
	return RunSpec{Name: "run-1", Image: "agent:test", Args: []string{"coordinator"}, Timeout: time.Minute}
}

func newClient(t *testing.T, cfg Config) (*Client, *fake.Clientset) {
	t.Helper()
	cs := fake.NewSimpleClientset()
	cfg.Namespace = ns
	cfg.PollInterval = 5 * time.Millisecond
	return NewWithClientset(cs, cfg), cs
}

func terminated(name string, code int32, reason string) corev1.ContainerStatus {
	return corev1.ContainerStatus{Name: name, State: corev1.ContainerState{
		Terminated: &corev1.ContainerStateTerminated{ExitCode: code, Reason: reason}}}
}

func waiting(name, reason string) corev1.ContainerStatus {
	return corev1.ContainerStatus{Name: name, State: corev1.ContainerState{
		Waiting: &corev1.ContainerStateWaiting{Reason: reason, Message: "test"}}}
}

func podFor(task string, init, main []corev1.ContainerStatus) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              task + "-pod",
			Namespace:         ns,
			Labels:            map[string]string{labelTask: task},
			CreationTimestamp: metav1.Now(),
		},
		Status: corev1.PodStatus{InitContainerStatuses: init, ContainerStatuses: main},
	}
}

func finishJob(t *testing.T, cs *fake.Clientset, name string, cond batchv1.JobConditionType, reason string, pod *corev1.Pod) {
	t.Helper()
	go func() {
		ctx := context.Background()
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			job, err := cs.BatchV1().Jobs(ns).Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				time.Sleep(2 * time.Millisecond)
				continue
			}
			if pod != nil {
				_, _ = cs.CoreV1().Pods(ns).Create(ctx, pod, metav1.CreateOptions{})
			}
			job.Status.Conditions = []batchv1.JobCondition{{
				Type: cond, Status: corev1.ConditionTrue, Reason: reason}}
			_, _ = cs.BatchV1().Jobs(ns).UpdateStatus(ctx, job, metav1.UpdateOptions{})
			return
		}
	}()
}

func requireClean(t *testing.T, cs *fake.Clientset) {
	t.Helper()
	jobs, err := cs.BatchV1().Jobs(ns).List(context.Background(), metav1.ListOptions{})
	require.NoError(t, err)
	require.Empty(t, jobs.Items, "job left behind")
	secrets, err := cs.CoreV1().Secrets(ns).List(context.Background(), metav1.ListOptions{})
	require.NoError(t, err)
	require.Empty(t, secrets.Items, "secret left behind")
}

func TestBuild_RejectsBadSpecs(t *testing.T) {
	c, _ := newClient(t, Config{})

	cases := []struct {
		name   string
		mutate func(*RunSpec)
	}{
		{"empty name", func(s *RunSpec) { s.Name = "" }},
		{"name over 63 chars", func(s *RunSpec) { s.Name = strings.Repeat("a", 64) }},
		// The next two fail until build() validates with validation.IsDNS1123Label.
		{"uppercase name", func(s *RunSpec) { s.Name = "Run-1" }},
		{"underscore in name", func(s *RunSpec) { s.Name = "run_1" }},
		{"missing image", func(s *RunSpec) { s.Image = "" }},
		{"unparseable cpu", func(s *RunSpec) { s.CPU = "lots" }},
		{"payload over limit", func(s *RunSpec) {
			s.Files = []File{{Path: "/app/big", Content: make([]byte, maxSecretLen+1)}}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			spec := validSpec()
			tc.mutate(&spec)
			_, _, err := c.build(spec)
			require.Error(t, err)
		})
	}
}

func TestBuild_SecretValuesStayOutOfPodSpec(t *testing.T) {
	c, _ := newClient(t, Config{})
	spec := validSpec()
	spec.Env = []string{"API_KEY=super-secret-value"}
	spec.Files = []File{{Path: "/app/config.json", Content: []byte(`{"token":"another-secret"}`)}}

	secret, job, err := c.build(spec)
	require.NoError(t, err)

	raw, err := json.Marshal(job)
	require.NoError(t, err)
	require.NotContains(t, string(raw), "super-secret-value")
	require.NotContains(t, string(raw), "another-secret")
	require.Equal(t, []byte("super-secret-value"), secret.Data["env.rox.API_KEY"])
}

func TestBuild_Mounts(t *testing.T) {
	c, _ := newClient(t, Config{})
	spec := validSpec()
	spec.Scratch = []string{"/work"}
	spec.Files = []File{{Path: "/app/config.json", Content: []byte("{}")}}
	spec.Init = []InitStep{{Name: "fetch-workspace", Image: "listener:test", Args: []string{"fetch-workspace"}}}

	_, job, err := c.build(spec)
	require.NoError(t, err)
	pod := job.Spec.Template.Spec

	require.Len(t, pod.InitContainers, 1)
	require.Len(t, pod.InitContainers[0].VolumeMounts, 1)
	require.Equal(t, "/work", pod.InitContainers[0].VolumeMounts[0].MountPath)

	agent := pod.Containers[0]
	require.Len(t, agent.VolumeMounts, 2)
	require.Equal(t, "/work", agent.VolumeMounts[0].MountPath)
	require.Equal(t, "/app/config.json", agent.VolumeMounts[1].MountPath)
	require.Equal(t, "file-0", agent.VolumeMounts[1].SubPath)
	require.True(t, agent.VolumeMounts[1].ReadOnly)
}

func TestBuild_Hardening(t *testing.T) {
	c, _ := newClient(t, Config{RuntimeClass: "gvisor"})
	spec := validSpec()
	spec.Timeout = 90 * time.Second

	_, job, err := c.build(spec)
	require.NoError(t, err)
	pod := job.Spec.Template.Spec

	require.Equal(t, corev1.RestartPolicyNever, pod.RestartPolicy)
	require.False(t, *pod.AutomountServiceAccountToken)
	require.False(t, *pod.EnableServiceLinks)
	require.Equal(t, "gvisor", *pod.RuntimeClassName)
	require.EqualValues(t, 0, *job.Spec.BackoffLimit)
	require.EqualValues(t, 90, *job.Spec.ActiveDeadlineSeconds)
	require.Equal(t, "rox-listener", job.Labels["app.kubernetes.io/managed-by"])
}

func jobWith(typ batchv1.JobConditionType, status corev1.ConditionStatus, reason string) *batchv1.Job {
	return &batchv1.Job{Status: batchv1.JobStatus{Conditions: []batchv1.JobCondition{{
		Type: typ, Status: status, Reason: reason}}}}
}

func TestFinished(t *testing.T) {
	cases := []struct {
		name   string
		job    *batchv1.Job
		done   bool
		reason string
	}{
		{"no conditions", &batchv1.Job{}, false, ""},
		{"complete", jobWith(batchv1.JobComplete, corev1.ConditionTrue, ""), true, ""},
		{"failed carries its reason", jobWith(batchv1.JobFailed, corev1.ConditionTrue, "DeadlineExceeded"), true, "DeadlineExceeded"},
		{"failed condition that is false", jobWith(batchv1.JobFailed, corev1.ConditionFalse, "x"), false, ""},
		{"suspended is not finished", jobWith(batchv1.JobSuspended, corev1.ConditionTrue, ""), false, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			done, reason := finished(tc.job)
			require.Equal(t, tc.done, done)
			require.Equal(t, tc.reason, reason)
		})
	}
}

func TestRun_AgentOutcomes(t *testing.T) {
	cases := []struct {
		name       string
		cond       batchv1.JobConditionType
		jobReason  string
		agent      corev1.ContainerStatus
		wantExit   int32
		wantReason string
	}{
		{"success", batchv1.JobComplete, "", terminated(agentName, 0, "Completed"), 0, "Completed"},
		{"agent exits 1", batchv1.JobFailed, "BackoffLimitExceeded", terminated(agentName, 1, "Error"), 1, "Error"},
		{"oom killed", batchv1.JobFailed, "BackoffLimitExceeded", terminated(agentName, 137, "OOMKilled"), 137, "OOMKilled"},
		{"deadline wins over container reason", batchv1.JobFailed, "DeadlineExceeded", terminated(agentName, 137, "Error"), 137, "DeadlineExceeded"},
		{"agent never started", batchv1.JobFailed, "BackoffLimitExceeded", corev1.ContainerStatus{Name: agentName}, -1, "BackoffLimitExceeded"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, cs := newClient(t, Config{})
			spec := validSpec()
			finishJob(t, cs, spec.Name, tc.cond, tc.jobReason,
				podFor(spec.Name, nil, []corev1.ContainerStatus{tc.agent}))

			res, err := c.Run(context.Background(), spec)
			require.NoError(t, err) // an agent failure is a Result, not an error
			require.EqualValues(t, tc.wantExit, res.ExitCode)
			require.Equal(t, tc.wantReason, res.Reason)
			require.NotEmpty(t, res.Logs) // the fake serves canned logs; we only check the path ran
			requireClean(t, cs)
		})
	}
}

func TestRun_InitStepFailureIsAnError(t *testing.T) {
	c, cs := newClient(t, Config{})
	spec := validSpec()
	finishJob(t, cs, spec.Name, batchv1.JobFailed, "BackoffLimitExceeded",
		podFor(spec.Name, []corev1.ContainerStatus{terminated("fetch-workspace", 2, "Error")}, nil))

	_, err := c.Run(context.Background(), spec)
	require.ErrorContains(t, err, `init step "fetch-workspace" failed (exit 2`)
	requireClean(t, cs)
}

func TestRun_FailedJobWithoutPod(t *testing.T) {
	c, cs := newClient(t, Config{})
	spec := validSpec()
	finishJob(t, cs, spec.Name, batchv1.JobFailed, "DeadlineExceeded", nil)

	res, err := c.Run(context.Background(), spec)
	require.NoError(t, err)
	require.EqualValues(t, -1, res.ExitCode)
	require.Equal(t, "DeadlineExceeded", res.Reason)
	requireClean(t, cs)
}

func TestRun_FailsFastOnImagePullBackOff(t *testing.T) {
	c, cs := newClient(t, Config{})
	spec := validSpec()
	_, err := cs.CoreV1().Pods(ns).Create(context.Background(),
		podFor(spec.Name, nil, []corev1.ContainerStatus{waiting(agentName, "ImagePullBackOff")}),
		metav1.CreateOptions{})
	require.NoError(t, err)

	_, err = c.Run(context.Background(), spec)
	require.ErrorContains(t, err, "cannot start")
	require.ErrorContains(t, err, "ImagePullBackOff")
	requireClean(t, cs)
}

func TestRun_BadSpecTouchesNothing(t *testing.T) {
	c, cs := newClient(t, Config{})
	spec := validSpec()
	spec.Image = ""

	_, err := c.Run(context.Background(), spec)
	require.Error(t, err)
	require.Empty(t, cs.Actions(), "an invalid spec must be rejected before any API call")
}

func TestRun_SecretIsOwnedByJob(t *testing.T) {
	c, cs := newClient(t, Config{})
	var updated *corev1.Secret
	cs.PrependReactor("update", "secrets", func(a k8stesting.Action) (bool, runtime.Object, error) {
		updated = a.(k8stesting.UpdateAction).GetObject().(*corev1.Secret).DeepCopy()
		return false, nil, nil // observe only, let the fake handle it
	})
	spec := validSpec()
	finishJob(t, cs, spec.Name, batchv1.JobComplete, "",
		podFor(spec.Name, nil, []corev1.ContainerStatus{terminated(agentName, 0, "Completed")}))

	_, err := c.Run(context.Background(), spec)
	require.NoError(t, err)
	require.NotNil(t, updated, "secret was never attached to the job")
	require.Len(t, updated.OwnerReferences, 1)
	require.Equal(t, "Job", updated.OwnerReferences[0].Kind)
	require.Equal(t, spec.Name, updated.OwnerReferences[0].Name)
}

func TestRun_JobCreateFailureStillRemovesSecret(t *testing.T) {
	c, cs := newClient(t, Config{})
	cs.PrependReactor("create", "jobs", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("quota exceeded")
	})

	_, err := c.Run(context.Background(), validSpec())
	require.ErrorContains(t, err, "creating job")
	requireClean(t, cs)
}

func TestRun_SurvivesTransientAPIErrors(t *testing.T) {
	c, cs := newClient(t, Config{})
	var gets atomic.Int32
	cs.PrependReactor("get", "jobs", func(k8stesting.Action) (bool, runtime.Object, error) {
		if gets.Add(1) <= 3 {
			return true, nil, errors.New("connection reset")
		}
		return false, nil, nil
	})
	spec := validSpec()
	finishJob(t, cs, spec.Name, batchv1.JobComplete, "",
		podFor(spec.Name, nil, []corev1.ContainerStatus{terminated(agentName, 0, "Completed")}))

	res, err := c.Run(context.Background(), spec)
	require.NoError(t, err)
	require.EqualValues(t, 0, res.ExitCode)
}

func TestRun_GivesUpOnPersistentAPIErrors(t *testing.T) {
	c, cs := newClient(t, Config{})
	cs.PrependReactor("get", "jobs", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("apiserver down")
	})

	_, err := c.Run(context.Background(), validSpec())
	require.ErrorContains(t, err, "polling job")
	requireClean(t, cs)
}

func TestRun_CancelledContextStillCleansUp(t *testing.T) {
	c, cs := newClient(t, Config{})
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(50*time.Millisecond, cancel)

	_, err := c.Run(ctx, validSpec())
	require.ErrorIs(t, err, context.Canceled)
	requireClean(t, cs)
}
