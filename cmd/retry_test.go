package main

import (
	"context"
	"strconv"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

const (
	testNamespace = "dev"
	testHost      = "rsi-dev"
	testFunc      = "user-credential-check"
	testImage     = testHost + "-" + testFunc
	testBuild     = testImage + "-build-1"
	testPod       = testBuild + "-build-pod"
	testNode      = "gke-test-node-abc123"
)

// newFakeClient sets up the dynamic fake client with the four GVRs that
// retry.go touches. The kdexfunctions GVR is namespaced via a
// kdexfunctionsList registration; same for the kpack types. Nodes are
// cluster-scoped.
func newFakeClient(objs ...runtime.Object) *dynamicfake.FakeDynamicClient {
	scheme := runtime.NewScheme()
	gvrToListKind := map[schema.GroupVersionResource]string{
		knativeServiceGVR: "ServiceList",
		kdexFunctionGVR:   "KDexFunctionList",
		kpackImageGVR:     "ImageList",
		kpackBuildGVR:     "BuildList",
		podGVR:            "PodList",
		nodeGVR:           "NodeList",
	}
	return dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, gvrToListKind, objs...)
}

func newKDexFunction(retries int, lastRetryAt time.Time, exhausted bool) *unstructured.Unstructured {
	attrs := map[string]any{}
	if retries > 0 {
		attrs[attrRetries] = strconv.Itoa(retries)
	}
	if !lastRetryAt.IsZero() {
		attrs[attrLastRetryAt] = lastRetryAt.Format(time.RFC3339)
	}
	if exhausted {
		attrs[attrExhausted] = "true"
	}
	obj := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "kdex.dev/v1alpha1",
		"kind":       "KDexFunction",
		"metadata": map[string]any{
			"name":      testFunc,
			"namespace": testNamespace,
		},
		"status": map[string]any{
			"attributes": attrs,
		},
	}}
	return obj
}

func newKPackImage(latestImage, latestBuildRef string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "kpack.io/v1alpha2",
		"kind":       "Image",
		"metadata": map[string]any{
			"name":      testImage,
			"namespace": testNamespace,
		},
		"status": map[string]any{
			"latestImage":    latestImage,
			"latestBuildRef": latestBuildRef,
		},
	}}
}

func newKPackBuild(succeededStatus string, podName string) *unstructured.Unstructured {
	conds := []any{
		map[string]any{"type": "Succeeded", "status": succeededStatus},
	}
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "kpack.io/v1alpha2",
		"kind":       "Build",
		"metadata": map[string]any{
			"name":      testBuild,
			"namespace": testNamespace,
		},
		"status": map[string]any{
			"conditions": conds,
			"podName":    podName,
		},
	}}
}

// podOption mutates a Pod under construction. Used by newPod() to build
// the eight distinct preemption / non-preemption test fixtures without
// duplicating boilerplate.
type podOption func(*unstructured.Unstructured)

func newPod(opts ...podOption) *unstructured.Unstructured {
	p := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "Pod",
		"metadata": map[string]any{
			"name":      testPod,
			"namespace": testNamespace,
		},
		"spec": map[string]any{
			"nodeName": testNode,
		},
		"status": map[string]any{},
	}}
	for _, o := range opts {
		o(p)
	}
	return p
}

func withDeletionTimestamp(ts string) podOption {
	return func(p *unstructured.Unstructured) {
		_ = unstructured.SetNestedField(p.Object, ts, "metadata", "deletionTimestamp")
	}
}

func withPodReason(reason string) podOption {
	return func(p *unstructured.Unstructured) {
		_ = unstructured.SetNestedField(p.Object, reason, "status", "reason")
	}
}

func withInitTerminated(name, reason string, exitCode *int) podOption {
	return func(p *unstructured.Unstructured) {
		terminated := map[string]any{"reason": reason}
		if exitCode != nil {
			terminated["exitCode"] = int64(*exitCode)
		}
		statuses, _, _ := unstructured.NestedSlice(p.Object, "status", "initContainerStatuses")
		statuses = append(statuses, map[string]any{
			"name": name,
			"state": map[string]any{
				"terminated": terminated,
			},
		})
		_ = unstructured.SetNestedSlice(p.Object, statuses, "status", "initContainerStatuses")
	}
}

func newNode(ready string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "Node",
		"metadata":   map[string]any{"name": testNode},
		"status": map[string]any{
			"conditions": []any{
				map[string]any{"type": "Ready", "status": ready},
			},
		},
	}}
}

func intPtr(i int) *int { return &i }

// ---- buildFailed -----------------------------------------------------

func TestBuildFailed(t *testing.T) {
	cases := []struct {
		name   string
		status string
		want   bool
	}{
		{"succeeded", "True", false},
		{"in flight", "Unknown", false},
		{"failed", "False", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := buildFailed(newKPackBuild(tc.status, testPod)); got != tc.want {
				t.Fatalf("buildFailed=%v, want %v", got, tc.want)
			}
		})
	}
}

// ---- isBuildPodPreempted ---------------------------------------------

func TestIsBuildPodPreempted_NoPodName(t *testing.T) {
	client := newFakeClient()
	build := newKPackBuild("False", "")
	got, err := isBuildPodPreempted(context.Background(), client, build, testNamespace)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got {
		t.Fatal("expected false when build has no podName")
	}
}

func TestIsBuildPodPreempted_DeletionTimestamp(t *testing.T) {
	pod := newPod(withDeletionTimestamp("2026-05-20T15:00:00Z"))
	client := newFakeClient(pod, newNode("True"))
	got, err := isBuildPodPreempted(context.Background(), client, newKPackBuild("False", testPod), testNamespace)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !got {
		t.Fatal("expected true for DeletionTimestamp + no exit code")
	}
}

func TestIsBuildPodPreempted_DeletionTimestampWithExitCode(t *testing.T) {
	// DeletionTimestamp + a recorded exit code means the container finished
	// THEN the pod was deleted (normal cleanup), not preempted mid-run.
	pod := newPod(
		withDeletionTimestamp("2026-05-20T15:00:00Z"),
		withInitTerminated("build", "Completed", intPtr(0)),
	)
	client := newFakeClient(pod, newNode("True"))
	got, err := isBuildPodPreempted(context.Background(), client, newKPackBuild("False", testPod), testNamespace)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got {
		t.Fatal("expected false when an init container already exited cleanly")
	}
}

func TestIsBuildPodPreempted_NodeShutdownReason(t *testing.T) {
	client := newFakeClient(newPod(withPodReason("NodeShutdown")), newNode("True"))
	got, err := isBuildPodPreempted(context.Background(), client, newKPackBuild("False", testPod), testNamespace)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !got {
		t.Fatal("expected true for status.reason=NodeShutdown")
	}
}

func TestIsBuildPodPreempted_NodeNotReady(t *testing.T) {
	client := newFakeClient(newPod(), newNode("False"))
	got, err := isBuildPodPreempted(context.Background(), client, newKPackBuild("False", testPod), testNamespace)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !got {
		t.Fatal("expected true when Pod's node Ready=False")
	}
}

func TestIsBuildPodPreempted_NodeGone(t *testing.T) {
	// No Node object registered → IsNotFound → treat as preempted.
	client := newFakeClient(newPod())
	got, err := isBuildPodPreempted(context.Background(), client, newKPackBuild("False", testPod), testNamespace)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !got {
		t.Fatal("expected true when Pod's node no longer exists")
	}
}

func TestIsBuildPodPreempted_InitKilled(t *testing.T) {
	pod := newPod(withInitTerminated("build", "Killed", nil))
	client := newFakeClient(pod, newNode("True"))
	got, err := isBuildPodPreempted(context.Background(), client, newKPackBuild("False", testPod), testNamespace)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !got {
		t.Fatal("expected true for init container reason=Killed")
	}
}

func TestIsBuildPodPreempted_OOMKilled(t *testing.T) {
	// OOMKilled is OUR sizing problem, not infra preemption. Must NOT retry.
	pod := newPod(withInitTerminated("build", "OOMKilled", intPtr(137)))
	client := newFakeClient(pod, newNode("True"))
	got, err := isBuildPodPreempted(context.Background(), client, newKPackBuild("False", testPod), testNamespace)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got {
		t.Fatal("expected false for OOMKilled (genuine resource failure)")
	}
}

func TestIsBuildPodPreempted_BuildpackDetectFailure(t *testing.T) {
	// Detect exiting with exit code 20 is a genuine "no buildpack
	// matched the source" failure. Pod ran to completion; no preemption.
	pod := newPod(withInitTerminated("detect", "Error", intPtr(20)))
	client := newFakeClient(pod, newNode("True"))
	got, err := isBuildPodPreempted(context.Background(), client, newKPackBuild("False", testPod), testNamespace)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got {
		t.Fatal("expected false for genuine build failure (detect exit=20)")
	}
}

// ---- envInt / envDuration --------------------------------------------

func TestEnvInt(t *testing.T) {
	t.Setenv("FOO_RETRIES", "7")
	if got := envInt("FOO_RETRIES", 3); got != 7 {
		t.Fatalf("envInt=%d, want 7", got)
	}
	t.Setenv("FOO_RETRIES", "")
	if got := envInt("FOO_RETRIES", 3); got != 3 {
		t.Fatalf("envInt empty=%d, want default 3", got)
	}
	t.Setenv("FOO_RETRIES", "garbage")
	if got := envInt("FOO_RETRIES", 3); got != 3 {
		t.Fatalf("envInt garbage=%d, want default 3", got)
	}
}

func TestEnvDuration(t *testing.T) {
	t.Setenv("FOO_COOL", "12m")
	if got := envDuration("FOO_COOL", time.Minute); got != 12*time.Minute {
		t.Fatalf("envDuration=%v, want 12m", got)
	}
	t.Setenv("FOO_COOL", "garbage")
	if got := envDuration("FOO_COOL", time.Minute); got != time.Minute {
		t.Fatalf("envDuration garbage=%v, want default 1m", got)
	}
}

// ---- getRetryState / setRetryState -----------------------------------

func TestGetRetryState_Empty(t *testing.T) {
	kf := newKDexFunction(0, time.Time{}, false)
	r, l, e := getRetryState(kf)
	if r != 0 || !l.IsZero() || e {
		t.Fatalf("expected zero state, got (%d, %v, %v)", r, l, e)
	}
}

func TestGetRetryState_Populated(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	kf := newKDexFunction(2, now, false)
	r, l, e := getRetryState(kf)
	if r != 2 {
		t.Errorf("retries=%d, want 2", r)
	}
	if !l.Equal(now) {
		t.Errorf("lastRetryAt=%v, want %v", l, now)
	}
	if e {
		t.Error("exhausted should be false")
	}
}

func TestSetRetryState_Roundtrip(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	kf := newKDexFunction(0, time.Time{}, false)
	client := newFakeClient(kf)
	cfg := &EnvConfig{FunctionName: testFunc, FunctionNamespace: testNamespace}

	if err := setRetryState(context.Background(), client, cfg, 1, now, false); err != nil {
		t.Fatalf("setRetryState: %v", err)
	}

	updated, err := client.Resource(kdexFunctionGVR).Namespace(testNamespace).Get(context.Background(), testFunc, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get back: %v", err)
	}
	r, l, e := getRetryState(updated)
	if r != 1 || !l.Equal(now) || e {
		t.Fatalf("roundtrip mismatch: got (%d, %v, %v) want (1, %v, false)", r, l, e, now)
	}
}

// ---- checkAndRetryFailedBuild end-to-end -----------------------------

func TestCheckAndRetryFailedBuild_NoFunctionHost(t *testing.T) {
	cfg := &EnvConfig{FunctionName: testFunc, FunctionNamespace: testNamespace}
	client := newFakeClient()
	if err := checkAndRetryFailedBuild(context.Background(), client, newKDexFunction(0, time.Time{}, false), cfg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestCheckAndRetryFailedBuild_NoImage(t *testing.T) {
	// No kpack Image in the cluster (executable-mode function or
	// pre-build) — must no-op cleanly.
	cfg := &EnvConfig{FunctionName: testFunc, FunctionHost: testHost, FunctionNamespace: testNamespace}
	client := newFakeClient()
	if err := checkAndRetryFailedBuild(context.Background(), client, newKDexFunction(0, time.Time{}, false), cfg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestCheckAndRetryFailedBuild_PreemptedTriggersRetry(t *testing.T) {
	cfg := &EnvConfig{FunctionName: testFunc, FunctionHost: testHost, FunctionNamespace: testNamespace}
	pod := newPod(withDeletionTimestamp("2026-05-20T15:00:00Z"))
	build := newKPackBuild("False", testPod)
	img := newKPackImage("", testBuild)
	kf := newKDexFunction(0, time.Time{}, false)
	client := newFakeClient(kf, img, build, pod, newNode("True"))

	if err := checkAndRetryFailedBuild(context.Background(), client, kf, cfg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Image should now carry the trigger annotation
	gotImg, err := client.Resource(kpackImageGVR).Namespace(testNamespace).Get(context.Background(), testImage, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get image: %v", err)
	}
	if _, ok := gotImg.GetAnnotations()[buildNeededAnnotation]; !ok {
		t.Errorf("expected %s annotation on Image after preempted-build retry", buildNeededAnnotation)
	}

	// KDexFunction should have retries=1
	gotKf, err := client.Resource(kdexFunctionGVR).Namespace(testNamespace).Get(context.Background(), testFunc, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get kdexfunction: %v", err)
	}
	r, _, exhausted := getRetryState(gotKf)
	if r != 1 {
		t.Errorf("retries=%d, want 1", r)
	}
	if exhausted {
		t.Error("exhausted should be false at first retry")
	}
}

func TestCheckAndRetryFailedBuild_GenuineFailureNoRetry(t *testing.T) {
	cfg := &EnvConfig{FunctionName: testFunc, FunctionHost: testHost, FunctionNamespace: testNamespace}
	pod := newPod(withInitTerminated("build", "OOMKilled", intPtr(137)))
	build := newKPackBuild("False", testPod)
	img := newKPackImage("", testBuild)
	kf := newKDexFunction(0, time.Time{}, false)
	client := newFakeClient(kf, img, build, pod, newNode("True"))

	if err := checkAndRetryFailedBuild(context.Background(), client, kf, cfg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	gotImg, _ := client.Resource(kpackImageGVR).Namespace(testNamespace).Get(context.Background(), testImage, metav1.GetOptions{})
	if _, ok := gotImg.GetAnnotations()[buildNeededAnnotation]; ok {
		t.Error("expected NO trigger annotation on Image for OOMKilled genuine failure")
	}
	gotKf, _ := client.Resource(kdexFunctionGVR).Namespace(testNamespace).Get(context.Background(), testFunc, metav1.GetOptions{})
	if r, _, _ := getRetryState(gotKf); r != 0 {
		t.Errorf("retries=%d, want 0 (budget untouched)", r)
	}
}

func TestCheckAndRetryFailedBuild_BudgetExhausted(t *testing.T) {
	t.Setenv("MAX_BUILD_RETRIES", "3")
	cfg := &EnvConfig{FunctionName: testFunc, FunctionHost: testHost, FunctionNamespace: testNamespace}
	pod := newPod(withDeletionTimestamp("2026-05-20T15:00:00Z"))
	build := newKPackBuild("False", testPod)
	img := newKPackImage("", testBuild)
	kf := newKDexFunction(3, time.Now().Add(-1*time.Hour).UTC(), false)
	client := newFakeClient(kf, img, build, pod, newNode("True"))

	if err := checkAndRetryFailedBuild(context.Background(), client, kf, cfg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	gotImg, _ := client.Resource(kpackImageGVR).Namespace(testNamespace).Get(context.Background(), testImage, metav1.GetOptions{})
	if _, ok := gotImg.GetAnnotations()[buildNeededAnnotation]; ok {
		t.Error("expected NO trigger annotation when budget exhausted")
	}
	gotKf, _ := client.Resource(kdexFunctionGVR).Namespace(testNamespace).Get(context.Background(), testFunc, metav1.GetOptions{})
	if _, _, exh := getRetryState(gotKf); !exh {
		t.Error("expected exhausted=true after budget hit")
	}
}

func TestCheckAndRetryFailedBuild_CooldownActive(t *testing.T) {
	t.Setenv("MAX_BUILD_RETRIES", "3")
	t.Setenv("RETRY_COOLDOWN", "30m")
	cfg := &EnvConfig{FunctionName: testFunc, FunctionHost: testHost, FunctionNamespace: testNamespace}
	pod := newPod(withDeletionTimestamp("2026-05-20T15:00:00Z"))
	build := newKPackBuild("False", testPod)
	img := newKPackImage("", testBuild)
	kf := newKDexFunction(1, time.Now().Add(-5*time.Minute).UTC(), false) // 5 min ago, cooldown 30 min
	client := newFakeClient(kf, img, build, pod, newNode("True"))

	if err := checkAndRetryFailedBuild(context.Background(), client, kf, cfg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	gotImg, _ := client.Resource(kpackImageGVR).Namespace(testNamespace).Get(context.Background(), testImage, metav1.GetOptions{})
	if _, ok := gotImg.GetAnnotations()[buildNeededAnnotation]; ok {
		t.Error("expected NO trigger annotation while cooldown active")
	}
}

func TestCheckAndRetryFailedBuild_ResetOnSuccess(t *testing.T) {
	cfg := &EnvConfig{FunctionName: testFunc, FunctionHost: testHost, FunctionNamespace: testNamespace}
	img := newKPackImage("registry/foo@sha256:abc", testBuild) // latestImage populated → success
	kf := newKDexFunction(2, time.Now().Add(-1*time.Hour).UTC(), false)
	client := newFakeClient(kf, img)

	if err := checkAndRetryFailedBuild(context.Background(), client, kf, cfg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	gotKf, _ := client.Resource(kdexFunctionGVR).Namespace(testNamespace).Get(context.Background(), testFunc, metav1.GetOptions{})
	if r, l, _ := getRetryState(gotKf); r != 0 || !l.IsZero() {
		t.Errorf("expected retry state reset after success, got (%d, %v)", r, l)
	}
}
