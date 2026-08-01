package main

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic"
)

// perHostFunction is the KDexFunction an iterating observer hands to
// checkAndRetryFailedBuild. Under a per-host CronJob this is the only
// place the function's identity exists -- env carries the host, not the
// name.
func perHostFunction() *unstructured.Unstructured {
	return newKDexFunction(0, time.Time{}, false)
}

// preemptedBuildFixture is a function whose kpack Build died to node
// preemption: Image has no latestImage, latest Build failed, and the
// Build's Pod reports NodeShutdown. Auto-recovery must fire on this.
func preemptedBuildFixture() []runtime.Object {
	return []runtime.Object{
		newKPackImage("", testBuild),
		newKPackBuild("False", testPod),
		newPod(withPodReason("NodeShutdown")),
		newNode("True"),
		perHostFunction(),
	}
}

func rebuildTriggered(t *testing.T, client dynamic.Interface) bool {
	t.Helper()
	img, err := client.Resource(kpackImageGVR).Namespace(testNamespace).
		Get(context.Background(), testImage, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get kpack Image: %v", err)
	}
	_, ok := img.GetAnnotations()[buildNeededAnnotation]
	return ok
}

// TestCheckAndRetry_PerHostModeStillRecovers is the regression test for
// the per-host topology (kdex-tech/host-manager#156).
//
// checkAndRetryFailedBuild used to build the kpack Image name from
// cfg.FunctionName. Under a per-host CronJob that env var is empty, so
// the name came out as "<host>-", the Get 404'd, IsNotFound returned nil,
// and the caller read nil as "no work". The result was that kpack
// preemption auto-recovery silently stopped running for every function on
// the host -- no error, no log line, no failed Job.
//
// The identity must come off the passed CR instead. Both modes observe
// the same function here and must reach the same outcome; only the env
// differs.
func TestCheckAndRetry_PerHostModeStillRecovers(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  *EnvConfig
	}{
		{
			// Legacy per-function CronJob. This is the control: it
			// passed before the fix and must keep passing.
			name: "legacy FUNCTION_NAME set",
			cfg:  &EnvConfig{FunctionNamespace: testNamespace, FunctionHost: testHost, FunctionName: testFunc},
		},
		{
			// Per-host CronJob: no FUNCTION_NAME. Failed before the fix.
			name: "per-host FUNCTION_NAME empty",
			cfg:  &EnvConfig{FunctionNamespace: testNamespace, FunctionHost: testHost},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := newFakeClient(preemptedBuildFixture()...)

			kfObj := perHostFunction()
			if err := checkAndRetryFailedBuild(context.Background(), client, kfObj, tc.cfg); err != nil {
				t.Fatalf("checkAndRetryFailedBuild: %v", err)
			}

			if !rebuildTriggered(t, client) {
				t.Fatalf("no %s annotation: auto-recovery did not fire for a preempted build",
					buildNeededAnnotation)
			}

			// The retry budget must be debited against the observed
			// function, not against whatever cfg happened to name.
			updated, err := client.Resource(kdexFunctionGVR).Namespace(testNamespace).
				Get(context.Background(), testFunc, metav1.GetOptions{})
			if err != nil {
				t.Fatalf("get KDexFunction: %v", err)
			}
			retries, lastRetryAt, exhausted := getRetryState(updated)
			if retries != 1 {
				t.Errorf("retries = %d, want 1 (budget not debited against the observed function)", retries)
			}
			if lastRetryAt.IsZero() {
				t.Error("lastRetryAt not recorded")
			}
			if exhausted {
				t.Error("exhausted should be false on the first retry")
			}
		})
	}
}

// TestCheckAndRetry_PerHostResetsBudgetOnSuccess covers the other branch
// that used to key off cfg.FunctionName: a successful build wipes the
// retry budget, and that patch must target the observed function too.
func TestCheckAndRetry_PerHostResetsBudgetOnSuccess(t *testing.T) {
	// Function carries a spent budget; its Image has since built cleanly.
	kf := newKDexFunction(2, time.Now().UTC().Add(-time.Hour), false)
	client := newFakeClient(
		newKPackImage("registry.example/img@sha256:abc", testBuild),
		kf,
	)

	cfg := &EnvConfig{FunctionNamespace: testNamespace, FunctionHost: testHost} // per-host: no name
	if err := checkAndRetryFailedBuild(context.Background(), client, kf, cfg); err != nil {
		t.Fatalf("checkAndRetryFailedBuild: %v", err)
	}

	updated, err := client.Resource(kdexFunctionGVR).Namespace(testNamespace).
		Get(context.Background(), testFunc, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get KDexFunction: %v", err)
	}
	retries, lastRetryAt, exhausted := getRetryState(updated)
	if retries != 0 || !lastRetryAt.IsZero() || exhausted {
		t.Fatalf("budget not reset: got (%d, %v, %v), want (0, zero, false)", retries, lastRetryAt, exhausted)
	}
}
