// kpack Build auto-recovery for preempted Build pods.
//
// kpack does not auto-retry preempted Builds — verified against
// buildpacks-community/kpack pkg/reconciler/image/build_required.go at
// v0.17.1: a new kpack.io/Build is only created on source/config/builder/
// lifecycle/trigger changes, never on the previous Build's failure
// status. A Build pod evicted by spot preemption, node shutdown, or
// voluntary disruption therefore leaves the parent kpack.io/Image
// permanently Ready=False.
//
// The observer CronJob (which already runs per-KDexFunction every
// 5 min) is the natural watchdog. checkAndRetryFailedBuild inspects
// the latest Build, classifies its failure mode, and on preemption
// signals issues a bounded retry via the image.kpack.io/
// additionalBuildNeeded annotation. Genuine failures (exit codes,
// OOM-of-our-own-code, kpack-side schema errors) are NOT retried;
// the retry budget exists to absorb infra-level churn, not to mask
// real bugs.

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
)

var (
	kpackImageGVR = schema.GroupVersionResource{Group: "kpack.io", Version: "v1alpha2", Resource: "images"}
	kpackBuildGVR = schema.GroupVersionResource{Group: "kpack.io", Version: "v1alpha2", Resource: "builds"}
	podGVR        = schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"}
	nodeGVR       = schema.GroupVersionResource{Group: "", Version: "v1", Resource: "nodes"}
)

const (
	// defaultMaxBuildRetries caps observer-driven retries when the
	// FaaSAdaptor doesn't override via Observer.MaxBuildRetries.
	defaultMaxBuildRetries = 3

	// defaultRetryCooldown is the floor between consecutive retries
	// when the FaaSAdaptor doesn't override via Observer.RetryCooldown.
	// At the default observer schedule "*/5 * * * *" this is 4 ticks,
	// enough to let kpack actually create + start the new Build before
	// the next tick can decide another retry is needed.
	defaultRetryCooldown = 20 * time.Minute

	// buildNeededAnnotation is kpack's user-facing trigger key. Changing
	// the annotation's value on a kpack.io/Image causes kpack to create
	// a new Build with reason=TRIGGER.
	buildNeededAnnotation = "image.kpack.io/additionalBuildNeeded"

	// Retry state lives in KDexFunction.status.attributes (a free-form
	// map) so we don't need a CRD bump for the bookkeeping. Sibling
	// CRD-level policy knobs are Observer.MaxBuildRetries +
	// Observer.RetryCooldown (kdex-crds v0.14.212).
	attrRetries     = "build.preemption.retries"
	attrLastRetryAt = "build.preemption.lastRetryAt"
	attrExhausted   = "build.preemption.exhausted"
)

// checkAndRetryFailedBuild is the entry point called from runObserve
// when the Knative Service isn't Ready (or doesn't exist yet). It is
// a no-op when:
//   - the function has no kpack Image (executable-mode or pre-build)
//   - the latest Build is still running or already succeeded
//   - the latest Build failed but the failure isn't a preemption
//   - the retry budget is exhausted or the cooldown is still active
//
// Returns nil for all "no work" cases so the observer's status-patch
// flow keeps running normally.
func checkAndRetryFailedBuild(
	ctx context.Context,
	client dynamic.Interface,
	kfObj *unstructured.Unstructured,
	cfg *EnvConfig,
) error {
	if cfg.FunctionHost == "" {
		// Without FUNCTION_HOST we can't compute the kpack Image name.
		// Older host-manager Jobs don't set it; skip rather than guess.
		return nil
	}
	imageName := cfg.FunctionHost + "-" + cfg.FunctionName

	imgClient := client.Resource(kpackImageGVR).Namespace(cfg.FunctionNamespace)
	img, err := imgClient.Get(ctx, imageName, metav1.GetOptions{})
	if err != nil {
		if kerrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("get kpack Image %s: %w", imageName, err)
	}

	// If the Image's last Build succeeded, reset the retry counter
	// (genuine progress wipes the budget for the next preemption).
	latestImage, _, _ := unstructured.NestedString(img.Object, "status", "latestImage")
	if latestImage != "" {
		return resetRetryStateIfSet(ctx, client, kfObj, cfg)
	}

	latestBuildRef, _, _ := unstructured.NestedString(img.Object, "status", "latestBuildRef")
	if latestBuildRef == "" {
		return nil
	}

	buildClient := client.Resource(kpackBuildGVR).Namespace(cfg.FunctionNamespace)
	build, err := buildClient.Get(ctx, latestBuildRef, metav1.GetOptions{})
	if err != nil {
		if kerrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("get kpack Build %s: %w", latestBuildRef, err)
	}

	if !buildFailed(build) {
		return nil
	}

	preempted, err := isBuildPodPreempted(ctx, client, build, cfg.FunctionNamespace)
	if err != nil {
		// Conservative: log-and-skip instead of looping on a transient
		// API hiccup. The next observe tick re-evaluates.
		fmt.Fprintf(os.Stderr, "preemption check failed (ignoring): %v\n", err)
		return nil
	}
	if !preempted {
		// Genuine build failure. Leave the budget alone.
		return nil
	}

	retries, lastRetryAt, exhausted := getRetryState(kfObj)
	if exhausted {
		return nil
	}

	maxRetries := envInt("MAX_BUILD_RETRIES", defaultMaxBuildRetries)
	cooldown := envDuration("RETRY_COOLDOWN", defaultRetryCooldown)

	if retries >= maxRetries {
		fmt.Printf("Build preemption budget exhausted (%d retries); marking exhausted\n", retries)
		return setRetryState(ctx, client, cfg, retries, lastRetryAt, true)
	}
	if !lastRetryAt.IsZero() && time.Since(lastRetryAt) < cooldown {
		return nil
	}

	now := time.Now().UTC()
	if err := triggerImageRebuild(ctx, imgClient, imageName, now); err != nil {
		return fmt.Errorf("trigger image rebuild: %w", err)
	}
	fmt.Printf("Preempted Build %s; triggered retry %d/%d\n", build.GetName(), retries+1, maxRetries)
	return setRetryState(ctx, client, cfg, retries+1, now, false)
}

// buildFailed returns true when the kpack Build's Succeeded condition
// is explicitly False. In-flight (Unknown) and successful (True) Builds
// return false.
func buildFailed(build *unstructured.Unstructured) bool {
	conds, _, _ := unstructured.NestedSlice(build.Object, "status", "conditions")
	for _, c := range conds {
		cond, ok := c.(map[string]any)
		if !ok {
			continue
		}
		if cond["type"] == "Succeeded" {
			return cond["status"] == "False"
		}
	}
	return false
}

// isBuildPodPreempted classifies the latest Build's referenced Pod
// against the preemption heuristic documented in
// docs/superpowers/specs/2026-05-20-kpack-build-auto-recovery-design.md.
// Conservative bias: when in doubt, return false (don't retry). A
// missing Pod returns false (kpack might have GC'd it; treating that
// as preempted would create a runaway loop on stale Builds).
func isBuildPodPreempted(
	ctx context.Context,
	client dynamic.Interface,
	build *unstructured.Unstructured,
	namespace string,
) (bool, error) {
	podName, _, _ := unstructured.NestedString(build.Object, "status", "podName")
	if podName == "" {
		return false, nil
	}
	podClient := client.Resource(podGVR).Namespace(namespace)
	pod, err := podClient.Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		if kerrors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}

	// Signal 1: Pod has a DeletionTimestamp with no preceding container
	// exit code. Canonical "evicted by external actor" pattern.
	if pod.GetDeletionTimestamp() != nil && !anyInitContainerHasExitCode(pod) {
		return true, nil
	}

	// Signal 2: Pod's status.reason set by node-shutdown / preemption.
	if reason, _, _ := unstructured.NestedString(pod.Object, "status", "reason"); reason != "" {
		switch reason {
		case "NodeShutdown", "Preempted", "Terminated":
			return true, nil
		}
	}

	// Signal 3: Pod's node is no longer Ready (Node gone, or its Ready
	// condition is False and pre-dates the pod's last container state).
	nodeName, _, _ := unstructured.NestedString(pod.Object, "spec", "nodeName")
	if nodeName != "" {
		nodeNotReady, err := isNodeNotReady(ctx, client, nodeName)
		if err != nil {
			return false, fmt.Errorf("node %s status: %w", nodeName, err)
		}
		if nodeNotReady {
			return true, nil
		}
	}

	// Signal 4: an init container terminated with reason=Killed (NOT
	// OOMKilled — that's our own sizing problem, not preemption). Pod
	// remains in Pending phase with the failed init container terminated.
	if anyInitContainerKilledExternally(pod) {
		return true, nil
	}

	return false, nil
}

// anyInitContainerHasExitCode reports whether any initContainerStatus
// records a terminated state with an explicit exitCode. Used by the
// DeletionTimestamp signal to distinguish "container ran to completion
// THEN pod was deleted" (genuine, exit code recorded) from "pod deleted
// while a container was running" (preemption, no exit code).
func anyInitContainerHasExitCode(pod *unstructured.Unstructured) bool {
	stats, _, _ := unstructured.NestedSlice(pod.Object, "status", "initContainerStatuses")
	for _, s := range stats {
		entry, ok := s.(map[string]any)
		if !ok {
			continue
		}
		state, _, _ := unstructured.NestedMap(entry, "state")
		terminated, _, _ := unstructured.NestedMap(state, "terminated")
		if terminated == nil {
			continue
		}
		if _, found := terminated["exitCode"]; found {
			return true
		}
	}
	return false
}

// anyInitContainerKilledExternally returns true when the highest-numbered
// terminated init container shows reason=Killed (NOT OOMKilled and NOT
// Error). kpack's init container chain is prepare → analyze → detect →
// restore → build → export; an external Kill on any of them is the
// preemption signal here.
func anyInitContainerKilledExternally(pod *unstructured.Unstructured) bool {
	stats, _, _ := unstructured.NestedSlice(pod.Object, "status", "initContainerStatuses")
	for _, s := range stats {
		entry, ok := s.(map[string]any)
		if !ok {
			continue
		}
		state, _, _ := unstructured.NestedMap(entry, "state")
		terminated, _, _ := unstructured.NestedMap(state, "terminated")
		if terminated == nil {
			continue
		}
		reason, _ := terminated["reason"].(string)
		if reason == "Killed" {
			return true
		}
	}
	return false
}

// isNodeNotReady fetches a Node and returns true when the Node either
// no longer exists OR its Ready condition is False. Used by the Pod's-
// node-disappeared preemption signal.
func isNodeNotReady(ctx context.Context, client dynamic.Interface, name string) (bool, error) {
	node, err := client.Resource(nodeGVR).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		if kerrors.IsNotFound(err) {
			return true, nil
		}
		return false, err
	}
	conds, _, _ := unstructured.NestedSlice(node.Object, "status", "conditions")
	for _, c := range conds {
		cond, ok := c.(map[string]any)
		if !ok {
			continue
		}
		if cond["type"] == "Ready" {
			return cond["status"] != "True", nil
		}
	}
	// No Ready condition recorded — be conservative and treat as Ready.
	return false, nil
}

// triggerImageRebuild sets the additionalBuildNeeded annotation on the
// kpack Image. kpack's reconciler detects the annotation value change
// and creates a new Build with reason=TRIGGER.
func triggerImageRebuild(
	ctx context.Context,
	imgClient dynamic.ResourceInterface,
	imageName string,
	at time.Time,
) error {
	patch := map[string]any{
		"metadata": map[string]any{
			"annotations": map[string]any{
				buildNeededAnnotation: at.Format(time.RFC3339),
			},
		},
	}
	body, err := encodePatch(patch)
	if err != nil {
		return err
	}
	_, err = imgClient.Patch(ctx, imageName, types.MergePatchType, body, metav1.PatchOptions{
		FieldManager: "kdex-knative-observer",
	})
	return err
}

// getRetryState reads the bookkeeping attributes from the KDexFunction.
// Missing attributes default to zero values. lastRetryAt errors during
// parse are treated as zero (so a corrupted attribute can't permanently
// block retries).
func getRetryState(kfObj *unstructured.Unstructured) (retries int, lastRetryAt time.Time, exhausted bool) {
	attrs, _, _ := unstructured.NestedStringMap(kfObj.Object, "status", "attributes")
	if attrs == nil {
		return 0, time.Time{}, false
	}
	if v := attrs[attrRetries]; v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			retries = n
		}
	}
	if v := attrs[attrLastRetryAt]; v != "" {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			lastRetryAt = t
		}
	}
	if attrs[attrExhausted] == "true" {
		exhausted = true
	}
	return retries, lastRetryAt, exhausted
}

// setRetryState patches the KDexFunction's status.attributes with the
// three bookkeeping keys. Uses merge-patch so other attribute writers
// (host-manager itself emits attributes too) don't get clobbered.
func setRetryState(
	ctx context.Context,
	client dynamic.Interface,
	cfg *EnvConfig,
	retries int,
	lastRetryAt time.Time,
	exhausted bool,
) error {
	attrs := map[string]any{
		attrRetries: strconv.Itoa(retries),
	}
	if !lastRetryAt.IsZero() {
		attrs[attrLastRetryAt] = lastRetryAt.Format(time.RFC3339)
	}
	if exhausted {
		attrs[attrExhausted] = "true"
	}
	patch := map[string]any{
		"status": map[string]any{
			"attributes": attrs,
		},
	}
	body, err := encodePatch(patch)
	if err != nil {
		return err
	}
	kfClient := client.Resource(kdexFunctionGVR).Namespace(cfg.FunctionNamespace)
	_, err = kfClient.Patch(ctx, cfg.FunctionName, types.MergePatchType, body, metav1.PatchOptions{
		FieldManager: "kdex-knative-observer",
	}, "status")
	return err
}

// resetRetryStateIfSet wipes the bookkeeping when the kpack Image has
// produced a successful build. Idempotent — early-returns when the
// state was already empty.
func resetRetryStateIfSet(
	ctx context.Context,
	client dynamic.Interface,
	kfObj *unstructured.Unstructured,
	cfg *EnvConfig,
) error {
	retries, lastRetryAt, exhausted := getRetryState(kfObj)
	if retries == 0 && lastRetryAt.IsZero() && !exhausted {
		return nil
	}
	// nil values via JSON merge-patch remove the keys.
	patch := map[string]any{
		"status": map[string]any{
			"attributes": map[string]any{
				attrRetries:     nil,
				attrLastRetryAt: nil,
				attrExhausted:   nil,
			},
		},
	}
	body, err := encodePatch(patch)
	if err != nil {
		return err
	}
	kfClient := client.Resource(kdexFunctionGVR).Namespace(cfg.FunctionNamespace)
	_, err = kfClient.Patch(ctx, cfg.FunctionName, types.MergePatchType, body, metav1.PatchOptions{
		FieldManager: "kdex-knative-observer",
	}, "status")
	return err
}

// envInt parses an env var as an integer; returns def on missing / empty
// / parse error. The Observer policy fields (MaxBuildRetries) flow
// through host-manager as env vars set on the observer CronJob's pod.
func envInt(key string, def int) int {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

// envDuration parses an env var as a Go duration string ("20m", "1h",
// etc.); returns def on missing / empty / parse error.
func envDuration(key string, def time.Duration) time.Duration {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return def
	}
	return d
}

// encodePatch marshals a patch object via JSON merge-patch.
func encodePatch(p map[string]any) ([]byte, error) {
	body, err := json.Marshal(p)
	if err != nil {
		return nil, fmt.Errorf("encode patch: %w", err)
	}
	return body, nil
}
