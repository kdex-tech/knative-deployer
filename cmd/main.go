package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
)

var (
	knativeServiceGVR = schema.GroupVersionResource{
		Group:    "serving.knative.dev",
		Version:  "v1",
		Resource: "services",
	}

	kdexFunctionGVR = schema.GroupVersionResource{
		Group:    "kdex.dev",
		Version:  "v1alpha1",
		Resource: "kdexfunctions",
	}
)

type EnvConfig struct {
	Audience                             string
	ForwardedEnvVars                     string
	FunctionBasePath                     string
	FunctionGeneration                   string
	FunctionHost                         string
	FunctionImage                        string
	FunctionInternal                     string
	FunctionName                         string
	FunctionNamespace                    string
	FunctionNodeSelector                 string
	FunctionServiceAccountName           string
	FunctionTolerations                  string
	FunctionUserEnv                      string
	FunctionVolumeMounts                 string
	FunctionVolumes                      string
	Issuer                               string
	JWKSURL                              string
	ScalingActivationScale               string
	ScalingInitialScale                  string
	ScalingMaxScale                      string
	ScalingMetric                        string
	ScalingMinScale                      string
	ScalingPanicThresholdPercentage      string
	ScalingPanicWindowPercentage         string
	ScalingScaleDownDelay                string
	ScalingScaleToZeroPodRetentionPeriod string
	ScalingStableWindow                  string
	ScalingTarget                        string
	ScalingTargetUtilizationPercentage   string
}

func LoadEnv() (*EnvConfig, error) {
	cfg := &EnvConfig{
		Audience:                             os.Getenv("AUDIENCE"),
		ForwardedEnvVars:                     os.Getenv("FORWARDED_ENV_VARS"),
		FunctionBasePath:                     os.Getenv("FUNCTION_BASEPATH"),
		FunctionGeneration:                   os.Getenv("FUNCTION_GENERATION"),
		FunctionHost:                         os.Getenv("FUNCTION_HOST"),
		FunctionImage:                        os.Getenv("FUNCTION_IMAGE"),
		FunctionInternal:                     os.Getenv("FUNCTION_INTERNAL"),
		FunctionName:                         os.Getenv("FUNCTION_NAME"),
		FunctionNamespace:                    os.Getenv("FUNCTION_NAMESPACE"),
		FunctionNodeSelector:                 os.Getenv("FUNCTION_NODE_SELECTOR"),
		FunctionServiceAccountName:           os.Getenv("FUNCTION_SERVICE_ACCOUNT_NAME"),
		FunctionTolerations:                  os.Getenv("FUNCTION_TOLERATIONS"),
		FunctionUserEnv:                      os.Getenv("FUNCTION_USER_ENV"),
		FunctionVolumeMounts:                 os.Getenv("FUNCTION_VOLUME_MOUNTS"),
		FunctionVolumes:                      os.Getenv("FUNCTION_VOLUMES"),
		Issuer:                               os.Getenv("ISSUER"),
		JWKSURL:                              os.Getenv("JWKS_URL"),
		ScalingActivationScale:               os.Getenv("SCALING_ACTIVATION_SCALE"),
		ScalingInitialScale:                  os.Getenv("SCALING_INITIAL_SCALE"),
		ScalingMaxScale:                      os.Getenv("SCALING_MAX_SCALE"),
		ScalingMetric:                        os.Getenv("SCALING_METRIC"),
		ScalingMinScale:                      os.Getenv("SCALING_MIN_SCALE"),
		ScalingPanicThresholdPercentage:      os.Getenv("SCALING_PANIC_THRESHOLD_PERCENTAGE"),
		ScalingPanicWindowPercentage:         os.Getenv("SCALING_PANIC_WINDOW_PERCENTAGE"),
		ScalingScaleDownDelay:                os.Getenv("SCALING_SCALE_DOWN_DELAY"),
		ScalingScaleToZeroPodRetentionPeriod: os.Getenv("SCALING_SCALE_TO_ZERO_POD_RETENTION_PERIOD"),
		ScalingStableWindow:                  os.Getenv("SCALING_STABLE_WINDOW"),
		ScalingTarget:                        os.Getenv("SCALING_TARGET"),
		ScalingTargetUtilizationPercentage:   os.Getenv("SCALING_TARGET_UTILIZATION_PERCENTAGE"),
	}

	isObserve := len(os.Args) > 1 && os.Args[1] == "observe"

	// The per-host observer CronJob covers many functions, so it has no single
	// FUNCTION_NAME -- it identifies its set with FUNCTION_HOST instead. Every
	// other mode (and the legacy per-function observer) still requires a name.
	// See kdex-tech/host-manager#156.
	if cfg.FunctionName == "" {
		if !isObserve {
			return nil, fmt.Errorf("FUNCTION_NAME is required")
		}
		if cfg.FunctionHost == "" {
			return nil, fmt.Errorf("observe requires FUNCTION_NAME (single function) or FUNCTION_HOST (per-host)")
		}
	}
	if cfg.FunctionNamespace == "" {
		return nil, fmt.Errorf("FUNCTION_NAMESPACE is required")
	}
	// Image might not be required for observe?
	// But let's keep it strict if deployer job provides it.
	// For observer cronjob, deployer might pass it too.
	// Let's make it optional for observe if needed, but for now strict.
	if cfg.FunctionImage == "" && len(os.Args) > 1 && os.Args[1] == "deploy" {
		return nil, fmt.Errorf("FUNCTION_IMAGE is required for deploy")
	}

	return cfg, nil
}

// buildContainerEnv assembles the env block that will land on the Knative
// Service container. Two layers:
//
//  1. ForwardedEnvVars — a comma-separated list of names whose values are
//     read out of the deployer-pod's own env (controller-populated common
//     vars: AUDIENCE, FUNCTION_*, ISSUER, etc.). These flow through as
//     plain {name, value} entries.
//  2. FunctionUserEnv — a JSON-marshaled []corev1.EnvVar from the
//     KDexFunction CR's spec.env, splat into the env block unchanged so
//     valueFrom.secretKeyRef / configMapKeyRef / etc. survive. Critical:
//     do NOT route user env through ForwardedEnvVars + os.Getenv — the
//     kubelet would have already dereferenced the secretKeyRef into a
//     plain string at deployer-pod start, leaking the secret value into
//     .spec.containers[0].env[].value on the resulting Revision YAML
//     (readable to anyone with `get revision`, much broader RBAC than
//     `get secret`). See kdex-tech/kdex-host-manager#XX.
//
// Extracted from runDeploy for unit testability; runDeploy passes
// os.Getenv for the forwarded-var lookup.
func buildContainerEnv(cfg *EnvConfig, getenv func(string) string) ([]map[string]any, error) {
	containerEnv := []map[string]any{}

	if cfg.ForwardedEnvVars != "" {
		for v := range strings.SplitSeq(cfg.ForwardedEnvVars, ",") {
			v = strings.TrimSpace(v)
			if v == "" {
				continue
			}
			containerEnv = append(containerEnv, map[string]any{
				"name":  v,
				"value": getenv(v),
			})
		}
	}

	if cfg.FunctionUserEnv != "" {
		var userEnv []map[string]any
		if err := json.Unmarshal([]byte(cfg.FunctionUserEnv), &userEnv); err != nil {
			return nil, fmt.Errorf("unmarshal FUNCTION_USER_ENV: %w", err)
		}
		containerEnv = append(containerEnv, userEnv...)
	}

	return containerEnv, nil
}

// scalingAnnotations builds the autoscaling.knative.dev/* annotation map from
// the SCALING_* env vars host-manager forwards. Empty cfg fields are skipped,
// so a returned len(0) map signals "no scaling block was set on the CR" and
// callers should omit the annotations key entirely.
func scalingAnnotations(cfg *EnvConfig) map[string]string {
	annotations := map[string]string{}
	if cfg.ScalingActivationScale != "" {
		annotations["autoscaling.knative.dev/activation-scale"] = cfg.ScalingActivationScale
	}
	if cfg.ScalingInitialScale != "" {
		annotations["autoscaling.knative.dev/initial-scale"] = cfg.ScalingInitialScale
	}
	if cfg.ScalingMaxScale != "" {
		annotations["autoscaling.knative.dev/max-scale"] = cfg.ScalingMaxScale
	}
	if cfg.ScalingMetric != "" {
		annotations["autoscaling.knative.dev/metric"] = cfg.ScalingMetric
	}
	if cfg.ScalingMinScale != "" {
		annotations["autoscaling.knative.dev/min-scale"] = cfg.ScalingMinScale
	}
	if cfg.ScalingPanicThresholdPercentage != "" {
		annotations["autoscaling.knative.dev/panic-threshold-percentage"] = cfg.ScalingPanicThresholdPercentage
	}
	if cfg.ScalingPanicWindowPercentage != "" {
		annotations["autoscaling.knative.dev/panic-window-percentage"] = cfg.ScalingPanicWindowPercentage
	}
	if cfg.ScalingScaleDownDelay != "" {
		annotations["autoscaling.knative.dev/scale-down-delay"] = cfg.ScalingScaleDownDelay
	}
	if cfg.ScalingScaleToZeroPodRetentionPeriod != "" {
		annotations["autoscaling.knative.dev/scale-to-zero-pod-retention-period"] = cfg.ScalingScaleToZeroPodRetentionPeriod
	}
	if cfg.ScalingTarget != "" {
		annotations["autoscaling.knative.dev/target"] = cfg.ScalingTarget
	}
	if cfg.ScalingTargetUtilizationPercentage != "" {
		annotations["autoscaling.knative.dev/target-utilization-percentage"] = cfg.ScalingTargetUtilizationPercentage
	}
	if cfg.ScalingStableWindow != "" {
		annotations["autoscaling.knative.dev/window"] = cfg.ScalingStableWindow
	}
	return annotations
}

// buildPodSpec assembles the Knative Revision PodSpec (spec.template.spec).
// Pure function — no I/O — for unit testability; runDeploy passes the
// already-built container env block.
//
// FUNCTION_TOLERATIONS / FUNCTION_NODE_SELECTOR steer the runtime pod onto a
// tainted node pool (kdex-tech/knative-deployer#3). FUNCTION_VOLUMES /
// FUNCTION_VOLUME_MOUNTS project file-based config — ConfigMap/Secret files —
// into the runtime pod/container (kdex-tech/kdex-crds#10). All REQUIRE the
// cluster to enable the matching Knative kubernetes.podspec-* feature flags,
// otherwise the Knative webhook rejects the applied spec.
func buildPodSpec(cfg *EnvConfig, containerEnv []map[string]any) (map[string]any, error) {
	container := map[string]any{
		"image": cfg.FunctionImage,
		"env":   containerEnv,
	}

	if cfg.FunctionVolumeMounts != "" {
		var volumeMounts []any
		if err := json.Unmarshal([]byte(cfg.FunctionVolumeMounts), &volumeMounts); err != nil {
			return nil, fmt.Errorf("unmarshal FUNCTION_VOLUME_MOUNTS: %w", err)
		}
		if len(volumeMounts) > 0 {
			container["volumeMounts"] = volumeMounts
		}
	}

	podSpec := map[string]any{
		"containers": []map[string]any{container},
	}

	// Honor the optional FUNCTION_SERVICE_ACCOUNT_NAME so the runtime pod
	// can run as a non-default KSA (e.g. for Workload Identity binding to a
	// GCP service account). When empty, Knative falls back to the
	// namespace's default ServiceAccount, preserving prior behavior.
	if cfg.FunctionServiceAccountName != "" {
		podSpec["serviceAccountName"] = cfg.FunctionServiceAccountName
	}

	if cfg.FunctionTolerations != "" {
		var tolerations []any
		if err := json.Unmarshal([]byte(cfg.FunctionTolerations), &tolerations); err != nil {
			return nil, fmt.Errorf("unmarshal FUNCTION_TOLERATIONS: %w", err)
		}
		if len(tolerations) > 0 {
			podSpec["tolerations"] = tolerations
		}
	}
	if cfg.FunctionNodeSelector != "" {
		var nodeSelector map[string]any
		if err := json.Unmarshal([]byte(cfg.FunctionNodeSelector), &nodeSelector); err != nil {
			return nil, fmt.Errorf("unmarshal FUNCTION_NODE_SELECTOR: %w", err)
		}
		if len(nodeSelector) > 0 {
			podSpec["nodeSelector"] = nodeSelector
		}
	}
	if cfg.FunctionVolumes != "" {
		var volumes []any
		if err := json.Unmarshal([]byte(cfg.FunctionVolumes), &volumes); err != nil {
			return nil, fmt.Errorf("unmarshal FUNCTION_VOLUMES: %w", err)
		}
		if len(volumes) > 0 {
			podSpec["volumes"] = volumes
		}
	}

	return podSpec, nil
}

// buildKnativeService constructs the serving.knative.dev/v1 Service object
// that runDeploy will SSA-apply. Pure function — no I/O — for unit testability.
//
// Annotation placement: autoscaling.knative.dev/* annotations live on
// spec.template.metadata.annotations (the Revision template). Knative's
// validation webhook rejects them anywhere else:
//
//	autoscaling annotations must be put under "spec.template.metadata.annotations" to work
//
// The Service-level metadata.annotations map is ALWAYS emitted as a
// (possibly empty) map. This preserves the SSA structural-ownership
// shape that pre-v0.1.23 maintained via service.SetAnnotations(...),
// without which Knative's webhook rejects re-applies because it interprets
// the missing map as the deployer trying to clear the
// serving.knative.dev/{creator,lastModifier} audit annotations it had
// previously stamped within deployer-owned map structure.
//
// Extracted from runDeploy for kdex-tech/knative-deployer#4; the
// always-emit-annotations safety net is the follow-on regression fix.
func buildKnativeService(cfg *EnvConfig, podSpec map[string]any) *unstructured.Unstructured {
	templateMeta := map[string]any{
		"labels": map[string]any{
			"kdex.dev/function":   cfg.FunctionName,
			"kdex.dev/generation": cfg.FunctionGeneration,
		},
	}
	if annotations := scalingAnnotations(cfg); len(annotations) > 0 {
		templateMeta["annotations"] = annotations
	}

	serviceLabels := map[string]any{
		"kdex.dev/function":   cfg.FunctionName,
		"kdex.dev/generation": cfg.FunctionGeneration,
	}
	// FUNCTION_INTERNAL marks the function cluster-only (kdex-tech/kdex-crds#6):
	// the Knative networking layer refuses external traffic to a Service
	// labeled cluster-local, so even if a host route slips through the
	// function is unreachable from outside the cluster. In-cluster callers
	// still reach it at <name>.<namespace>.svc.cluster.local.
	if cfg.FunctionInternal == "true" {
		serviceLabels["networking.knative.dev/visibility"] = "cluster-local"
	}

	return &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "serving.knative.dev/v1",
			"kind":       "Service",
			"metadata": map[string]any{
				"name":      cfg.FunctionName,
				"namespace": cfg.FunctionNamespace,
				"labels":    serviceLabels,
				// Empty placeholder to preserve SSA structural ownership of
				// metadata.annotations. See doc comment above.
				"annotations": map[string]string{},
			},
			"spec": map[string]any{
				"template": map[string]any{
					"metadata": templateMeta,
					"spec":     podSpec,
				},
			},
		},
	}
}

func main() {
	cmd := "deploy"
	if len(os.Args) > 1 {
		cmd = os.Args[1]
	}

	var err error
	switch cmd {
	case "deploy":
		err = runDeploy()
	case "observe":
		err = runObserve()
	default:
		err = fmt.Errorf("unknown command: %s", cmd)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func getDynamicClient() (dynamic.Interface, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to get in-cluster config: %w", err)
	}

	client, err := dynamic.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create dynamic client: %w", err)
	}
	return client, nil
}

func runDeploy() error {
	cfg, err := LoadEnv()
	if err != nil {
		return err
	}

	client, err := getDynamicClient()
	if err != nil {
		return err
	}

	containerEnv, err := buildContainerEnv(cfg, os.Getenv)
	if err != nil {
		return err
	}

	podSpec, err := buildPodSpec(cfg, containerEnv)
	if err != nil {
		return err
	}

	service := buildKnativeService(cfg, podSpec)

	resourceClient := client.Resource(knativeServiceGVR).Namespace(cfg.FunctionNamespace)

	// We'll use Server-Side Apply
	data, err := json.Marshal(service)
	if err != nil {
		return fmt.Errorf("failed to marshal service: %w", err)
	}

	// Force ownership to allow overwriting
	force := true
	_, err = resourceClient.Patch(context.Background(), cfg.FunctionName, types.ApplyPatchType, data, metav1.PatchOptions{
		FieldManager: "kdex-knative-deployer",
		Force:        &force,
	})
	if err != nil {
		return fmt.Errorf("failed to apply knative service: %w", err)
	}

	fmt.Printf("Knative Service %s/%s applied successfully\n", cfg.FunctionNamespace, cfg.FunctionName)

	// Wait for Readiness
	fmt.Println("Waiting for service to be Ready...")
	url, err := waitForReady(context.Background(), resourceClient, cfg.FunctionName)
	if err != nil {
		return fmt.Errorf("failed to wait for service readiness: %w", err)
	}

	fmt.Printf("Service is Ready. URL: %s\n", url)

	// Write termination message
	if err := writeTerminationMessage(url); err != nil {
		return fmt.Errorf("failed to write termination message: %w", err)
	}

	return nil
}

// ObservedByLabel marks a KDexFunction as belonging to a host's observer set.
// host-manager stamps it with the host name when it provisions the per-host
// observer CronJob; this observer lists by it to find its work.
//
// Making the set explicit (rather than "every function in the namespace") keeps
// the previous semantics: only adaptor-deployed functions were ever observed --
// service-backed ones never had an observer CronJob.
// See kdex-tech/host-manager#156.
const ObservedByLabel = "kdex.dev/observed-by"

// observedFunctions resolves the KDexFunctions this observer pass covers.
//
// Two modes, so a new image can be rolled out under old per-function CronJobs
// before host-manager switches to the per-host topology:
//
//   - legacy: FUNCTION_NAME set -> that single function.
//   - per-host: FUNCTION_HOST set -> every function labelled for that host.
func observedFunctions(
	ctx context.Context,
	client dynamic.Interface,
	cfg *EnvConfig,
) ([]unstructured.Unstructured, error) {
	kfClient := client.Resource(kdexFunctionGVR).Namespace(cfg.FunctionNamespace)

	if cfg.FunctionName != "" {
		kfObj, err := kfClient.Get(ctx, cfg.FunctionName, metav1.GetOptions{})
		if err != nil {
			return nil, fmt.Errorf("failed to get kdex function: %w", err)
		}
		return []unstructured.Unstructured{*kfObj}, nil
	}

	selector := fmt.Sprintf("%s=%s", ObservedByLabel, cfg.FunctionHost)
	list, err := kfClient.List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return nil, fmt.Errorf("failed to list kdex functions (%s): %w", selector, err)
	}
	return list.Items, nil
}

// runObserve observes every function in this pass's set. One function's failure
// must not skip the rest -- a single un-gettable Knative Service used to abort
// the whole CronJob, which in per-host mode would strand every other function
// on the host.
func runObserve() error {
	cfg, err := LoadEnv()
	if err != nil {
		return err
	}

	client, err := getDynamicClient()
	if err != nil {
		return err
	}

	ctx := context.Background()

	functions, err := observedFunctions(ctx, client, cfg)
	if err != nil {
		return err
	}
	if len(functions) == 0 {
		fmt.Printf("No functions to observe (host=%s, namespace=%s)\n", cfg.FunctionHost, cfg.FunctionNamespace)
		return nil
	}

	return observePass(ctx, client, functions, cfg, time.Now())
}

// observePass observes a resolved set of functions and decides the pass's exit
// status. Split out of runObserve, which reads env and builds a client itself,
// so the loop's no-fail-fast behaviour and the #9 exit-code contract are
// testable against a fake client rather than only reasoned about.
func observePass(
	ctx context.Context,
	client dynamic.Interface,
	functions []unstructured.Unstructured,
	cfg *EnvConfig,
	now time.Time,
) error {
	outcome := observeOutcome{Observed: len(functions)}

	for i := range functions {
		kfObj := &functions[i]
		ns, name := kfObj.GetNamespace(), kfObj.GetName()

		obsErr := observeFunction(ctx, client, kfObj, cfg)
		if obsErr == nil {
			// Recovered (or never failed) -- drop any stale failure so the
			// function doesn't look broken forever. No-op when nothing is set.
			if clearErr := clearObserveFailure(ctx, client, kfObj); clearErr != nil && !errors.IsNotFound(clearErr) {
				// Not fatal: the pass observed this function fine. But a stale
				// error left behind is exactly the "looks broken forever"
				// failure mode, so say so loudly.
				fmt.Fprintf(os.Stderr, "observe %s/%s: succeeded but failed to clear stale failure: %v\n",
					ns, name, clearErr)
			}
			continue
		}

		outcome.Failed++
		fmt.Fprintf(os.Stderr, "observe %s/%s: %v\n", ns, name, obsErr)

		recErr := recordObserveFailure(ctx, client, kfObj, obsErr, now)
		switch {
		case recErr == nil:
		case errors.IsNotFound(recErr):
			// Deleted between listing and patching. Nothing to attribute to.
			fmt.Fprintf(os.Stderr, "observe %s/%s: gone before its failure could be recorded\n", ns, name)
		default:
			outcome.Unrecorded = append(outcome.Unrecorded,
				fmt.Sprintf("%s/%s: %v", ns, name, recErr))
			fmt.Fprintf(os.Stderr, "observe %s/%s: FAILED TO RECORD failure on status: %v\n", ns, name, recErr)
		}
	}

	fmt.Printf("%s\n", outcome.summary())

	// Exit code reflects the LOOP, not the functions (kdex-tech/knative-deployer#9).
	//
	// A per-function failure is now recorded on that function's own status, so
	// it is queryable without log archaeology and does not need the Job's exit
	// code to carry it. Failing the Job for it would mean one persistently
	// broken function marks every run red until operators stop reading the
	// signal -- which is the state this replaces.
	//
	// The one exception is a failure we could not record: exiting zero is only
	// safe while every failure lands somewhere durable, so losing that makes it
	// a real, actionable Job failure.
	if len(outcome.Unrecorded) > 0 {
		return fmt.Errorf("%s; %d failure(s) could not be recorded on their function's status: %s",
			outcome.summary(), len(outcome.Unrecorded), strings.Join(outcome.Unrecorded, "; "))
	}

	return nil
}

// observeFunction runs the original single-function observation against one
// KDexFunction. Per-function inputs come from the CR rather than env vars,
// since in per-host mode one CronJob covers many functions.
func observeFunction(
	ctx context.Context,
	client dynamic.Interface,
	kfObj *unstructured.Unstructured,
	cfg *EnvConfig,
) error {
	name := kfObj.GetName()
	namespace := kfObj.GetNamespace()

	// basePath is per-function, so read it off the CR. Fall back to the env
	// var for the legacy single-function CronJob, which still sets it.
	basePath, found, _ := unstructured.NestedString(kfObj.Object, "spec", "api", "basePath")
	if !found || basePath == "" {
		basePath = cfg.FunctionBasePath
	}

	// 1. Get Knative Service Status. A NotFound is a normal state for
	// brand-new functions that haven't built+deployed yet; we keep
	// going so the kpack-Build auto-recovery check below still runs
	// (preempted Build pods are exactly the case where the Knative
	// Service hasn't appeared yet).
	ksClient := client.Resource(knativeServiceGVR).Namespace(namespace)
	ksObj, err := ksClient.Get(ctx, name, metav1.GetOptions{})
	ksNotFound := false
	if err != nil {
		if errors.IsNotFound(err) {
			ksNotFound = true
			fmt.Printf("Knative Service %s/%s not found (yet)\n", namespace, name)
		} else {
			return fmt.Errorf("failed to get knative service: %w", err)
		}
	}

	isReady := false
	msg := ""
	url := ""
	if !ksNotFound {
		isReady, msg, url = parseKnativeStatus(ksObj)
		fmt.Printf("Observation %s/%s: Ready=%v, Msg=%s, URL=%s\n", namespace, name, isReady, msg, url)
	}

	kfClient := client.Resource(kdexFunctionGVR).Namespace(namespace)

	// 2a. kpack Build auto-recovery. When the Knative Service isn't
	// Ready (or doesn't exist), inspect the upstream kpack Build for a
	// preemption signal and retry via image.kpack.io/additionalBuildNeeded
	// if we're inside the budget + cooldown. No-op for the happy path
	// (Service Ready or no kpack Image involved) and for genuine build
	// failures (those need operator attention, not auto-retry).
	if !isReady {
		if retryErr := checkAndRetryFailedBuild(ctx, client, kfObj, cfg); retryErr != nil {
			fmt.Fprintf(os.Stderr, "auto-recovery check error (continuing): %v\n", retryErr)
		}
	}

	if ksNotFound {
		return nil
	}

	// 3. Update Status if needed
	// We only sync URL and State if it diverged or isn't set

	// Check current state
	status, _, _ := unstructured.NestedMap(kfObj.Object, "status")
	currentState, _, _ := unstructured.NestedString(status, "state")
	currentURL, _, _ := unstructured.NestedString(status, "url")

	needsUpdate := false

	// Status transition logic
	newState := currentState
	newDetail := ""

	if isReady {
		if currentState != "Ready" {
			newState = "Ready"
			newDetail = fmt.Sprintf("Ready: %s%s", url, basePath)
			needsUpdate = true
		}
		if currentURL != url {
			needsUpdate = true
		}
	} else {
		// If not ready, we might want to reflect that, but avoid flapping during transient issues.
		// For now, if it WAS Ready and now is NOT, maybe we should degrade it?
		// But Knative scales to zero, so it might be "Ready" but not running.
		// "Ready" condition in Knative Service usually means configuration is valid and routes are set up.
		// Scale to zero doesn't clear Ready condition usually.
		if currentState == "Ready" {
			// It was ready, now it's not.
			newState = "FunctionDeployed" // Fallback? Or keep Ready but Degraded condition?
			newDetail = fmt.Sprintf("NotReady: %s%s", url, basePath)
			needsUpdate = true
		}
	}

	if needsUpdate {
		fmt.Printf("Updating KDexFunction %s/%s status: State=%s -> %s\n", namespace, name, currentState, newState)

		// Update Status
		// Note: We should use Apply or UpdateStatus

		// Let's patch spec/status.
		// Construct patch
		var patch map[string]any
		specPatch := map[string]any{
			"status": map[string]any{
				"state": newState,
				"url":   url,
			},
		}
		if newDetail != "" {
			specPatch["status"].(map[string]any)["detail"] = newDetail
		}

		// Also update conditions?
		// Simplifying for now.

		patch = specPatch
		patchBytes, _ := json.Marshal(patch)

		_, err = kfClient.Patch(ctx, name, types.MergePatchType, patchBytes, metav1.PatchOptions{
			FieldManager: "kdex-knative-observer",
		}, "status")
		if err != nil {
			return fmt.Errorf("failed to patch kdex function status: %w", err)
		}
	} else {
		fmt.Println("No status update needed")
	}

	return nil
}

func parseKnativeStatus(obj *unstructured.Unstructured) (bool, string, string) {
	status, found, err := unstructured.NestedMap(obj.Object, "status")
	if err != nil || !found {
		return false, "No status", ""
	}

	url, _, _ := unstructured.NestedString(status, "url")

	conditions, found, err := unstructured.NestedSlice(status, "conditions")
	if err != nil || !found {
		return false, "No conditions", url
	}

	for _, c := range conditions {
		cond, ok := c.(map[string]any)
		if !ok {
			continue
		}
		if cond["type"] == "Ready" {
			if cond["status"] == "True" {
				return true, "", url
			}
			return false, fmt.Sprintf("%v", cond["message"]), url
		}
	}

	return false, "Ready condition not found", url
}

func waitForReady(ctx context.Context, client dynamic.ResourceInterface, name string) (string, error) {
	timeout := time.After(5 * time.Minute)
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-timeout:
			return "", fmt.Errorf("timeout waiting for service readiness")
		case <-ticker.C:
			obj, err := client.Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				if errors.IsNotFound(err) {
					continue
				}
				return "", err
			}

			isReady, msg, url := parseKnativeStatus(obj)

			if isReady {
				return url, nil
			}

			if msg != "" {
				fmt.Printf("Waiting... (Reason: %s)\n", msg)
			}
		}
	}
}

func writeTerminationMessage(url string) error {
	msg := map[string]string{
		"url": url,
	}
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	path := "/dev/termination-log"
	if custom := os.Getenv("TERMINATION_LOG_PATH"); custom != "" {
		path = custom
	}

	return os.WriteFile(path, data, 0644)
}
