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
	FunctionName                         string
	FunctionNamespace                    string
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
		FunctionName:                         os.Getenv("FUNCTION_NAME"),
		FunctionNamespace:                    os.Getenv("FUNCTION_NAMESPACE"),
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

	if cfg.FunctionName == "" {
		return nil, fmt.Errorf("FUNCTION_NAME is required")
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

	// Prepare env vars for the container
	containerEnv := []map[string]any{}

	// Add forwarded env vars
	if cfg.ForwardedEnvVars != "" {
		vars := strings.Split(cfg.ForwardedEnvVars, ",")
		for _, v := range vars {
			v = strings.TrimSpace(v)
			if v == "" {
				continue
			}
			val := os.Getenv(v)
			containerEnv = append(containerEnv, map[string]any{
				"name":  v,
				"value": val,
			})
		}
	}

	// Prepare Knative Service definition
	service := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "serving.knative.dev/v1",
			"kind":       "Service",
			"metadata": map[string]any{
				"name":      cfg.FunctionName,
				"namespace": cfg.FunctionNamespace,
				"labels": map[string]any{
					"kdex.dev/function":   cfg.FunctionName,
					"kdex.dev/generation": cfg.FunctionGeneration,
				},
			},
			"spec": map[string]any{
				"template": map[string]any{
					"metadata": map[string]any{
						"labels": map[string]any{
							"kdex.dev/function":   cfg.FunctionName,
							"kdex.dev/generation": cfg.FunctionGeneration,
						},
					},
					"spec": map[string]any{
						"containers": []map[string]any{
							{
								"image": cfg.FunctionImage,
								"env":   containerEnv,
							},
						},
					},
				},
			},
		},
	}

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

	service.SetAnnotations(annotations)

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

func runObserve() error {
	cfg, err := LoadEnv()
	if err != nil {
		return err
	}

	client, err := getDynamicClient()
	if err != nil {
		return err
	}

	// 1. Get Knative Service Status
	ksClient := client.Resource(knativeServiceGVR).Namespace(cfg.FunctionNamespace)
	ksObj, err := ksClient.Get(context.Background(), cfg.FunctionName, metav1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			// Service deleted? Should probably report this.
			fmt.Printf("Knative Service %s/%s not found\n", cfg.FunctionNamespace, cfg.FunctionName)
			// TODO: Update KDexFunction to failure/unknown?
			return nil
		}
		return fmt.Errorf("failed to get knative service: %w", err)
	}

	isReady, msg, url := parseKnativeStatus(ksObj)
	fmt.Printf("Observation: Ready=%v, Msg=%s, URL=%s\n", isReady, msg, url)

	// 2. Get KDexFunction
	kfClient := client.Resource(kdexFunctionGVR).Namespace(cfg.FunctionNamespace)
	kfObj, err := kfClient.Get(context.Background(), cfg.FunctionName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to get kdex function: %w", err)
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
			newDetail = fmt.Sprintf("Ready: %s%s", url, cfg.FunctionBasePath)
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
			newDetail = fmt.Sprintf("NotReady: %s%s", url, cfg.FunctionBasePath)
			needsUpdate = true
		}
	}

	if needsUpdate {
		fmt.Printf("Updating KDexFunction status: State=%s -> %s\n", currentState, newState)

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

		_, err = kfClient.Patch(context.Background(), cfg.FunctionName, types.MergePatchType, patchBytes, metav1.PatchOptions{
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
