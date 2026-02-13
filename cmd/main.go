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
	FunctionHost       string
	FunctionGeneration string
	FunctionName       string
	FunctionNamespace  string
	FunctionImage      string
	ForwardedEnvVars   string
	Audience           string
	Issuer             string
	JWKSURL            string
}

func LoadEnv() (*EnvConfig, error) {
	cfg := &EnvConfig{
		FunctionHost:       os.Getenv("FUNCTION_HOST"),
		FunctionGeneration: os.Getenv("FUNCTION_GENERATION"),
		FunctionName:       os.Getenv("FUNCTION_NAME"),
		FunctionNamespace:  os.Getenv("FUNCTION_NAMESPACE"),
		FunctionImage:      os.Getenv("FUNCTION_IMAGE"),
		ForwardedEnvVars:   os.Getenv("FORWARDED_ENV_VARS"),
		Audience:           os.Getenv("AUDIENCE"),
		Issuer:             os.Getenv("ISSUER"),
		JWKSURL:            os.Getenv("JWKS_URL"),
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
						"annotations": map[string]any{
							"autoscaling.knative.dev/minScale": "0", // Default to scale to zero
						},
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
			newDetail = fmt.Sprintf("Ready: %s", url)
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
			newDetail = fmt.Sprintf("NotReady: %s", msg)
			needsUpdate = true
		}
	}

	if needsUpdate {
		fmt.Printf("Updating KDexFunction status: State=%s -> %s\n", currentState, newState)

		// Update Status
		// Note: We should use Apply or UpdateStatus

		// Let's patch spec/status.
		// Construct patch
		patch := map[string]any{}
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
