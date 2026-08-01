package main

import (
	"context"
	"os"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// labelledFunction builds a KDexFunction carrying the observed-by label for a
// host, plus a basePath so per-function detail strings can be checked.
func labelledFunction(name, host, basePath string) *unstructured.Unstructured {
	meta := map[string]any{
		"name":      name,
		"namespace": testNamespace,
	}
	if host != "" {
		meta["labels"] = map[string]any{ObservedByLabel: host}
	}
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "kdex.dev/v1alpha1",
		"kind":       "KDexFunction",
		"metadata":   meta,
		"spec": map[string]any{
			"api": map[string]any{"basePath": basePath},
		},
	}}
}

// TestObservedFunctions_PerHostSelectsLabelledOnly is the core of the per-host
// topology (kdex-tech/host-manager#156): one CronJob per host must pick up
// exactly the functions host-manager labelled for that host -- not every
// function in the namespace. Service-backed functions never had an observer,
// and that must stay true.
func TestObservedFunctions_PerHostSelectsLabelledOnly(t *testing.T) {
	client := newFakeClient(
		labelledFunction("fn-a", testHost, "/v1/a"),
		labelledFunction("fn-b", testHost, "/v1/b"),
		labelledFunction("fn-other-host", "other-host", "/v1/c"),
		labelledFunction("fn-unmanaged", "", "/v1/d"), // service-backed: no label
	)

	cfg := &EnvConfig{FunctionNamespace: testNamespace, FunctionHost: testHost}
	got, err := observedFunctions(context.Background(), client, cfg)
	if err != nil {
		t.Fatalf("observedFunctions: %v", err)
	}

	names := map[string]bool{}
	for _, f := range got {
		names[f.GetName()] = true
	}
	if len(got) != 2 || !names["fn-a"] || !names["fn-b"] {
		t.Fatalf("per-host set = %v; want exactly fn-a and fn-b", names)
	}
	if names["fn-other-host"] {
		t.Error("picked up a function belonging to a different host")
	}
	if names["fn-unmanaged"] {
		t.Error("picked up an unlabelled (service-backed) function")
	}
}

// TestObservedFunctions_LegacySingleFunction keeps the old per-function
// CronJobs working while the new image rolls out ahead of host-manager's
// topology switch.
func TestObservedFunctions_LegacySingleFunction(t *testing.T) {
	client := newFakeClient(
		labelledFunction("fn-a", testHost, "/v1/a"),
		labelledFunction("fn-b", testHost, "/v1/b"),
	)

	cfg := &EnvConfig{
		FunctionNamespace: testNamespace,
		FunctionHost:      testHost,
		FunctionName:      "fn-b", // legacy CronJob sets this
	}
	got, err := observedFunctions(context.Background(), client, cfg)
	if err != nil {
		t.Fatalf("observedFunctions: %v", err)
	}
	if len(got) != 1 || got[0].GetName() != "fn-b" {
		t.Fatalf("legacy mode = %v; want just fn-b", got)
	}
}

// TestObservedFunctions_EmptySetIsNotAnError: a host with no functions yet
// must be a clean no-op, not a CronJob failure that alerts operators.
func TestObservedFunctions_EmptySetIsNotAnError(t *testing.T) {
	client := newFakeClient()
	cfg := &EnvConfig{FunctionNamespace: testNamespace, FunctionHost: testHost}

	got, err := observedFunctions(context.Background(), client, cfg)
	if err != nil {
		t.Fatalf("empty set should not error: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("want empty, got %d", len(got))
	}
}

// TestLoadEnv_ObserveAcceptsHostWithoutName pins the relaxed contract: the
// per-host observer has no single FUNCTION_NAME, but every other mode still
// requires one.
func TestLoadEnv_ObserveAcceptsHostWithoutName(t *testing.T) {
	t.Cleanup(os.Clearenv)

	origArgs := os.Args
	t.Cleanup(func() { os.Args = origArgs })

	os.Clearenv()
	os.Args = []string{"deployer", "observe"}
	_ = os.Setenv("FUNCTION_NAMESPACE", testNamespace)
	_ = os.Setenv("FUNCTION_HOST", testHost)

	cfg, err := LoadEnv()
	if err != nil {
		t.Fatalf("observe with FUNCTION_HOST and no FUNCTION_NAME: %v", err)
	}
	if cfg.FunctionHost != testHost {
		t.Errorf("FunctionHost = %q", cfg.FunctionHost)
	}

	// Neither name nor host -> still an error.
	_ = os.Unsetenv("FUNCTION_HOST")
	if _, err := LoadEnv(); err == nil {
		t.Error("observe with neither FUNCTION_NAME nor FUNCTION_HOST must fail")
	}

	// Non-observe mode still demands FUNCTION_NAME.
	os.Args = []string{"deployer", "deploy"}
	if _, err := LoadEnv(); err == nil {
		t.Error("deploy without FUNCTION_NAME must fail")
	}
}
