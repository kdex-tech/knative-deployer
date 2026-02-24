package main

import (
	"os"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestLoadEnv(t *testing.T) {
	// Backup and restore environment
	t.Cleanup(func() {
		os.Clearenv()
	})

	os.Clearenv()
	_, err := LoadEnv()
	if err == nil {
		t.Fatal("Expected error when FUNCTION_NAME is missing")
	}

	_ = os.Setenv("FUNCTION_NAME", "myfunc")
	_, err = LoadEnv()
	if err == nil {
		t.Fatal("Expected error when FUNCTION_NAMESPACE is missing")
	}

	_ = os.Setenv("FUNCTION_NAMESPACE", "myns")

	// Default case (not deploy command)
	cfg, err := LoadEnv()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if cfg.FunctionName != "myfunc" || cfg.FunctionNamespace != "myns" {
		t.Errorf("Unexpected config values: %+v", cfg)
	}

	// Test deploy args
	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()
	os.Args = []string{"cmd", "deploy"}

	_, err = LoadEnv()
	if err == nil {
		t.Fatal("Expected error when FUNCTION_IMAGE is missing for deploy")
	}

	_ = os.Setenv("FUNCTION_IMAGE", "myimg")
	cfg, err = LoadEnv()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if cfg.FunctionImage != "myimg" {
		t.Errorf("Unexpected image: %s", cfg.FunctionImage)
	}
}

func TestParseKnativeStatus(t *testing.T) {
	obj := &unstructured.Unstructured{
		Object: map[string]any{},
	}

	ready, msg, url := parseKnativeStatus(obj)
	if ready || msg != "No status" || url != "" {
		t.Errorf("Expected not ready, No status, empty url. Got %v, %s, %s", ready, msg, url)
	}

	obj.Object["status"] = map[string]any{
		"url": "http://myurl",
	}

	ready, msg, url = parseKnativeStatus(obj)
	if ready || msg != "No conditions" || url != "http://myurl" {
		t.Errorf("Expected not ready, No conditions, http://myurl. Got %v, %s, %s", ready, msg, url)
	}

	obj.Object["status"] = map[string]any{
		"url": "http://myurl",
		"conditions": []any{
			map[string]any{
				"type":   "Ready",
				"status": "True",
			},
		},
	}

	ready, msg, url = parseKnativeStatus(obj)
	if !ready || msg != "" || url != "http://myurl" {
		t.Errorf("Expected ready, empty msg, http://myurl. Got %v, %s, %s", ready, msg, url)
	}

	obj.Object["status"] = map[string]any{
		"url": "http://myurl",
		"conditions": []any{
			map[string]any{
				"type":    "Ready",
				"status":  "False",
				"message": "some error",
			},
		},
	}

	ready, msg, url = parseKnativeStatus(obj)
	if ready || msg != "some error" || url != "http://myurl" {
		t.Errorf("Expected not ready, some error, http://myurl. Got %v, %s, %s", ready, msg, url)
	}
}

func TestRunDeployAndObserve(t *testing.T) {
	os.Clearenv()
	_ = os.Setenv("FUNCTION_NAME", "myfunc")
	_ = os.Setenv("FUNCTION_NAMESPACE", "myns")
	_ = os.Setenv("FUNCTION_IMAGE", "myimg")
	_ = os.Setenv("KUBERNETES_SERVICE_HOST", "localhost")
	_ = os.Setenv("KUBERNETES_SERVICE_PORT", "6443")

	err := runDeploy()
	if err == nil {
		t.Fatal("Expected error because cluster is not reachable")
	}

	err = runObserve()
	if err == nil {
		t.Log("BUG: runObserve unexpectedly succeeded when given a mock KUBERNETES_SERVICE_HOST")
		t.Fatal("Expected error because cluster is not reachable")
	}
}

func TestWriteTerminationMessage(t *testing.T) {
	f, err := os.CreateTemp("", "term-log")
	if err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	defer func() {
		_ = os.Remove(f.Name())
	}()

	_ = os.Setenv("TERMINATION_LOG_PATH", f.Name())
	defer func() {
		_ = os.Unsetenv("TERMINATION_LOG_PATH")
	}()

	err = writeTerminationMessage("http://foo.bar")
	if err != nil {
		t.Fatal(err)
	}

	b, _ := os.ReadFile(f.Name())
	if string(b) != `{"url":"http://foo.bar"}` {
		t.Errorf("Unexpected output: %s", string(b))
	}
}
