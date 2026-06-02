package main

import (
	"os"
	"strings"
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

func TestLoadEnv_ServiceAccountName(t *testing.T) {
	t.Cleanup(func() { os.Clearenv() })
	os.Clearenv()
	_ = os.Setenv("FUNCTION_NAME", "myfunc")
	_ = os.Setenv("FUNCTION_NAMESPACE", "myns")

	// Unset: FunctionServiceAccountName should be empty.
	cfg, err := LoadEnv()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.FunctionServiceAccountName != "" {
		t.Errorf("expected empty FunctionServiceAccountName when env unset; got %q", cfg.FunctionServiceAccountName)
	}

	// Set: the cfg field carries the value through.
	_ = os.Setenv("FUNCTION_SERVICE_ACCOUNT_NAME", "my-runtime-sa")
	cfg, err = LoadEnv()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.FunctionServiceAccountName != "my-runtime-sa" {
		t.Errorf("FunctionServiceAccountName = %q; want my-runtime-sa", cfg.FunctionServiceAccountName)
	}
}

func TestLoadEnv_TolerationsAndNodeSelector(t *testing.T) {
	t.Cleanup(func() { os.Clearenv() })
	os.Clearenv()
	_ = os.Setenv("FUNCTION_NAME", "myfunc")
	_ = os.Setenv("FUNCTION_NAMESPACE", "myns")

	// Unset: both fields empty, no rejection.
	cfg, err := LoadEnv()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.FunctionTolerations != "" || cfg.FunctionNodeSelector != "" {
		t.Errorf("expected both empty when unset; got tols=%q ns=%q",
			cfg.FunctionTolerations, cfg.FunctionNodeSelector)
	}

	// Set: env values flow through verbatim (parsing happens in runDeploy).
	tols := `[{"key":"cloud.google.com/gke-spot","operator":"Equal","value":"true","effect":"NoSchedule"}]`
	ns := `{"component":"workload","kubernetes.io/arch":"arm64"}`
	_ = os.Setenv("FUNCTION_TOLERATIONS", tols)
	_ = os.Setenv("FUNCTION_NODE_SELECTOR", ns)
	cfg, err = LoadEnv()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.FunctionTolerations != tols {
		t.Errorf("FunctionTolerations = %q; want %q", cfg.FunctionTolerations, tols)
	}
	if cfg.FunctionNodeSelector != ns {
		t.Errorf("FunctionNodeSelector = %q; want %q", cfg.FunctionNodeSelector, ns)
	}
}

// buildContainerEnv: secretKeyRef survives the host-manager -> deployer
// Job -> knative-deployer boundary unchanged. Regression for the
// security finding where the kubelet dereferenced spec.env's
// valueFrom.secretKeyRef at deployer-pod start (because host-manager
// used to splice spec.env into the Job's env block directly), and the
// resulting Revision YAML stored secrets as plaintext .value entries.
// The fix routes user env opaquely as JSON in FUNCTION_USER_ENV; this
// test pins that the unmarshaled entries land in containerEnv with
// valueFrom shape intact.
func TestBuildContainerEnv_PreservesSecretKeyRef(t *testing.T) {
	cfg := &EnvConfig{
		FunctionUserEnv: `[
			{"name":"RESEND_API_KEY","valueFrom":{"secretKeyRef":{"name":"knowdrive-resend-credentials","key":"api_key"}}},
			{"name":"HOST_DOMAIN","value":"dev.knowdrive.ai"}
		]`,
	}
	env, err := buildContainerEnv(cfg, func(string) string { return "" })
	if err != nil {
		t.Fatalf("buildContainerEnv: %v", err)
	}
	if len(env) != 2 {
		t.Fatalf("env len = %d; want 2", len(env))
	}

	// Entry 0: secretKeyRef preserved (no inline .value).
	if env[0]["name"] != "RESEND_API_KEY" {
		t.Errorf("env[0].name = %v; want RESEND_API_KEY", env[0]["name"])
	}
	if _, hasValue := env[0]["value"]; hasValue {
		t.Errorf("env[0] must NOT have a 'value' key (secretKeyRef path); got %+v", env[0])
	}
	vf, ok := env[0]["valueFrom"].(map[string]any)
	if !ok {
		t.Fatalf("env[0].valueFrom not a map: %+v", env[0]["valueFrom"])
	}
	skr, ok := vf["secretKeyRef"].(map[string]any)
	if !ok {
		t.Fatalf("env[0].valueFrom.secretKeyRef not a map: %+v", vf["secretKeyRef"])
	}
	if skr["name"] != "knowdrive-resend-credentials" || skr["key"] != "api_key" {
		t.Errorf("secretKeyRef = %+v; want {name:knowdrive-resend-credentials, key:api_key}", skr)
	}

	// Entry 1: plain literal value flows through.
	if env[1]["name"] != "HOST_DOMAIN" || env[1]["value"] != "dev.knowdrive.ai" {
		t.Errorf("env[1] = %+v; want {name:HOST_DOMAIN, value:dev.knowdrive.ai}", env[1])
	}
}

// ForwardedEnvVars (the legacy path) still works for the controller-
// populated common vars (AUDIENCE / FUNCTION_* / ISSUER / etc.). Those
// are not secrets and need name+value forwarding via os.Getenv.
func TestBuildContainerEnv_ForwardedVarsFromGetenv(t *testing.T) {
	cfg := &EnvConfig{ForwardedEnvVars: "AUDIENCE,FUNCTION_NAME, , FUNCTION_NAMESPACE"}
	env, err := buildContainerEnv(cfg, func(name string) string {
		return map[string]string{
			"AUDIENCE":           "http://x.svc",
			"FUNCTION_NAME":      "fn",
			"FUNCTION_NAMESPACE": "ns",
		}[name]
	})
	if err != nil {
		t.Fatalf("buildContainerEnv: %v", err)
	}
	// Empty entry in the comma list (between FUNCTION_NAME and FUNCTION_NAMESPACE)
	// is skipped, so 3 entries not 4.
	if len(env) != 3 {
		t.Fatalf("env len = %d; want 3 (empty entry should be skipped)", len(env))
	}
	want := []map[string]any{
		{"name": "AUDIENCE", "value": "http://x.svc"},
		{"name": "FUNCTION_NAME", "value": "fn"},
		{"name": "FUNCTION_NAMESPACE", "value": "ns"},
	}
	for i, w := range want {
		if env[i]["name"] != w["name"] || env[i]["value"] != w["value"] {
			t.Errorf("env[%d] = %+v; want %+v", i, env[i], w)
		}
	}
}

// Both layers compose: forwarded common vars first, then user env splat
// after. Order matters because Kubernetes resolves env-var
// references positionally (later entries can reference earlier names).
func TestBuildContainerEnv_BothLayersInOrder(t *testing.T) {
	cfg := &EnvConfig{
		ForwardedEnvVars: "AUDIENCE",
		FunctionUserEnv:  `[{"name":"RESEND_API_KEY","valueFrom":{"secretKeyRef":{"name":"s","key":"k"}}}]`,
	}
	env, err := buildContainerEnv(cfg, func(string) string { return "http://x" })
	if err != nil {
		t.Fatalf("buildContainerEnv: %v", err)
	}
	if len(env) != 2 || env[0]["name"] != "AUDIENCE" || env[1]["name"] != "RESEND_API_KEY" {
		t.Errorf("expected [AUDIENCE, RESEND_API_KEY]; got %+v", env)
	}
}

// Malformed FUNCTION_USER_ENV must error (don't silently drop user env -
// the caller should see the misconfiguration and fail the deploy Job).
func TestBuildContainerEnv_MalformedUserEnvErrors(t *testing.T) {
	cfg := &EnvConfig{FunctionUserEnv: `not-valid-json`}
	if _, err := buildContainerEnv(cfg, func(string) string { return "" }); err == nil {
		t.Fatal("expected error on malformed FUNCTION_USER_ENV; got nil")
	}
}

// FUNCTION_USER_ENV carries the function CR's spec.env block opaquely
// (JSON-marshaled []corev1.EnvVar) so valueFrom.secretKeyRef shape
// survives the host-manager -> deployer-Job -> knative-deployer boundary.
// This test pins that LoadEnv reads it verbatim; the JSON unmarshal +
// containerEnv splice happens later in runDeploy.
func TestLoadEnv_FunctionUserEnv(t *testing.T) {
	t.Cleanup(func() { os.Clearenv() })
	os.Clearenv()
	_ = os.Setenv("FUNCTION_NAME", "myfunc")
	_ = os.Setenv("FUNCTION_NAMESPACE", "myns")

	cfg, err := LoadEnv()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.FunctionUserEnv != "" {
		t.Errorf("expected empty FunctionUserEnv when env unset; got %q", cfg.FunctionUserEnv)
	}

	userEnv := `[{"name":"RESEND_API_KEY","valueFrom":{"secretKeyRef":{"name":"knowdrive-resend-credentials","key":"api_key"}}},{"name":"HOST_DOMAIN","value":"dev.knowdrive.ai"}]`
	_ = os.Setenv("FUNCTION_USER_ENV", userEnv)
	cfg, err = LoadEnv()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.FunctionUserEnv != userEnv {
		t.Errorf("FunctionUserEnv = %q; want %q", cfg.FunctionUserEnv, userEnv)
	}
}

// TestBuildPodSpec_VolumesAndMounts pins kdex-tech/kdex-crds#10: FUNCTION_VOLUMES
// lands on spec.template.spec.volumes and FUNCTION_VOLUME_MOUNTS lands on the
// function container's volumeMounts, so file-based config can be projected into
// executable/source-built functions.
func TestBuildPodSpec_VolumesAndMounts(t *testing.T) {
	cfg := &EnvConfig{
		FunctionImage:        "img",
		FunctionVolumes:      `[{"name":"cfg","secret":{"secretName":"app-cfg"}}]`,
		FunctionVolumeMounts: `[{"name":"cfg","mountPath":"/etc/app"}]`,
	}
	podSpec, err := buildPodSpec(cfg, []map[string]any{})
	if err != nil {
		t.Fatalf("buildPodSpec: %v", err)
	}

	vols, ok := podSpec["volumes"].([]any)
	if !ok || len(vols) != 1 {
		t.Fatalf("podSpec.volumes = %#v; want 1 entry", podSpec["volumes"])
	}
	if name := vols[0].(map[string]any)["name"]; name != "cfg" {
		t.Errorf("volumes[0].name = %v; want cfg", name)
	}

	containers := podSpec["containers"].([]map[string]any)
	mounts, ok := containers[0]["volumeMounts"].([]any)
	if !ok || len(mounts) != 1 {
		t.Fatalf("containers[0].volumeMounts = %#v; want 1 entry", containers[0]["volumeMounts"])
	}
	if mp := mounts[0].(map[string]any)["mountPath"]; mp != "/etc/app" {
		t.Errorf("volumeMounts[0].mountPath = %v; want /etc/app", mp)
	}
}

// TestBuildPodSpec_NoVolumes_OmitsKeys keeps the additive path a no-op: with no
// volumes/mounts the keys must be absent (preserves pre-#10 podspec shape).
func TestBuildPodSpec_NoVolumes_OmitsKeys(t *testing.T) {
	podSpec, err := buildPodSpec(&EnvConfig{FunctionImage: "img"}, []map[string]any{})
	if err != nil {
		t.Fatalf("buildPodSpec: %v", err)
	}
	if _, ok := podSpec["volumes"]; ok {
		t.Error("podSpec.volumes present but should be omitted")
	}
	containers := podSpec["containers"].([]map[string]any)
	if _, ok := containers[0]["volumeMounts"]; ok {
		t.Error("containers[0].volumeMounts present but should be omitted")
	}
}

// TestBuildKnativeService_InternalIsClusterLocal pins kdex-tech/kdex-crds#6:
// FUNCTION_INTERNAL=true labels the Service networking.knative.dev/visibility=
// cluster-local so Knative refuses external traffic to it.
func TestBuildKnativeService_InternalIsClusterLocal(t *testing.T) {
	cfg := &EnvConfig{FunctionName: "fn", FunctionNamespace: "dev", FunctionInternal: "true"}
	svc := buildKnativeService(cfg, map[string]any{"containers": []map[string]any{{"image": "img"}}})

	meta := svc.Object["metadata"].(map[string]any)
	labels := meta["labels"].(map[string]any)
	if got := labels["networking.knative.dev/visibility"]; got != "cluster-local" {
		t.Errorf("Service label networking.knative.dev/visibility = %v; want cluster-local", got)
	}
}

// TestBuildKnativeService_NonInternalHasNoVisibilityLabel keeps non-internal
// functions externally reachable (no visibility label).
func TestBuildKnativeService_NonInternalHasNoVisibilityLabel(t *testing.T) {
	cfg := &EnvConfig{FunctionName: "fn", FunctionNamespace: "dev"}
	svc := buildKnativeService(cfg, map[string]any{"containers": []map[string]any{{"image": "img"}}})

	meta := svc.Object["metadata"].(map[string]any)
	labels := meta["labels"].(map[string]any)
	if _, ok := labels["networking.knative.dev/visibility"]; ok {
		t.Error("non-internal Service must not carry the cluster-local visibility label")
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

// Knative's validation webhook rejects autoscaling.knative.dev/* annotations
// placed on the Service's metadata.annotations:
//
//	autoscaling annotations must be put under "spec.template.metadata.annotations" to work
//
// buildKnativeService MUST emit them under the Revision template's metadata.
// Regression coverage for kdex-tech/knative-deployer#4.
func TestBuildKnativeService_ScalingAnnotationsOnTemplateMetadata(t *testing.T) {
	cfg := &EnvConfig{
		FunctionName:       "myfunc",
		FunctionNamespace:  "myns",
		FunctionGeneration: "1",
		ScalingMinScale:    "1",
		ScalingTarget:      "100",
	}
	podSpec := map[string]any{
		"containers": []map[string]any{{"image": "myimg"}},
	}

	svc := buildKnativeService(cfg, podSpec)

	// Autoscaling annotations must be on the Revision template's metadata.
	spec, ok := svc.Object["spec"].(map[string]any)
	if !ok {
		t.Fatalf("spec missing or wrong type: %#v", svc.Object["spec"])
	}
	tmpl, ok := spec["template"].(map[string]any)
	if !ok {
		t.Fatalf("spec.template missing or wrong type: %#v", spec["template"])
	}
	tmplMeta, ok := tmpl["metadata"].(map[string]any)
	if !ok {
		t.Fatalf("spec.template.metadata missing or wrong type: %#v", tmpl["metadata"])
	}
	tmplAnnos, ok := tmplMeta["annotations"].(map[string]string)
	if !ok {
		t.Fatalf("spec.template.metadata.annotations missing or wrong type: %#v", tmplMeta["annotations"])
	}
	if got := tmplAnnos["autoscaling.knative.dev/min-scale"]; got != "1" {
		t.Errorf("spec.template.metadata.annotations[\"autoscaling.knative.dev/min-scale\"] = %q, want %q", got, "1")
	}
	if got := tmplAnnos["autoscaling.knative.dev/target"]; got != "100" {
		t.Errorf("spec.template.metadata.annotations[\"autoscaling.knative.dev/target\"] = %q, want %q", got, "100")
	}

	// And NOT on the Service's metadata.annotations — Knative's webhook
	// rejects that placement.
	svcMeta, ok := svc.Object["metadata"].(map[string]any)
	if !ok {
		t.Fatalf("metadata missing or wrong type: %#v", svc.Object["metadata"])
	}
	if svcAnnos, present := svcMeta["annotations"]; present {
		annoMap, ok := svcAnnos.(map[string]string)
		if !ok {
			t.Fatalf("metadata.annotations wrong type: %#v", svcAnnos)
		}
		for k := range annoMap {
			if strings.HasPrefix(k, "autoscaling.knative.dev/") {
				t.Errorf("autoscaling key %q on Service metadata.annotations; must live under spec.template.metadata.annotations (Knative webhook will reject)", k)
			}
		}
	}
}

// When the CR carries no scaling block at all, buildKnativeService must omit
// the annotations key entirely — empty maps would still serialize and may
// confuse downstream readers that probe for presence.
func TestBuildKnativeService_NoScalingProducesNoTemplateAnnotations(t *testing.T) {
	cfg := &EnvConfig{
		FunctionName:       "myfunc",
		FunctionNamespace:  "myns",
		FunctionGeneration: "1",
	}
	podSpec := map[string]any{
		"containers": []map[string]any{{"image": "myimg"}},
	}

	svc := buildKnativeService(cfg, podSpec)

	tmplMeta := svc.Object["spec"].(map[string]any)["template"].(map[string]any)["metadata"].(map[string]any)
	if _, present := tmplMeta["annotations"]; present {
		t.Errorf("spec.template.metadata.annotations should be absent when no SCALING_* env is set; got %#v", tmplMeta["annotations"])
	}
}

// TestBuildKnativeService_AlwaysEmitsServiceMetadataAnnotations pins a
// regression for the SSA ownership shape on the Service object's
// metadata.annotations field.
//
// Pre-v0.1.23 always called service.SetAnnotations(annotations) — which
// writes metadata.annotations to either a populated map (when SCALING_*
// envs were set) OR an empty map literal {} (when they weren't). Either
// way, the SSA apply CLAIMED structural ownership of metadata.annotations
// under the "kdex-knative-deployer" field manager.
//
// v0.1.23's refactor stopped touching Service-level annotations entirely.
// When functions had no scaling block, the marshaled JSON omitted the
// metadata.annotations field entirely. With Force=true SSA, the apiserver
// then interpreted the missing field as the deployer releasing ownership
// of metadata.annotations — and Knative's mutating webhook (which had
// stamped serving.knative.dev/{creator,lastModifier} on those previously
// deployer-owned annotations) rejected the apply:
//
//	admission webhook "validation.webhook.serving.knative.dev" denied:
//	annotation value is immutable: metadata.annotations.serving.knative.dev/creator
//	invalid value: : metadata.annotations.serving.knative.dev/lastModifier
//
// Symptom on the cluster: every deployer Job for a function WITHOUT a
// scaling block (user-service-{profile,auth,admin}) BackoffLimitExceeded
// at v0.1.23. Functions WITH a scaling block were unaffected because
// their template annotations map kept the structural claim alive.
//
// Fix: always emit metadata.annotations as a (possibly empty) map so
// the SSA structural ownership shape matches what v0.1.22 was doing.
// This way Knative's webhook keeps its serving.knative.dev/* keys
// undisturbed and re-applies of unchanged Services no-op.
func TestBuildKnativeService_AlwaysEmitsServiceMetadataAnnotations(t *testing.T) {
	cfg := &EnvConfig{
		FunctionName:       "myfunc",
		FunctionNamespace:  "myns",
		FunctionGeneration: "1",
		// Intentionally no SCALING_* envs — this is the user-service-*
		// shape that broke at v0.1.23.
	}
	podSpec := map[string]any{
		"containers": []map[string]any{{"image": "myimg"}},
	}

	svc := buildKnativeService(cfg, podSpec)

	meta, ok := svc.Object["metadata"].(map[string]any)
	if !ok {
		t.Fatalf("metadata missing or wrong type: %#v", svc.Object["metadata"])
	}
	annosRaw, present := meta["annotations"]
	if !present {
		t.Fatalf("metadata.annotations key absent; SSA needs the structural shape preserved (Knative webhook rejects re-applies otherwise). Got metadata = %#v", meta)
	}
	annos, ok := annosRaw.(map[string]string)
	if !ok {
		t.Fatalf("metadata.annotations wrong type: %#v", annosRaw)
	}
	// No autoscaling annotations should be here — those go under the
	// Revision template (the original #4 fix).
	for k := range annos {
		if strings.HasPrefix(k, "autoscaling.knative.dev/") {
			t.Errorf("autoscaling key %q on Service metadata.annotations; must live under spec.template.metadata.annotations", k)
		}
	}
}
