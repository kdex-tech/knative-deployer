#!/bin/bash

set -e

echo "Verifying KDex KNative FaaSAdaptor chart..."

NAMESPACE="kdex-system"
CHART_DIR="./chart"

verify_namespaces() {
  local rendered="$1"
  local bad
  bad=$(echo "$rendered" | awk -v ns="$NAMESPACE" '
    $1 == "namespace:" {
      val = $2;
      gsub(/^"|"$/, "", val);
      gsub(/^'\''|'\''$/, "", val);
      if (val != "" && val != ns && val != "default" && val != "kube-system") {
        print val;
      }
    }
  ')
  if [ -n "$bad" ]; then
    echo "✘ Found unexpected hardcoded namespaces:"
    echo "$bad"
    exit 1
  fi
  echo "✔ All resources target $NAMESPACE (or allowed defaults)."
}

assert_kind_present() {
  local rendered="$1"; local kind="$2"
  if echo "$rendered" | grep -q "kind: $kind"; then
    echo "✔ $kind found."
  else
    echo "✘ $kind NOT found."
    exit 1
  fi
}

assert_kind_absent() {
  local rendered="$1"; local kind="$2"
  if echo "$rendered" | grep -q "kind: $kind"; then
    echo "✘ $kind unexpectedly present."
    exit 1
  fi
  echo "✔ $kind absent (as expected)."
}

assert_substring_absent() {
  local rendered="$1"; local pattern="$2"; local label="$3"
  if echo "$rendered" | grep -q "$pattern"; then
    echo "✘ $label unexpectedly present."
    exit 1
  fi
  echo "✔ $label absent (as expected)."
}

# --- Default render: bundled Knative Serving + kpack both ON ---
echo
echo "[1/2] Rendering with default values (knativeServing.enabled=true, kpack.enabled=true)..."
RENDERED_DEFAULT=$(helm template test-release "$CHART_DIR" --namespace "$NAMESPACE")

assert_kind_present "$RENDERED_DEFAULT" "KDexClusterFaaSAdaptor"
assert_kind_present "$RENDERED_DEFAULT" "ClusterBuilder"
assert_kind_present "$RENDERED_DEFAULT" "ClusterRole"
verify_namespaces "$RENDERED_DEFAULT"

# --- Toggles-off render: external Knative Serving + external kpack ---
echo
echo "[2/2] Rendering with knativeServing.enabled=false, kpack.enabled=false..."
RENDERED_OFF=$(helm template test-release "$CHART_DIR" --namespace "$NAMESPACE" \
  --set knativeServing.enabled=false --set kpack.enabled=false)

assert_kind_present "$RENDERED_OFF" "KDexClusterFaaSAdaptor"
assert_kind_present "$RENDERED_OFF" "ClusterRole"
# Check for declared resources (apiVersion: …) rather than `kind: …` matches,
# because the deployer's CRD references kpack kinds (e.g. builderRef.kind:
# ClusterBuilder) by name without an apiVersion. apiVersion lines only appear
# on actual resource declarations.
assert_substring_absent "$RENDERED_OFF" "apiVersion: serving.knative.dev"      "knative-serving resources"
assert_substring_absent "$RENDERED_OFF" "apiVersion: networking.internal.knative.dev" "knative networking resources"
assert_substring_absent "$RENDERED_OFF" "apiVersion: caching.internal.knative.dev"    "knative caching resources"
assert_substring_absent "$RENDERED_OFF" "apiVersion: kpack.io/"                "kpack resources"
verify_namespaces "$RENDERED_OFF"

echo
echo "✔ Chart verification successful!"
