#!/bin/bash

set -e

echo "Verifying KDex KNative FaaSAdaptor chart..."

NAMESPACE="kdex-system"
CHART_DIR="./chart"

# Render templates
echo "Rendering templates..."
RENDERED=$(helm template test-release $CHART_DIR --namespace $NAMESPACE)

# Check for KDexClusterFaaSAdaptor
if echo "$RENDERED" | grep -q "kind: KDexClusterFaaSAdaptor"; then
  echo "✔ KDexClusterFaaSAdaptor found."
else
  echo "✘ KDexClusterFaaSAdaptor NOT found."
  exit 1
fi

# Check for namespaces (ignore empty ones and common defaults)
# Use awk to find lines where the key is exactly 'namespace:' and check the value.
BAD_NAMESPACES=$(echo "$RENDERED" | awk -v ns="$NAMESPACE" '
  $1 == "namespace:" {
    val = $2;
    # Strip quotes if present
    gsub(/^"|"$/, "", val);
    gsub(/^'\''|'\''$/, "", val);
    if (val != "" && val != ns && val != "default" && val != "kube-system") {
      print val;
    }
  }
')

if [ -n "$BAD_NAMESPACES" ]; then
  echo "✘ Found unexpected hardcoded namespaces:"
  echo "$BAD_NAMESPACES"
  exit 1
else
  echo "✔ All resources target $NAMESPACE (or allowed defaults)."
fi

# Check for ClusterBuilder
if echo "$RENDERED" | grep -q "kind: ClusterBuilder"; then
  echo "✔ ClusterBuilder found."
else
  echo "✘ ClusterBuilder NOT found."
  exit 1
fi

# Check for RBAC
if echo "$RENDERED" | grep -q "kind: ClusterRole"; then
  echo "✔ ClusterRole found."
else
  echo "✘ ClusterRole NOT found."
  exit 1
fi

echo "✔ Chart verification successful!"
