// Per-function attribution for the observer.
//
// Before the per-host topology (kdex-tech/host-manager#156) a failing observer
// was a failed Job named after its function, so attribution was free. Aggregated
// into one Job per host, that is gone: one red Job covering N functions, and
// working out which one meant reading pod logs before they aged out.
//
// So each function carries its own observation failure on its own status, and
// that is what makes the Job's exit code free to mean "did the pass run"
// (see runObserve). Queryable directly:
//
//	kubectl get kdexfunction -n dev \
//	  -o custom-columns=NAME:.metadata.name,OBSERVE:.status.attributes.observe\.error
//
// See kdex-tech/knative-deployer#9.

package main

import (
	"context"
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
)

const (
	// attrObserveError holds the most recent observation failure. Its presence
	// is the signal; its absence means the last pass observed the function
	// cleanly.
	attrObserveError = "observe.error"

	// attrObserveFailingSince is when the CURRENT run of failures started, not
	// when the last one happened. An unchanged error leaves it alone, so it
	// answers "how long has this been broken" -- the question an operator
	// actually asks -- and avoids rewriting status every tick.
	attrObserveFailingSince = "observe.failingSince"
)

// Status.attributes rather than a condition or the state field, deliberately:
//
//   - state is the function's lifecycle (Ready, FunctionDeployed, ...). A failed
//     observation means we do not KNOW the state, so overwriting it would
//     manufacture information we do not have and destroy the last good value.
//   - conditions would be the idiomatic home, but the observer's status write is
//     a JSON merge-patch, under which arrays replace wholesale -- so a naive
//     conditions write would clobber conditions owned by other writers.
//     Attributes is a map, so a merge-patch touches only these keys.
//   - status.attributes already carries comparable per-function bookkeeping (the
//     kpack retry keys in retry.go) and needs no CRD change.
//
// If KDexFunction later grows observer-owned conditions with a
// server-side-apply writer, this is the thing to migrate.

// observeErrorAttr returns the failure currently recorded on a function, or ""
// when it was last observed cleanly.
func observeErrorAttr(kfObj *unstructured.Unstructured) string {
	attrs, _, _ := unstructured.NestedStringMap(kfObj.Object, "status", "attributes")
	return attrs[attrObserveError]
}

// recordObserveFailure writes an observation failure onto the function.
//
// Re-recording an IDENTICAL message is a no-op: it preserves failingSince and
// avoids a status write on every tick for a function that is simply still
// broken, which at the default */5 schedule would be a write per function per
// five minutes forever.
//
// A NotFound is reported as such by the caller: the function was deleted
// between listing and patching, which is benign, not a lost failure.
func recordObserveFailure(
	ctx context.Context,
	client dynamic.Interface,
	kfObj *unstructured.Unstructured,
	obsErr error,
	now time.Time,
) error {
	message := obsErr.Error()
	if observeErrorAttr(kfObj) == message {
		return nil
	}

	patch := map[string]any{
		"status": map[string]any{
			"attributes": map[string]any{
				attrObserveError:        message,
				attrObserveFailingSince: now.UTC().Format(time.RFC3339),
			},
		},
	}
	body, err := encodePatch(patch)
	if err != nil {
		return err
	}

	kfClient := client.Resource(kdexFunctionGVR).Namespace(kfObj.GetNamespace())
	_, err = kfClient.Patch(ctx, kfObj.GetName(), types.MergePatchType, body, metav1.PatchOptions{
		FieldManager: "kdex-knative-observer",
	}, "status")
	return err
}

// clearObserveFailure removes the failure keys once a function observes
// cleanly. Without this a function that failed once looks broken forever.
//
// Idempotent: no write when nothing is recorded, so the steady state of a
// healthy host is zero status writes from this path.
func clearObserveFailure(
	ctx context.Context,
	client dynamic.Interface,
	kfObj *unstructured.Unstructured,
) error {
	attrs, _, _ := unstructured.NestedStringMap(kfObj.Object, "status", "attributes")
	if attrs[attrObserveError] == "" && attrs[attrObserveFailingSince] == "" {
		return nil
	}

	// nil values remove the keys under a JSON merge-patch.
	patch := map[string]any{
		"status": map[string]any{
			"attributes": map[string]any{
				attrObserveError:        nil,
				attrObserveFailingSince: nil,
			},
		},
	}
	body, err := encodePatch(patch)
	if err != nil {
		return err
	}

	kfClient := client.Resource(kdexFunctionGVR).Namespace(kfObj.GetNamespace())
	_, err = kfClient.Patch(ctx, kfObj.GetName(), types.MergePatchType, body, metav1.PatchOptions{
		FieldManager: "kdex-knative-observer",
	}, "status")
	return err
}

// observeOutcome is one pass's result, kept separate from the error return so
// runObserve can report per-function failures without conflating them with a
// failure of the pass itself.
type observeOutcome struct {
	Observed int
	Failed   int
	// Unrecorded counts failures that could NOT be written to their function's
	// status. These are the ones that justify a non-zero exit: exiting zero is
	// only safe while every failure is durably attributed somewhere.
	Unrecorded []string
}

func (o observeOutcome) summary() string {
	return fmt.Sprintf("observed %d function(s), %d failed", o.Observed, o.Failed)
}
