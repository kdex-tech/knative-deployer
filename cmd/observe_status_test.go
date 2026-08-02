/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	kerrors "k8s.io/apimachinery/pkg/api/errors"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8stesting "k8s.io/client-go/testing"
)

// functionWithAttrs builds a KDexFunction carrying the given status.attributes.
func functionWithAttrs(attrs map[string]string) *unstructured.Unstructured {
	obj := map[string]any{
		"apiVersion": "kdex.dev/v1alpha1",
		"kind":       "KDexFunction",
		"metadata": map[string]any{
			"name":      testFunc,
			"namespace": testNamespace,
		},
	}
	if attrs != nil {
		converted := map[string]any{}
		for k, v := range attrs {
			converted[k] = v
		}
		obj["status"] = map[string]any{"attributes": converted}
	}
	return &unstructured.Unstructured{Object: obj}
}

func readAttrs(t *testing.T, client dynamic.Interface) map[string]string {
	t.Helper()
	got, err := client.Resource(kdexFunctionGVR).Namespace(testNamespace).
		Get(context.Background(), testFunc, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get function: %v", err)
	}
	attrs, _, _ := unstructured.NestedStringMap(got.Object, "status", "attributes")
	return attrs
}

// TestRecordObserveFailure_WritesAttribution is the core of
// kdex-tech/knative-deployer#9: a per-function failure must land somewhere
// queryable, not only on stderr of an aggregate Job covering N functions.
func TestRecordObserveFailure_WritesAttribution(t *testing.T) {
	kf := functionWithAttrs(nil)
	client := newFakeClient(kf)
	now := time.Date(2026, 8, 2, 12, 0, 0, 0, time.UTC)

	err := recordObserveFailure(context.Background(), client, kf,
		errors.New("failed to get knative service: boom"), now)
	if err != nil {
		t.Fatalf("recordObserveFailure: %v", err)
	}

	attrs := readAttrs(t, client)
	if got := attrs[attrObserveError]; got != "failed to get knative service: boom" {
		t.Errorf("%s = %q", attrObserveError, got)
	}
	if got := attrs[attrObserveFailingSince]; got != "2026-08-02T12:00:00Z" {
		t.Errorf("%s = %q, want the first-failure time", attrObserveFailingSince, got)
	}
}

// TestRecordObserveFailure_UnchangedErrorDoesNotRewrite pins two things at once:
// failingSince must keep meaning "failing SINCE" rather than "failed most
// recently", and a function that is simply still broken must not cost a status
// write every tick (a write per function per 5 minutes, forever).
func TestRecordObserveFailure_UnchangedErrorDoesNotRewrite(t *testing.T) {
	first := time.Date(2026, 8, 2, 12, 0, 0, 0, time.UTC)
	kf := functionWithAttrs(map[string]string{
		attrObserveError:        "boom",
		attrObserveFailingSince: first.Format(time.RFC3339),
	})
	client := newFakeClient(kf)

	var patches int
	client.PrependReactor("patch", "kdexfunctions", func(k8stesting.Action) (bool, runtime.Object, error) {
		patches++
		return false, nil, nil
	})

	later := first.Add(90 * time.Minute)
	if err := recordObserveFailure(context.Background(), client, kf, errors.New("boom"), later); err != nil {
		t.Fatalf("recordObserveFailure: %v", err)
	}

	if patches != 0 {
		t.Errorf("an unchanged error issued %d status write(s); want none", patches)
	}
	if got := readAttrs(t, client)[attrObserveFailingSince]; got != first.Format(time.RFC3339) {
		t.Errorf("failingSince = %q, want it pinned to the first failure %q", got, first.Format(time.RFC3339))
	}
}

// A DIFFERENT error is a new fact and must be written, restamping failingSince.
func TestRecordObserveFailure_ChangedErrorRewrites(t *testing.T) {
	first := time.Date(2026, 8, 2, 12, 0, 0, 0, time.UTC)
	kf := functionWithAttrs(map[string]string{
		attrObserveError:        "boom",
		attrObserveFailingSince: first.Format(time.RFC3339),
	})
	client := newFakeClient(kf)

	later := first.Add(time.Hour)
	if err := recordObserveFailure(context.Background(), client, kf, errors.New("different boom"), later); err != nil {
		t.Fatalf("recordObserveFailure: %v", err)
	}

	attrs := readAttrs(t, client)
	if attrs[attrObserveError] != "different boom" {
		t.Errorf("%s = %q", attrObserveError, attrs[attrObserveError])
	}
	if attrs[attrObserveFailingSince] != later.Format(time.RFC3339) {
		t.Errorf("failingSince = %q, want restamped to %q", attrs[attrObserveFailingSince], later.Format(time.RFC3339))
	}
}

// TestClearObserveFailure_ClearsOnRecovery: without this a function that failed
// once looks broken forever, which #9 calls out explicitly.
func TestClearObserveFailure_ClearsOnRecovery(t *testing.T) {
	kf := functionWithAttrs(map[string]string{
		attrObserveError:        "boom",
		attrObserveFailingSince: "2026-08-02T12:00:00Z",
		// A neighbouring writer's key must survive the clear.
		attrRetries: "2",
	})
	client := newFakeClient(kf)

	if err := clearObserveFailure(context.Background(), client, kf); err != nil {
		t.Fatalf("clearObserveFailure: %v", err)
	}

	attrs := readAttrs(t, client)
	if attrs[attrObserveError] != "" || attrs[attrObserveFailingSince] != "" {
		t.Errorf("failure keys survived the clear: %v", attrs)
	}
	if attrs[attrRetries] != "2" {
		t.Errorf("clear clobbered another writer's attribute: %v", attrs)
	}
}

// A clean function must not be patched at all, so the steady state of a healthy
// host is zero status writes from this path.
func TestClearObserveFailure_NoWriteWhenNothingRecorded(t *testing.T) {
	kf := functionWithAttrs(map[string]string{attrRetries: "1"})
	client := newFakeClient(kf)

	var patches int
	client.PrependReactor("patch", "kdexfunctions", func(k8stesting.Action) (bool, runtime.Object, error) {
		patches++
		return false, nil, nil
	})

	if err := clearObserveFailure(context.Background(), client, kf); err != nil {
		t.Fatalf("clearObserveFailure: %v", err)
	}
	if patches != 0 {
		t.Errorf("clearing a clean function issued %d write(s); want none", patches)
	}
}

// TestRecordObserveFailure_DoesNotTouchState guards the representation choice.
// A failed observation means the state is UNKNOWN, so recording it must not
// overwrite the last good state/url with something invented.
func TestRecordObserveFailure_DoesNotTouchState(t *testing.T) {
	kf := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "kdex.dev/v1alpha1",
		"kind":       "KDexFunction",
		"metadata":   map[string]any{"name": testFunc, "namespace": testNamespace},
		"status": map[string]any{
			"state": "Ready",
			"url":   "https://fn.example.com",
		},
	}}
	client := newFakeClient(kf)

	err := recordObserveFailure(context.Background(), client, kf,
		errors.New("boom"), time.Now())
	if err != nil {
		t.Fatalf("recordObserveFailure: %v", err)
	}

	got, gerr := client.Resource(kdexFunctionGVR).Namespace(testNamespace).
		Get(context.Background(), testFunc, metav1.GetOptions{})
	if gerr != nil {
		t.Fatalf("get: %v", gerr)
	}
	state, _, _ := unstructured.NestedString(got.Object, "status", "state")
	url, _, _ := unstructured.NestedString(got.Object, "status", "url")
	if state != "Ready" {
		t.Errorf("state = %q; a failed observation must not overwrite the last known state", state)
	}
	if url != "https://fn.example.com" {
		t.Errorf("url = %q; a failed observation must not overwrite the last known url", url)
	}
}

// observeTargets builds a set of functions for observePass, returning both the
// slice the loop iterates and the same objects registered with the fake client
// so status patches land somewhere. None have a Knative Service registered, so
// observeFunction treats each as "not deployed yet" -- a clean observation.
func observeTargets(names ...string) ([]unstructured.Unstructured, *dynamicfake.FakeDynamicClient) {
	functions := make([]unstructured.Unstructured, 0, len(names))
	objs := make([]runtime.Object, 0, len(names))
	for _, n := range names {
		fn := unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "kdex.dev/v1alpha1",
			"kind":       "KDexFunction",
			"metadata":   map[string]any{"name": n, "namespace": testNamespace},
		}}
		functions = append(functions, fn)
		objs = append(objs, fn.DeepCopy())
	}
	return functions, newFakeClient(objs...)
}

// TestObservePass_PerFunctionFailureDoesNotFailThePass is the exit-code half of
// kdex-tech/knative-deployer#9, exercised through the real loop.
//
// Before this, any per-function failure returned an aggregated error and the
// CronJob went red. One wedged function then marked EVERY run failed until
// operators stopped reading the signal. Now the failure is recorded on that
// function's own status and the pass reports success, because the pass did run.
func TestObservePass_PerFunctionFailureDoesNotFailThePass(t *testing.T) {
	functions, client := observeTargets("fn-a", "fn-b")

	// Make fn-a's Knative Service Get fail with something that is NOT NotFound,
	// which is how observeFunction reports a real observation failure.
	client.PrependReactor("get", "services", func(action k8stesting.Action) (bool, runtime.Object, error) {
		if action.(k8stesting.GetAction).GetName() == "fn-a" {
			return true, nil, errors.New("etcdserver: request timed out")
		}
		return false, nil, nil
	})

	err := observePass(context.Background(), client, functions, &EnvConfig{
		FunctionNamespace: testNamespace,
		FunctionHost:      testHost,
	}, time.Now())

	if err != nil {
		t.Fatalf("a per-function failure must not fail the pass (#9); got: %v", err)
	}

	// ...and it must be attributed, which is what makes exiting zero safe.
	got, gerr := client.Resource(kdexFunctionGVR).Namespace(testNamespace).
		Get(context.Background(), "fn-a", metav1.GetOptions{})
	if gerr != nil {
		t.Fatalf("get fn-a: %v", gerr)
	}
	attrs, _, _ := unstructured.NestedStringMap(got.Object, "status", "attributes")
	if attrs[attrObserveError] == "" {
		t.Error("the failure exited zero without being recorded anywhere — the exact regression #9 guards against")
	}
}

// TestObservePass_UnrecordableFailureFailsThePass pins the one exception:
// exiting zero is only defensible while every failure lands somewhere durable,
// so losing that makes it a real, actionable Job failure.
func TestObservePass_UnrecordableFailureFailsThePass(t *testing.T) {
	functions, client := observeTargets("fn-a")

	client.PrependReactor("get", "services", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("etcdserver: request timed out")
	})
	// The status write itself fails, with something other than NotFound.
	client.PrependReactor("patch", "kdexfunctions", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("conflict")
	})

	err := observePass(context.Background(), client, functions, &EnvConfig{
		FunctionNamespace: testNamespace,
		FunctionHost:      testHost,
	}, time.Now())

	if err == nil {
		t.Fatal("a failure that could not be recorded must fail the pass — otherwise it surfaces nowhere at all")
	}
	if !strings.Contains(err.Error(), "could not be recorded") {
		t.Errorf("error should name the lost attribution; got: %v", err)
	}
}

// A function deleted between listing and patching is benign, not lost
// attribution: there is no longer anything to attribute the failure to.
func TestObservePass_DeletedFunctionDoesNotFailThePass(t *testing.T) {
	functions, client := observeTargets("fn-a")

	client.PrependReactor("get", "services", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("etcdserver: request timed out")
	})
	client.PrependReactor("patch", "kdexfunctions", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, kerrors.NewNotFound(
			schema.GroupResource{Group: "kdex.dev", Resource: "kdexfunctions"}, "fn-a")
	})

	err := observePass(context.Background(), client, functions, &EnvConfig{
		FunctionNamespace: testNamespace,
		FunctionHost:      testHost,
	}, time.Now())

	if err != nil {
		t.Fatalf("a function deleted mid-pass must not fail the pass; got: %v", err)
	}
}

// TestObservePass_OneFailureDoesNotSkipTheRest re-pins the no-fail-fast
// guarantee from #6 now that the loop has been restructured: an early return
// here would strand every function after the first failure.
func TestObservePass_OneFailureDoesNotSkipTheRest(t *testing.T) {
	functions, client := observeTargets("fn-a", "fn-b", "fn-c")

	observed := map[string]bool{}
	client.PrependReactor("get", "services", func(action k8stesting.Action) (bool, runtime.Object, error) {
		name := action.(k8stesting.GetAction).GetName()
		observed[name] = true
		if name == "fn-a" {
			return true, nil, errors.New("etcdserver: request timed out")
		}
		return false, nil, nil
	})

	if err := observePass(context.Background(), client, functions, &EnvConfig{
		FunctionNamespace: testNamespace,
		FunctionHost:      testHost,
	}, time.Now()); err != nil {
		t.Fatalf("observePass: %v", err)
	}

	for _, n := range []string{"fn-a", "fn-b", "fn-c"} {
		if !observed[n] {
			t.Errorf("%s was never observed — a failure earlier in the set skipped it", n)
		}
	}
}
