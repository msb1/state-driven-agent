package agent

import (
	"context"
	"fmt"
	"sync"
)

// TransientToolError is the only adapter error that authorizes a bounded retry.
type TransientToolError struct{ Message string }

func (e TransientToolError) Error() string { return e.Message }

type IdempotentTool func(context.Context, map[string]any, string) (string, error)
type ToolReconciler func(context.Context, map[string]any, string) (string, bool, error)

var recoveryAdapters = struct {
	sync.RWMutex
	idempotent map[string]IdempotentTool
	reconcile  map[string]ToolReconciler
}{idempotent: map[string]IdempotentTool{}, reconcile: map[string]ToolReconciler{}}

// RegisterIdempotentTool registers an adapter that must honor the persisted key.
// The returned function unregisters this exact registration for test cleanup.
func RegisterIdempotentTool(name string, adapter IdempotentTool) func() {
	recoveryAdapters.Lock()
	recoveryAdapters.idempotent[name] = adapter
	recoveryAdapters.Unlock()
	return func() {
		recoveryAdapters.Lock()
		delete(recoveryAdapters.idempotent, name)
		recoveryAdapters.Unlock()
	}
}

func RegisterToolReconciler(name string, reconciler ToolReconciler) func() {
	recoveryAdapters.Lock()
	recoveryAdapters.reconcile[name] = reconciler
	recoveryAdapters.Unlock()
	return func() {
		recoveryAdapters.Lock()
		delete(recoveryAdapters.reconcile, name)
		recoveryAdapters.Unlock()
	}
}

func recoveryAdapter(name string) (IdempotentTool, ToolReconciler) {
	recoveryAdapters.RLock()
	defer recoveryAdapters.RUnlock()
	return recoveryAdapters.idempotent[name], recoveryAdapters.reconcile[name]
}

// IdempotencyCapability is a trusted adapter declaration, never supplied by a model.
// Nil RetentionSeconds is an explicit indefinite retention guarantee.
type IdempotencyCapability struct {
	Target, KeyScope string
	RetentionSeconds *float64
}
type Verification struct {
	Outcome           string
	Evidence          map[string]any
	Result            string
	ExternalReference string
}
type ActionVerifier func(context.Context, map[string]any, string) (Verification, error)

var durableAdapters = struct {
	sync.RWMutex
	capabilities map[string]IdempotencyCapability
	verifiers    map[string]ActionVerifier
}{capabilities: map[string]IdempotencyCapability{}, verifiers: map[string]ActionVerifier{}}

func RegisterDurableIdempotentTool(name string, capability IdempotencyCapability, adapter IdempotentTool) func() {
	if capability.Target == "" || capability.KeyScope == "" || (capability.RetentionSeconds != nil && *capability.RetentionSeconds <= 0) {
		panic("invalid trusted idempotency capability")
	}
	undo := RegisterIdempotentTool(name, adapter)
	durableAdapters.Lock()
	durableAdapters.capabilities[name] = capability
	durableAdapters.Unlock()
	return func() {
		undo()
		durableAdapters.Lock()
		delete(durableAdapters.capabilities, name)
		durableAdapters.Unlock()
	}
}
func RegisterActionVerifier(name string, verify ActionVerifier) func() {
	durableAdapters.Lock()
	durableAdapters.verifiers[name] = verify
	durableAdapters.Unlock()
	return func() { durableAdapters.Lock(); delete(durableAdapters.verifiers, name); durableAdapters.Unlock() }
}
func actionCapability(name string) (IdempotencyCapability, bool) {
	durableAdapters.RLock()
	defer durableAdapters.RUnlock()
	capability, ok := durableAdapters.capabilities[name]
	return capability, ok
}

func actionVerifier(name string) ActionVerifier {
	durableAdapters.RLock()
	defer durableAdapters.RUnlock()
	return durableAdapters.verifiers[name]
}
func validateVerification(v Verification) error {
	if len(v.Evidence) == 0 {
		return fmt.Errorf("verification requires authoritative evidence")
	}
	switch v.Outcome {
	case "succeeded", "failed":
		if (v.Outcome == "failed") != (resultStatus(v.Result) == "failed") {
			return fmt.Errorf("verification result status mismatch")
		}
	case "not_executed", "unknown":
	default:
		return fmt.Errorf("invalid verification outcome")
	}
	return nil
}
