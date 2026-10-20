package agent

import (
	"context"
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
