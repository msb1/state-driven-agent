package main

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// This transaction double records whether admission issued a session write.
// It never connects to PostgreSQL.
type admissionTx struct {
	pgx.Tx
	status        string
	fence         int64
	reads, writes int
}

type admissionRow struct{ scan func(...any) error }

func (r admissionRow) Scan(values ...any) error { return r.scan(values...) }

func (tx *admissionTx) QueryRow(_ context.Context, sql string, _ ...any) pgx.Row {
	return admissionRow{scan: func(values ...any) error {
		if strings.HasPrefix(sql, "SELECT") {
			if !strings.Contains(sql, "FOR UPDATE") {
				panic("admission read must lock the session")
			}
			tx.reads++
			*values[0].(*string) = tx.status
		} else {
			tx.writes++
			tx.fence++
			*values[0].(*int64) = tx.fence
		}
		return nil
	}}
}

func TestTerminalRunAdmissionDoesNotChangeSessionFence(t *testing.T) {
	for _, status := range []string{"complete", "failed"} {
		t.Run(status, func(t *testing.T) {
			tx := &admissionTx{status: status, fence: 2}
			_, err := acquireLocked(context.Background(), tx, uuid.NewString(), uuid.New(), 30, true)
			if !errors.Is(err, ErrTerminalSession) || tx.writes != 0 || tx.fence != 2 || tx.reads != 1 {
				t.Fatalf("terminal rejection changed authority: err=%v reads=%d writes=%d fence=%d", err, tx.reads, tx.writes, tx.fence)
			}
		})
	}
}

func TestActiveRunAndTerminalResumeCanAcquire(t *testing.T) {
	for _, trial := range []struct {
		status     string
		activeOnly bool
	}{{"active", true}, {"complete", false}, {"failed", false}} {
		tx := &admissionTx{status: trial.status, fence: 2}
		fence, err := acquireLocked(context.Background(), tx, uuid.NewString(), uuid.New(), 30, trial.activeOnly)
		if err != nil || fence != 3 || tx.writes != 1 {
			t.Fatalf("admission status=%s activeOnly=%v: fence=%d writes=%d err=%v", trial.status, trial.activeOnly, fence, tx.writes, err)
		}
	}
}

func TestDiagnosticPageEntryMatchesPythonContract(t *testing.T) {
	encoded, err := json.Marshal(diagnosticPageEntry{Ordinal: 4, Payload: map[string]any{"reason": "workflow_complete"}})
	if err != nil {
		t.Fatal(err)
	}
	var actual map[string]any
	if err = json.Unmarshal(encoded, &actual); err != nil {
		t.Fatal(err)
	}
	if len(actual) != 2 || actual["ordinal"] != float64(4) || actual["payload"] == nil {
		t.Fatalf("diagnostic entry must expose ordinal and payload: %s", encoded)
	}
	if actual["payload"].(map[string]any)["reason"] != "workflow_complete" {
		t.Fatal("payload changed")
	}
}
