package agent

import (
	"github.com/google/uuid"
	"time"
)

func stageActionAttempt(s *Session, x *ToolExecution, kind, status string, evidence map[string]any) {
	now := time.Now().UTC().Truncate(time.Microsecond)
	if status == "started" {
		x.AttemptID = uuid.NewString()
		x.AttemptNumber++
		x.AttemptStartedAt = &now
		x.AttemptKind = kind
	}
	if x.AttemptID == "" {
		return
	}
	owner := "00000000-0000-0000-0000-000000000000"
	fence := int64(0)
	if s.ExecutionOwner != nil {
		owner = *s.ExecutionOwner
	}
	if s.ExecutionFence != nil {
		fence = *s.ExecutionFence
	}
	var started any = x.AttemptStartedAt
	for _, r := range s.AttemptsPending {
		if r["attempt_id"] == x.AttemptID {
			started = r["started_at"]
		}
	}
	row := map[string]any{"attempt_id": x.AttemptID, "session_id": s.ID, "execution_id": x.ExecutionID, "command_id": s.CommandID, "attempt_number": x.AttemptNumber, "kind": kind, "status": status, "payload_sha256": x.PayloadSHA256, "owner": owner, "fence": fence, "started_at": started, "finished_at": nil, "evidence": evidence}
	if status != "started" {
		row["finished_at"] = now
	}
	replaced := false
	for i, prior := range s.AttemptsPending {
		if prior["attempt_id"] == x.AttemptID {
			s.AttemptsPending[i] = row
			replaced = true
			break
		}
	}
	if !replaced {
		s.AttemptsPending = append(s.AttemptsPending, row)
	}
	record := Clone(row)
	delete(record, "session_id")
	label := "tool_attempt"
	if status != "started" {
		label = "tool_attempt_settled"
	}
	ArchiveRecord(s, label, record)
}
