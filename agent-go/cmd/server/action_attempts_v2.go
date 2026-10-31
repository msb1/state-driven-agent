package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/state-driven-agent/agent-go/internal/agent"
)

func writeActionAttempts(ctx context.Context, tx pgx.Tx, state *agent.Session) error {
	for _, r := range state.AttemptsPending {
		evidence, _ := json.Marshal(r["evidence"])
		tag, e := tx.Exec(ctx, `INSERT INTO agent_action_attempts(attempt_id,session_id,execution_id,command_id,attempt_number,kind,status,payload_sha256,owner,fence,started_at,finished_at,evidence) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13) ON CONFLICT(attempt_id) DO UPDATE SET status=EXCLUDED.status,finished_at=EXCLUDED.finished_at,evidence=EXCLUDED.evidence WHERE agent_action_attempts.status='started' AND agent_action_attempts.execution_id=EXCLUDED.execution_id AND agent_action_attempts.command_id=EXCLUDED.command_id AND agent_action_attempts.payload_sha256=EXCLUDED.payload_sha256 AND agent_action_attempts.owner=EXCLUDED.owner AND agent_action_attempts.fence=EXCLUDED.fence`, r["attempt_id"], state.ID, r["execution_id"], r["command_id"], r["attempt_number"], r["kind"], r["status"], r["payload_sha256"], r["owner"], r["fence"], r["started_at"], r["finished_at"], evidence)
		if e != nil {
			return e
		}
		if tag.RowsAffected() != 1 {
			return fmt.Errorf("immutable action attempt changed")
		}
	}
	return nil
}
