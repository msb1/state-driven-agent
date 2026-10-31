package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"os"
)

func RegisterFixtureRecipient(db *pgxpool.Pool) {
	RegisterDurableIdempotentTool("fixture_idempotent_write", IdempotencyCapability{Target: "fixture://action-receipts", KeyScope: "deployment-namespace"}, func(ctx context.Context, args map[string]any, key string) (string, error) {
		fingerprint, e := DigestV2([]any{"agent-payload-v1", "fixture_idempotent_write", "fixture://action-receipts", args})
		if e != nil {
			return "", e
		}
		result, e := CanonicalV2(map[string]any{"effect_id": key, "value": args["value"]})
		if e != nil {
			return "", e
		}
		request, e := CanonicalV2(args)
		if e != nil {
			return "", e
		}
		tx, e := db.Begin(ctx)
		if e != nil {
			return "", e
		}
		defer tx.Rollback(ctx)
		_, e = tx.Exec(ctx, `INSERT INTO agent_fixture_action_receipts(namespace,idempotency_key,payload_sha256,request,result) VALUES($1,$2,$3,$4,$5) ON CONFLICT(namespace,idempotency_key) DO NOTHING`, os.Getenv("AGENT_DEPLOYMENT_NAMESPACE"), key, fingerprint, request, result)
		if e != nil {
			return "", e
		}
		var hash string
		var prior []byte
		if e = tx.QueryRow(ctx, `SELECT payload_sha256,result FROM agent_fixture_action_receipts WHERE namespace=$1 AND idempotency_key=$2 FOR UPDATE`, os.Getenv("AGENT_DEPLOYMENT_NAMESPACE"), key).Scan(&hash, &prior); e != nil {
			return "", e
		}
		if hash != fingerprint {
			return "", fmt.Errorf("recipient key identifies different payload")
		}
		if e = tx.Commit(ctx); e != nil {
			return "", e
		}
		var value any
		if e = json.Unmarshal(prior, &value); e != nil {
			return "", e
		}
		canonical, e := CanonicalV2(value)
		return string(canonical), e
	})
	RegisterToolReconciler("fixture_idempotent_write", func(ctx context.Context, args map[string]any, key string) (string, bool, error) {
		hash, e := DigestV2([]any{"agent-payload-v1", "fixture_idempotent_write", "fixture://action-receipts", args})
		if e != nil {
			return "", false, e
		}
		var stored string
		var result []byte
		e = db.QueryRow(ctx, `SELECT payload_sha256,result FROM agent_fixture_action_receipts WHERE namespace=$1 AND idempotency_key=$2`, os.Getenv("AGENT_DEPLOYMENT_NAMESPACE"), key).Scan(&stored, &result)
		if e == pgx.ErrNoRows {
			return "", false, nil
		}
		if e != nil {
			return "", false, e
		}
		if hash != stored {
			return "", false, fmt.Errorf("recipient payload mismatch")
		}
		var v any
		e = json.Unmarshal(result, &v)
		if e != nil {
			return "", false, e
		}
		b, e := CanonicalV2(v)
		return string(b), e == nil, e
	})
}
