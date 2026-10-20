package main

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestDU11C03SharedFixtureFingerprints(t *testing.T) {
	if err := loadDotEnv(); err != nil {
		t.Fatal(err)
	}
	dsn := os.Getenv("AGENT_FIXTURE_DATABASE_URL")
	if dsn == "" {
		dsn = os.Getenv("AGENT_SESSION_DATABASE_URL")
	}
	if dsn == "" {
		t.Skip("set AGENT_FIXTURE_DATABASE_URL or AGENT_SESSION_DATABASE_URL to qualify the shared fixture database")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	db, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var database, schema string
	if err = db.QueryRow(ctx, "SELECT current_database(),current_schema()").Scan(&database, &schema); err != nil {
		t.Fatal(err)
	}
	t.Logf("DU11_C03_DATABASE database=%s schema=%s", database, schema)
	tables := []struct {
		name, key string
		want      int
	}{
		{"agent_fixture_access_logs", "line_number", 650},
		{"agent_fixture_customers", "id", 2200},
		{"agent_fixture_orders", "order_id", 6502},
		{"agent_fixture_employees", "emp_id", 1000},
	}
	for _, table := range tables {
		rows, err := db.Query(ctx, fmt.Sprintf("SELECT to_jsonb(t)::text FROM %s t ORDER BY %s", table.name, table.key))
		if err != nil {
			t.Fatalf("%s: %v", table.name, err)
		}
		h := sha256.New()
		count := 0
		for rows.Next() {
			var row string
			if err = rows.Scan(&row); err != nil {
				rows.Close()
				t.Fatal(err)
			}
			h.Write([]byte(row))
			h.Write([]byte("\n"))
			count++
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			t.Fatal(err)
		}
		fingerprint := fmt.Sprintf("%x", h.Sum(nil))
		t.Logf("DU11_C03_FIXTURE table=%s rows=%d sha256=%s", table.name, count, fingerprint)
		if count != table.want {
			t.Errorf("%s row count=%d want %d", table.name, count, table.want)
		}
		if strings.TrimSpace(database) == "" || strings.TrimSpace(schema) == "" {
			t.Fatal("database identity must be present without logging a connection string")
		}
	}
}
