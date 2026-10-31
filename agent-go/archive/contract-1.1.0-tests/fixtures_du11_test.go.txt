package main

import (
	"strings"
	"testing"

	"github.com/state-driven-agent/agent-go/internal/agent"
)

func TestDU11C02FixtureToolsMatchSharedSerializationAndValidation(t *testing.T) {
	s, ctx := du05Repository(t)
	for _, ddl := range []string{
		`CREATE TABLE agent_fixture_access_logs(line_number integer primary key,line text not null)`,
		`CREATE TABLE agent_fixture_customers(id integer primary key,name text,city text)`,
		`CREATE TABLE agent_fixture_orders(order_id integer primary key,cust_id text,amount numeric(12,2),ordered_at date)`,
		`CREATE TABLE agent_fixture_employees(emp_id integer primary key,name text,age text,salary text,city text)`,
	} {
		if _, err := s.db.Exec(ctx, ddl); err != nil {
			t.Fatal(err)
		}
	}
	for _, q := range []string{
		`INSERT INTO agent_fixture_access_logs VALUES (1,'10.0.0.1 a b c d e f 500'),(2,'10.0.0.2 a b c d e f 200'),(3,'10.0.0.1 a b c d e f 500')`,
		`INSERT INTO agent_fixture_customers VALUES (1,'A','Chicago'),(2,'B','chicago')`,
		`INSERT INTO agent_fixture_orders VALUES (1,'ID_001',10.25,'2026-01-01'),(2,'ID_002',20.00,'2026-02-01')`,
		`INSERT INTO agent_fixture_employees VALUES (1,'One','30','90000','New York'),(2,'Two','40','95000','new york'),(3,'Three','NULL','$80,000','Chicago'),(4,'Four','41; extra_field_corrupted','UNKNOWN','Chicago')`,
	} {
		if _, err := s.db.Exec(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	tools := agent.PostgresTools(s.db)
	call := func(name string, args map[string]any) string {
		t.Helper()
		v, err := tools[name](ctx, args)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		return v
	}
	if got := call("inspect_log", map[string]any{"start_line": 1, "line_count": 1}); got != `1: 10.0.0.1 a b c d e f 500` {
		t.Fatalf("inspect_log=%q", got)
	}
	if got := call("analyze_500_ips", map[string]any{"robust_parser": true}); got != `{"top": [["10.0.0.1", 2]], "malformed_records_ignored": 10}` {
		t.Fatalf("analyze output=%s", got)
	}
	if got := call("validate_top_offending_ip", map[string]any{"ip": "10.0.0.1", "count": 2}); !strings.Contains(got, `"valid": true`) {
		t.Fatalf("log validation=%s", got)
	}
	if got := call("inspect_sql_schema", map[string]any{}); !strings.Contains(got, `"chicago_examples"`) || !strings.Contains(got, `"string_key_examples"`) {
		t.Fatalf("schema output missing examples: %s", got)
	}
	if got := call("query_chicago_q1_revenue", map[string]any{"normalize_keys": true, "normalize_city": true}); got != `{"total": 30.25, "normalize_keys": true, "normalize_city": true}` {
		t.Fatalf("revenue=%s", got)
	}
	if got := call("validate_chicago_q1_revenue", map[string]any{"total": 30.25}); !strings.Contains(got, `"valid": true`) {
		t.Fatalf("revenue validation=%s", got)
	}
	// The shared Python tool samples employee rows at positions 9, 10, 15, 22,
	// and 28. This focused fixture has only four rows, so it should return an
	// empty examples array while retaining the actual record count.
	if got := call("inspect_employee_audit_schema", map[string]any{}); !strings.Contains(got, `"record_count": 4`) || !strings.Contains(got, `"examples": []`) {
		t.Fatalf("employee schema/examples=%s", got)
	}
	for parser, marker := range map[string]string{"naive": "salary='$80,000'", "currency_cleaned": "age='NULL'", "null_cleaned": "41; extra_field_corrupted'"} {
		if got := call("audit_employee_phase_1", map[string]any{"parser": parser}); !strings.Contains(got, marker) || !strings.Contains(got, "Traceback (most recent call last):") {
			t.Fatalf("%s diagnostic=%s", parser, got)
		}
	}
	phase1 := call("audit_employee_phase_1", map[string]any{"parser": "robust"})
	if !strings.Contains(phase1, `"corrupted_row_ids": [3, 4]`) {
		t.Fatalf("phase1=%s", phase1)
	}
	median := call("calculate_new_york_median_salary", map[string]any{"normalize_city": true, "exclude_invalid_salary": true})
	if median != `{"phase": "Phase 2", "median_salary": 92500.0, "valid_new_york_employees": 2, "status": "calculated"}` {
		t.Fatalf("median=%s", median)
	}
	report := call("generate_employee_phase_3_report", map[string]any{"normalize_city": true, "drop_invalid_age": true})
	if !strings.HasPrefix(report, "Phase 3 — clean city audit\n| City |") || !strings.Contains(report, "normalization-check 48:") {
		t.Fatalf("report missing shared table/trace")
	}
	validation := call("validate_employee_audit", map[string]any{"phase_1_logged": true, "normalize_city": true, "drop_invalid_age": true, "median_salary": 92500.0})
	if !strings.Contains(validation, `"valid": true`) {
		t.Fatalf("audit validation=%s", validation)
	}
}
