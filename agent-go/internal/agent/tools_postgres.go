package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"regexp"
	"sort"
	"strings"
)

// PostgresTools exposes the deterministic shared fixtures without any SQLite dependency.
func PostgresTools(db *pgxpool.Pool) map[string]Tool {
	return map[string]Tool{
		"inspect_log": func(ctx context.Context, a map[string]any) (string, error) {
			start := integer(a, "start_line")
			count := integer(a, "line_count")
			rows, e := db.Query(ctx, "SELECT line_number,line FROM agent_fixture_access_logs WHERE line_number >= $1 ORDER BY line_number LIMIT $2", start, count)
			if e != nil {
				return "", e
			}
			defer rows.Close()
			var b strings.Builder
			for rows.Next() {
				var n int
				var line string
				if e = rows.Scan(&n, &line); e != nil {
					return "", e
				}
				fmt.Fprintf(&b, "%d: %s\n", n, line)
			}
			return strings.TrimSuffix(b.String(), "\n"), rows.Err()
		},
		"analyze_500_ips": func(ctx context.Context, a map[string]any) (string, error) {
			rows, e := db.Query(ctx, "SELECT line FROM agent_fixture_access_logs ORDER BY line_number")
			if e != nil {
				return "", e
			}
			defer rows.Close()
			counts := map[string]int{}
			robust, _ := a["robust_parser"].(bool)
			line := 0
			re := regexp.MustCompile(`^(?P<ip>(?:\d{1,3}\.){3}\d{1,3})\s+.*?\s(?P<status>\d{3})$`)
			for rows.Next() {
				line++
				var x string
				rows.Scan(&x)
				if robust {
					m := re.FindStringSubmatch(x)
					if m == nil {
						continue
					}
					if m[2] == "500" {
						counts[m[1]]++
					}
				} else {
					f := strings.Split(x, " ")
					if len(f) <= 7 {
						return fmt.Sprintf("ERROR: naive parser failed at line %d: IndexError: list index out of range. Try robust_parser=true.", line), nil
					}
					if f[7] == "500" {
						counts[f[0]]++
					}
				}
			}
			return topJSON(counts, robust), nil
		},
		"validate_top_offending_ip": func(_ context.Context, a map[string]any) (string, error) {
			ok := fmt.Sprint(a["ip"]) == "10.0.0.5" && integer(a, "count") == 83
			return fmt.Sprintf(`{"valid":%t,"message":%q}`, ok, map[bool]string{true: "Validated.", false: "Incorrect; inspect parsing and retry."}[ok]), nil
		},
		"query_chicago_q1_revenue": func(ctx context.Context, a map[string]any) (string, error) {
			keys, _ := a["normalize_keys"].(bool)
			city, _ := a["normalize_city"].(bool)
			if !keys {
				return `{"total":0,"normalize_keys":false,"normalize_city":false}`, nil
			}
			condition := "c.city = 'Chicago'"
			if city {
				condition = "lower(c.city) = 'chicago'"
			}
			var total float64
			e := db.QueryRow(ctx, "SELECT COALESCE(SUM(o.amount),0) FROM agent_fixture_orders o JOIN agent_fixture_customers c ON c.id=CAST(REPLACE(o.cust_id,'ID_','') AS INTEGER) WHERE "+condition+" AND o.ordered_at >= DATE '2026-01-01' AND o.ordered_at < DATE '2026-04-01'").Scan(&total)
			if e != nil {
				return "", e
			}
			return fmt.Sprintf(`{"total":%.2f,"normalize_keys":%t,"normalize_city":%t}`, total, keys, city), nil
		},
		"validate_chicago_q1_revenue": func(_ context.Context, a map[string]any) (string, error) {
			v, ok := number(a["total"])
			valid := ok && fmt.Sprintf("%.2f", v) == "271017.77"
			return fmt.Sprintf(`{"valid":%t,"expected_hint":%q}`, valid, map[bool]string{true: "Validated.", false: "Normalize city case and ID_ foreign keys."}[valid]), nil
		},
		"calculate_new_york_median_salary": func(ctx context.Context, a map[string]any) (string, error) {
			city, _ := a["normalize_city"].(bool)
			salary, _ := a["exclude_invalid_salary"].(bool)
			if !salary {
				return verboseFailure("AssertionError: salary array contains UNKNOWN; filter invalid salary fields before median()."), nil
			}
			if !city {
				return verboseFailure("AssertionError: Expected median 92500, but your script calculated 71000. Match city case-insensitively."), nil
			}
			var n int
			var median float64
			e := db.QueryRow(ctx, "WITH x AS (SELECT salary::numeric v FROM agent_fixture_employees WHERE lower(city)='new york' AND salary <> 'UNKNOWN' AND salary !~ '^\\$') SELECT COUNT(*), percentile_cont(0.5) WITHIN GROUP (ORDER BY v) FROM x").Scan(&n, &median)
			if e != nil {
				return "", e
			}
			return fmt.Sprintf(`{"phase":"Phase 2","median_salary":%.1f,"valid_new_york_employees":%d,"status":"calculated"}`, median, n), nil
		},
		"inspect_sql_schema": func(context.Context, map[string]any) (string, error) {
			return `{"schema":{"customers":"id/name/city","orders":"order_id/cust_id/amount/ordered_at"},"rules":"normalize city case and ID_ foreign keys"}`, nil
		},
		"inspect_employee_audit_schema": func(ctx context.Context, _ map[string]any) (string, error) {
			var n int
			e := db.QueryRow(ctx, "SELECT count(*) FROM agent_fixture_employees").Scan(&n)
			return fmt.Sprintf(`{"schema":{"table":"employees","columns":["emp_id INTEGER","name TEXT","age TEXT","salary TEXT","city TEXT"]},"record_count":%d,"rules":"age is invalid when NULL or semicolon-delimited; salary is invalid when currency-formatted or UNKNOWN; city matching is case-insensitive."}`, n), e
		},
		"audit_employee_phase_1": func(ctx context.Context, a map[string]any) (string, error) {
			p := fmt.Sprint(a["parser"])
			if p != "robust" {
				m := map[string]string{"naive": "$80,000", "currency_cleaned": "NULL", "null_cleaned": "41; extra_field_corrupted"}
				return "ERROR: Phase 1 parser crashed; " + m[p], nil
			}
			rows, e := db.Query(ctx, "SELECT emp_id,age,salary FROM agent_fixture_employees ORDER BY emp_id")
			if e != nil {
				return "", e
			}
			defer rows.Close()
			age, salary := []int{}, []int{}
			for rows.Next() {
				var id int
				var x, y string
				rows.Scan(&id, &x, &y)
				if x == "NULL" || strings.Contains(x, ";") {
					age = append(age, id)
				}
				if y == "UNKNOWN" || strings.HasPrefix(y, "$") {
					salary = append(salary, id)
				}
			}
			b, _ := json.Marshal(map[string]any{"phase": "Phase 1", "corrupted_age_ids": age, "corrupted_salary_ids": salary, "status": "logged"})
			return string(b), nil
		},
		"generate_employee_phase_3_report": func(ctx context.Context, a map[string]any) (string, error) {
			city, _ := a["normalize_city"].(bool)
			age, _ := a["drop_invalid_age"].(bool)
			if !city {
				return "ERROR: Phase 3 would split New York and new york. Set normalize_city=true.", nil
			}
			if !age {
				return "ERROR: Phase 3 cannot average age while NULL and semicolon-corrupted ages remain. Set drop_invalid_age=true.", nil
			}
			rows, e := db.Query(ctx, "SELECT initcap(lower(city)),count(*),round(avg(age::integer),2) FROM agent_fixture_employees WHERE age <> 'NULL' AND age !~ ';' GROUP BY 1 ORDER BY 1")
			if e != nil {
				return "", e
			}
			defer rows.Close()
			var b strings.Builder
			b.WriteString("Phase 3 — clean city audit\n| City | Valid headcount | Average age |\n| --- | ---: | ---: |\n")
			for rows.Next() {
				var c string
				var n int
				var avg float64
				rows.Scan(&c, &n, &avg)
				fmt.Fprintf(&b, "| %s | %d | %.2f |\n", c, n, avg)
			}
			return b.String(), nil
		},
		"validate_employee_audit": func(_ context.Context, a map[string]any) (string, error) {
			m, _ := number(a["median_salary"])
			ok := a["phase_1_logged"] == true && a["normalize_city"] == true && a["drop_invalid_age"] == true && m == 92500
			return fmt.Sprintf(`{"valid":%t,"expected_median_salary":92500.0}`, ok), nil
		},
	}
}
func integer(a map[string]any, k string) int { v, _ := number(a[k]); return int(v) }
func number(x any) (float64, bool) {
	switch v := x.(type) {
	case float64:
		return v, true
	case int:
		return float64(v), true
	case json.Number:
		q, e := v.Float64()
		return q, e == nil
	}
	return 0, false
}
func topJSON(c map[string]int, robust bool) string {
	type pair struct {
		IP string
		N  int
	}
	p := []pair{}
	for ip, n := range c {
		p = append(p, pair{ip, n})
	}
	sort.Slice(p, func(i, j int) bool { return p[i].N > p[j].N })
	if len(p) > 5 {
		p = p[:5]
	}
	out := make([][]any, len(p))
	for i, x := range p {
		out[i] = []any{x.IP, x.N}
	}
	b, _ := json.Marshal(map[string]any{"top": out, "malformed_records_ignored": map[bool]int{true: 10, false: 0}[robust]})
	return string(b)
}
func verboseFailure(s string) string {
	return s + "\nMetric test runner context:\n" + strings.Repeat("  assertion-frame: candidate=[raw salary values]\n", 32) + "Revise the stated normalization rule and retry the metric."
}
