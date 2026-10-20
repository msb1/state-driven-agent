package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"math"
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
			firstSeen := map[string]int{}
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
						if _, ok := firstSeen[m[1]]; !ok {
							firstSeen[m[1]] = line
						}
						counts[m[1]]++
					}
				} else {
					f := strings.Split(x, " ")
					if len(f) <= 7 {
						return fmt.Sprintf("ERROR: naive parser failed at line %d: IndexError: list index out of range. Try robust_parser=true.", line), nil
					}
					if f[7] == "500" {
						if _, ok := firstSeen[f[0]]; !ok {
							firstSeen[f[0]] = line
						}
						counts[f[0]]++
					}
				}
			}
			return topJSON(counts, firstSeen, robust), nil
		},
		"validate_top_offending_ip": func(ctx context.Context, a map[string]any) (string, error) {
			// Preserve the fixture-only unit adapter used by the transport tests.
			// Live tools always receive a database pool and derive the answer from it.
			if db == nil {
				ok := fmt.Sprint(a["ip"]) == "10.0.0.5" && integer(a, "count") == 83
				return fmt.Sprintf(`{"valid": %t, "message": %q}`, ok, map[bool]string{true: "Validated.", false: "Incorrect; inspect parsing and retry."}[ok]), nil
			}
			expected, e := topLogResult(ctx, db)
			if e != nil {
				return "", e
			}
			ok := fmt.Sprint(a["ip"]) == expected[0] && integer(a, "count") == expected[1]
			return fmt.Sprintf(`{"valid": %t, "message": %q}`, ok, map[bool]string{true: "Validated.", false: "Incorrect; inspect parsing and retry."}[ok]), nil
		},
		"query_chicago_q1_revenue": func(ctx context.Context, a map[string]any) (string, error) {
			keys, _ := a["normalize_keys"].(bool)
			city, _ := a["normalize_city"].(bool)
			condition := "c.city = 'Chicago'"
			if city {
				condition = "lower(c.city) = 'chicago'"
			}
			var total float64
			keyExpr := "o.cust_id::INTEGER"
			if keys {
				keyExpr = "CAST(REPLACE(o.cust_id, 'ID_', '') AS INTEGER)"
			}
			e := db.QueryRow(ctx, "SELECT ROUND(COALESCE(SUM(o.amount),0),2) FROM agent_fixture_orders o JOIN agent_fixture_customers c ON c.id="+keyExpr+" WHERE "+condition+" AND o.ordered_at >= '2026-01-01' AND o.ordered_at < '2026-04-01'").Scan(&total)
			if e != nil {
				return "", e
			}
			return fmt.Sprintf(`{"total": %s, "normalize_keys": %t, "normalize_city": %t}`, pythonFloat(total), keys, city), nil
		},
		"validate_chicago_q1_revenue": func(ctx context.Context, a map[string]any) (string, error) {
			v, ok := number(a["total"])
			if db == nil {
				valid := ok && math.Round(v*100) == math.Round(271017.77*100)
				return fmt.Sprintf(`{"valid": %t, "expected_hint": %q}`, valid, map[bool]string{true: "Validated.", false: "Normalize city case and ID_ foreign keys."}[valid]), nil
			}
			expectedText, err := queryChicago(ctx, db, true, true)
			if err != nil {
				return "", err
			}
			var expected struct {
				Total float64 `json:"total"`
			}
			if err = json.Unmarshal([]byte(expectedText), &expected); err != nil {
				return "", err
			}
			valid := ok && math.Round(v*100) == math.Round(expected.Total*100)
			return fmt.Sprintf(`{"valid": %t, "expected_hint": %q}`, valid, map[bool]string{true: "Validated.", false: "Normalize city case and ID_ foreign keys."}[valid]), nil
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
			return fmt.Sprintf(`{"phase": "Phase 2", "median_salary": %.1f, "valid_new_york_employees": %d, "status": "calculated"}`, median, n), nil
		},
		"inspect_sql_schema": func(ctx context.Context, _ map[string]any) (string, error) {
			return inspectSQLSchema(ctx, db)
		},
		"inspect_employee_audit_schema": func(ctx context.Context, _ map[string]any) (string, error) {
			var n int
			e := db.QueryRow(ctx, "SELECT count(*) FROM agent_fixture_employees").Scan(&n)
			if e != nil {
				return "", e
			}
			rows, e := db.Query(ctx, "SELECT emp_id,name,age,salary,city FROM (SELECT emp_id,name,age,salary,city,row_number() OVER (ORDER BY emp_id) AS row_position FROM agent_fixture_employees) employees WHERE row_position IN (9,10,15,22,28) ORDER BY row_position")
			if e != nil {
				return "", e
			}
			defer rows.Close()
			examples := []string{}
			for rows.Next() {
				var id int
				var name, age, salary, city string
				if e = rows.Scan(&id, &name, &age, &salary, &city); e != nil {
					return "", e
				}
				examples = append(examples, fmt.Sprintf("[%d, %q, %q, %q, %q]", id, name, age, salary, city))
			}
			if e = rows.Err(); e != nil {
				return "", e
			}
			return fmt.Sprintf(`{"schema": {"table": "employees", "columns": ["emp_id INTEGER", "name TEXT", "age TEXT", "salary TEXT", "city TEXT"]}, "record_count": %d, "examples": [%s], "rules": "age is invalid when NULL or semicolon-delimited; salary is invalid when currency-formatted or UNKNOWN; city matching is case-insensitive."}`, n, strings.Join(examples, ", ")), nil
		},
		"audit_employee_phase_1": func(ctx context.Context, a map[string]any) (string, error) {
			p := fmt.Sprint(a["parser"])
			if p != "robust" {
				failure := map[string]struct {
					row          int
					field, value string
				}{"naive": {10, "salary", "$80,000"}, "currency_cleaned": {15, "age", "NULL"}, "null_cleaned": {22, "age", "41; extra_field_corrupted"}}
				if f, ok := failure[p]; ok {
					return employeeFailure(f.row, f.field, f.value), nil
				}
				return "ERROR: parser must be naive, currency_cleaned, null_cleaned, or robust.", nil
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
			union := map[int]bool{}
			for _, id := range age {
				union[id] = true
			}
			for _, id := range salary {
				union[id] = true
			}
			all := make([]int, 0, len(union))
			for id := range union {
				all = append(all, id)
			}
			sort.Ints(all)
			return fmt.Sprintf(`{"phase": "Phase 1", "corrupted_age_ids": %s, "corrupted_salary_ids": %s, "corrupted_row_ids": %s, "status": "logged"}`, pythonStyleJSON(age), pythonStyleJSON(salary), pythonStyleJSON(all)), nil
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
			rows, e := db.Query(ctx, "SELECT initcap(lower(city)),count(*),avg(age::integer) FROM agent_fixture_employees WHERE age <> 'NULL' AND age !~ ';' GROUP BY 1 ORDER BY 1")
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
				if e = rows.Scan(&c, &n, &avg); e != nil {
					return "", e
				}
				fmt.Fprintf(&b, "| %s | %d | %.2f |\n", c, n, avg)
			}
			if e = rows.Err(); e != nil {
				return "", e
			}
			b.WriteString("\nPhase 3 normalization trace (diagnostic evidence):\n")
			for i := 1; i <= 48; i++ {
				fmt.Fprintf(&b, "normalization-check %02d: city=lowercase, age=NULL-or-semicolon excluded, salary=UNKNOWN-or-currency excluded, aggregate=verified", i)
				if i < 48 {
					b.WriteByte('\n')
				}
			}
			return b.String(), nil
		},
		"validate_employee_audit": func(ctx context.Context, a map[string]any) (string, error) {
			m, _ := number(a["median_salary"])
			ok := a["phase_1_logged"] == true && a["normalize_city"] == true && a["drop_invalid_age"] == true
			if db == nil {
				ok = ok && m == 92500
				return fmt.Sprintf(`{"valid": %t, "expected_median_salary": 92500.0}`, ok), nil
			}
			var expected float64
			if err := db.QueryRow(ctx, "WITH x AS (SELECT salary::numeric v FROM agent_fixture_employees WHERE lower(city)='new york' AND salary <> 'UNKNOWN' AND salary !~ '^\\$') SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY v) FROM x").Scan(&expected); err != nil {
				return "", err
			}
			ok = ok && math.Round(m*100) == math.Round(expected*100)
			return fmt.Sprintf(`{"valid": %t, "expected_median_salary": %s, "hint": %q}`, ok, pythonFloat(expected), map[bool]string{true: "Validated all three phases.", false: "Log Phase 1, normalize city casing, and drop invalid ages."}[ok]), nil
		},
	}
}
func integer(a map[string]any, k string) int { v, _ := number(a[k]); return int(v) }
func number(x any) (float64, bool) {
	switch v := x.(type) {
	case float64:
		return v, true
	case int64:
		return float64(v), true
	case int:
		return float64(v), true
	case json.Number:
		q, e := v.Float64()
		return q, e == nil
	}
	return 0, false
}
func topJSON(c map[string]int, first map[string]int, robust bool) string {
	type pair struct {
		IP string
		N  int
	}
	p := []pair{}
	for ip, n := range c {
		p = append(p, pair{ip, n})
	}
	sort.Slice(p, func(i, j int) bool {
		if p[i].N == p[j].N {
			return first[p[i].IP] < first[p[j].IP]
		}
		return p[i].N > p[j].N
	})
	if len(p) > 5 {
		p = p[:5]
	}
	out := make([][]any, len(p))
	for i, x := range p {
		out[i] = []any{x.IP, x.N}
	}
	return fmt.Sprintf(`{"top": %s, "malformed_records_ignored": %d}`, pythonStyleJSON(out), map[bool]int{true: 10, false: 0}[robust])
}
func verboseFailure(s string) string {
	var b strings.Builder
	b.WriteString(s + "\nMetric test runner context:\n")
	for i := 1; i <= 32; i++ {
		fmt.Fprintf(&b, "  assertion-frame %02d: candidate=[raw salary values]; city predicate and invalid-value filter under test", i)
		b.WriteByte('\n')
	}
	b.WriteString("Revise the stated normalization rule and retry the metric.")
	return b.String()
}

func employeeFailure(row int, field, value string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "ERROR: Phase 1 parser crashed at employee %d; %s='%s'.\n", row, field, value)
	b.WriteString("Traceback (most recent call last):\n  File \"/simulated/audit.py\", line 84, in parse_employee_batch\n")
	fmt.Fprintf(&b, "    normalized_%s = %s_parser(raw_%s)\n  File \"/simulated/audit.py\", line 39, in %s_parser\n", field, field, field, field)
	fmt.Fprintf(&b, "    raise ValueError('unsupported %s representation')\nValueError: unsupported %s representation '%s'\n", field, field, value)
	b.WriteString("Parser window (not a successful audit):\n")
	for i := row - 8; i < row+8; i++ {
		if i < 1 {
			continue
		}
		fmt.Fprintf(&b, "  %4d | parsed_%s[%d] = %s_parser(record[%d])\n", i, field, i, field, i)
	}
	b.WriteString("Revise only the parser assumption exposed by this failure, then retry incrementally.")
	return strings.TrimSuffix(b.String(), "\n")
}

func intArray(values []int) string { b, _ := json.Marshal(values); return string(b) }
func compactString(value any) string {
	var b strings.Builder
	e := json.NewEncoder(&b)
	e.SetEscapeHTML(false)
	if e.Encode(value) != nil {
		return "null"
	}
	return strings.NewReplacer("\\u2028", "\u2028", "\\u2029", "\u2029").Replace(strings.TrimSuffix(b.String(), "\n"))
}
func pythonFloat(value float64) string { // Python repr keeps a decimal marker for integral floats.
	s := fmt.Sprintf("%.15g", value)
	if !strings.ContainsAny(s, ".eE") {
		s += ".0"
	}
	return s
}

func topLogResult(ctx context.Context, db *pgxpool.Pool) ([2]any, error) {
	rows, err := db.Query(ctx, "SELECT line FROM agent_fixture_access_logs ORDER BY line_number")
	if err != nil {
		return [2]any{}, err
	}
	defer rows.Close()
	re := regexp.MustCompile(`^(?P<ip>(?:\d{1,3}\.){3}\d{1,3})\s+.*?\s(?P<status>\d{3})$`)
	counts := map[string]int{}
	first := map[string]int{}
	lineNo := 0
	for rows.Next() {
		lineNo++
		var line string
		if err = rows.Scan(&line); err != nil {
			return [2]any{}, err
		}
		m := re.FindStringSubmatch(line)
		if m != nil && m[2] == "500" {
			if _, ok := first[m[1]]; !ok {
				first[m[1]] = lineNo
			}
			counts[m[1]]++
		}
	}
	if err = rows.Err(); err != nil {
		return [2]any{}, err
	}
	best := ""
	for ip, n := range counts {
		if best == "" || n > counts[best] || n == counts[best] && first[ip] < first[best] {
			best = ip
		}
	}
	if best == "" {
		return [2]any{}, fmt.Errorf("fixture contains no valid HTTP 500 record")
	}
	return [2]any{best, counts[best]}, nil
}

func queryChicago(ctx context.Context, db *pgxpool.Pool, keys, city bool) (string, error) {
	keyExpr := "o.cust_id::INTEGER"
	if keys {
		keyExpr = "CAST(REPLACE(o.cust_id, 'ID_', '') AS INTEGER)"
	}
	condition := "c.city = 'Chicago'"
	if city {
		condition = "lower(c.city) = 'chicago'"
	}
	var total float64
	err := db.QueryRow(ctx, "SELECT ROUND(COALESCE(SUM(o.amount),0),2) FROM agent_fixture_orders o JOIN agent_fixture_customers c ON c.id="+keyExpr+" WHERE "+condition+" AND o.ordered_at >= '2026-01-01' AND o.ordered_at < '2026-04-01'").Scan(&total)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`{"total": %s, "normalize_keys": %t, "normalize_city": %t}`, pythonFloat(total), keys, city), nil
}

func inspectSQLSchema(ctx context.Context, db *pgxpool.Pool) (string, error) {
	type column struct {
		Name string `json:"column_name"`
		Type string `json:"data_type"`
	}
	// Use ordered information_schema values and text casts to preserve Python's JSON-compatible tuple rows.
	customers := [][]string{}
	orders := [][]string{}
	for _, item := range []struct {
		table  string
		target *[][]string
	}{{"agent_fixture_customers", &customers}, {"agent_fixture_orders", &orders}} {
		rows, err := db.Query(ctx, "SELECT column_name,data_type FROM information_schema.columns WHERE table_name=$1 ORDER BY ordinal_position", item.table)
		if err != nil {
			return "", err
		}
		for rows.Next() {
			var a, b string
			if err = rows.Scan(&a, &b); err != nil {
				rows.Close()
				return "", err
			}
			*item.target = append(*item.target, []string{a, b})
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return "", err
		}
	}
	chicago := [][]any{}
	rows, err := db.Query(ctx, "SELECT id,name,city FROM agent_fixture_customers WHERE lower(city)='chicago' LIMIT 6")
	if err != nil {
		return "", err
	}
	for rows.Next() {
		var id int
		var name, city string
		if err = rows.Scan(&id, &name, &city); err != nil {
			rows.Close()
			return "", err
		}
		chicago = append(chicago, []any{id, name, city})
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return "", err
	}
	bad := [][]any{}
	rows, err = db.Query(ctx, "SELECT order_id,cust_id,amount::text,ordered_at::text FROM agent_fixture_orders WHERE cust_id LIKE 'ID_%' LIMIT 6")
	if err != nil {
		return "", err
	}
	for rows.Next() {
		var id int
		var cust, amount, dt string
		if err = rows.Scan(&id, &cust, &amount, &dt); err != nil {
			rows.Close()
			return "", err
		}
		bad = append(bad, []any{id, cust, amount, dt})
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return "", err
	}
	value := struct {
		Schema  map[string]any `json:"schema"`
		Chicago [][]any        `json:"chicago_examples"`
		Keys    [][]any        `json:"string_key_examples"`
	}{map[string]any{"customers": customers, "orders": orders}, chicago, bad}
	return pythonStyleJSON(value), nil
}

// pythonStyleJSON reproduces json.dumps' default separator spacing for fixture text.
func pythonStyleJSON(value any) string {
	compact := compactString(value)
	var b strings.Builder
	quoted := false
	escaped := false
	for _, r := range compact {
		if quoted {
			if r > 0x7f {
				if r <= 0xffff {
					fmt.Fprintf(&b, `\u%04x`, r)
				} else {
					x := r - 0x10000
					fmt.Fprintf(&b, `\u%04x\u%04x`, 0xd800+(x>>10), 0xdc00+(x&0x3ff))
				}
			} else {
				b.WriteRune(r)
			}
			if escaped {
				escaped = false
			} else if r == '\\' {
				escaped = true
			} else if r == '"' {
				quoted = false
			}
			continue
		}
		if r == '"' {
			quoted = true
			b.WriteRune(r)
			continue
		}
		b.WriteRune(r)
		if r == ',' || r == ':' {
			b.WriteByte(' ')
		}
	}
	return b.String()
}
