CREATE TABLE agent_fixture_access_logs (line_number INTEGER PRIMARY KEY, line TEXT NOT NULL);
CREATE TABLE agent_fixture_customers (id INTEGER PRIMARY KEY, name TEXT NOT NULL, city TEXT NOT NULL);
CREATE TABLE agent_fixture_orders (
    order_id INTEGER PRIMARY KEY, cust_id TEXT NOT NULL, amount NUMERIC(12,2) NOT NULL, ordered_at DATE NOT NULL
);
CREATE TABLE agent_fixture_employees (
    emp_id INTEGER PRIMARY KEY, name TEXT NOT NULL, age TEXT NOT NULL, salary TEXT NOT NULL, city TEXT NOT NULL
);
CREATE INDEX agent_fixture_orders_customer_date ON agent_fixture_orders(cust_id,ordered_at);
CREATE INDEX agent_fixture_employees_city ON agent_fixture_employees(city);
CREATE TABLE agent_fixture_idempotency_vectors (
    vector_id TEXT PRIMARY KEY, namespace TEXT NOT NULL,
    session_id UUID NOT NULL, execution_id UUID NOT NULL,
    action_type TEXT NOT NULL, target TEXT NOT NULL, arguments JSONB NOT NULL,
    expected_key TEXT NOT NULL CHECK (expected_key ~ '^[0-9a-f]{64}$'),
    expected_payload_sha256 TEXT NOT NULL CHECK (expected_payload_sha256 ~ '^[0-9a-f]{64}$'),
    canonical_key TEXT NOT NULL, canonical_payload TEXT NOT NULL
);
CREATE TABLE agent_fixture_action_receipts (
    namespace TEXT NOT NULL, idempotency_key TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    request JSONB NOT NULL, result JSONB NOT NULL,
    effect_count INTEGER NOT NULL DEFAULT 1 CHECK (effect_count = 1),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY(namespace,idempotency_key)
);
