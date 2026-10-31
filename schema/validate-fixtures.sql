SELECT l.line_number,l.line,c.id,c.name,c.city,o.order_id,o.cust_id,o.amount,o.ordered_at,
    e.emp_id,e.name,e.age,e.salary,e.city,
    v.vector_id,v.namespace,v.session_id,v.execution_id,v.action_type,v.target,v.arguments,
    v.expected_key,v.expected_payload_sha256,v.canonical_key,v.canonical_payload,
    r.namespace,r.idempotency_key,r.payload_sha256,r.request,r.result,r.effect_count,r.created_at
FROM agent_fixture_access_logs l
CROSS JOIN agent_fixture_customers c
CROSS JOIN agent_fixture_orders o
CROSS JOIN agent_fixture_employees e
CROSS JOIN agent_fixture_idempotency_vectors v
CROSS JOIN agent_fixture_action_receipts r
WHERE FALSE;
