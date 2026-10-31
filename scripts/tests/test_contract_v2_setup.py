"""Offline checks for the contract-2 setup inputs; no PostgreSQL or agent calls."""
import hashlib
import importlib.util
import json
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'scripts'))
from idempotency_contract import canonical, strict_json, vectors
from schema_layout import tables

spec = importlib.util.spec_from_file_location('fixture_generation', ROOT / 'agent-python/scripts/generate_fixtures.py')
fixtures = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fixtures)


class SetupInputs(unittest.TestCase):
    def test_fixed_identity_vectors_and_intent_boundaries(self):
        data = vectors()
        actions = {v['vector_id']: v for v in data['actions']}
        original, retry, changed = (actions[key] for key in ['original', 'retry_reordered', 'changed_payload'])
        self.assertEqual(original['expected_key'], retry['expected_key'])
        self.assertEqual(original['expected_payload_sha256'], retry['expected_payload_sha256'])
        self.assertEqual(original['expected_key'], changed['expected_key'])
        self.assertNotEqual(original['expected_payload_sha256'], changed['expected_payload_sha256'])
        for name in ['second_intent', 'namespace_isolation']:
            self.assertNotEqual(original['expected_key'], actions[name]['expected_key'])
        self.assertEqual(data['commands'][0]['expected_sha256'], data['commands'][1]['expected_sha256'])
        self.assertNotEqual(data['commands'][0]['expected_sha256'], data['commands'][2]['expected_sha256'])

    def test_canonical_types_numbers_and_strict_input(self):
        self.assertEqual(canonical({'b': 1.0, 'a': -0.0}), b'{"a":0,"b":1}')
        self.assertNotEqual(canonical(True), canonical(1))
        self.assertNotEqual(canonical('é'), canonical('e\u0301'))
        for raw in ['{"x":1,"x":2}', '{"x":NaN}', '{"x":1e999}', '{"x":9007199254740993}', '"\\ud800"']:
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                strict_json(raw)

    def test_repeatable_business_fixtures_match_pinned_contents(self):
        first = fixtures.business_rows()
        self.assertEqual(first, fixtures.business_rows())
        expected = json.loads((ROOT / 'schema/fixture-oracles.json').read_text())['tables']
        actual = {}
        for table, rows in first.items():
            digest = hashlib.sha256()
            for row in rows:
                digest.update(canonical(list(row)) + b'\n')
            actual[table] = {'count': len(rows), 'sha256': digest.hexdigest()}
        self.assertEqual(actual, expected)

    def test_layout_manifest_matches_active_ddl_and_recovery_links(self):
        manifest = json.loads((ROOT / 'schema/layout-v6.json').read_text())
        self.assertEqual(len(tables('postgres.sql')), 12)
        self.assertEqual(len(tables('fixtures.sql')), 6)
        for definition, expected in manifest.items():
            source = (ROOT / 'schema' / definition).read_bytes()
            self.assertEqual(hashlib.sha256(source).hexdigest(), expected['source_sha256'])
            self.assertEqual(set(tables(definition)), set(expected['tables']))
        attempts = manifest['postgres.sql']['tables']['agent_action_attempts']
        self.assertIn(['command_id', 'uuid', True, None], attempts[0])
        self.assertTrue(any(c[0] == 'agent_action_attempts_session_id_command_id_fkey' for c in attempts[1]))


class RecoveryResults(unittest.TestCase):
    def test_verification_cannot_label_failure_as_success(self):
        from agent_python.recovery import Verification
        for outcome,result in [('succeeded','ERROR: failure'),('failed','apparently successful')]:
            with self.subTest(outcome=outcome), self.assertRaises(ValueError):
                Verification(outcome,{'authoritative':True},result)


if __name__ == '__main__':
    unittest.main()
