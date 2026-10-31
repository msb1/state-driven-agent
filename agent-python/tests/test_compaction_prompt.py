"""Compaction instructions avoid the literal placeholders observed in failed runs."""
import unittest
from types import SimpleNamespace
from xml.etree import ElementTree as ET

from agent_python.llm import LlmClient
from agent_python.context import validate_ledger


class CompactionPrompt(unittest.TestCase):
    def client(self):
        client = LlmClient.__new__(LlmClient)
        client.config = SimpleNamespace(model='model', memory=SimpleNamespace(
            max_summary_tokens=4096, summary_output_tokens=512))
        client.compaction_feedback = None
        return client

    def test_goal_with_xml_characters_is_supplied_as_complete_element(self):
        goal = 'Audit A & B; preserve <records> exactly.'
        payload = self.client().compaction_payload(goal, [], None)
        prompt = payload['messages'][0]['content']
        start = prompt.index('<USER_GOAL>')
        end = prompt.index('</USER_GOAL>') + len('</USER_GOAL>')
        element = prompt[start:end]
        self.assertEqual(ET.fromstring(element).text, goal)
        self.assertNotIn('...', prompt)
        proposal = '<COMPACTED_STATE>' + element + (
            '<GLOBAL_LESSON_LEDGER>- None.</GLOBAL_LESSON_LEDGER>'
            '<DEAD_ENDS>- None.</DEAD_ENDS>'
            '<CURRENT_LOCAL_PIVOT>Inspect records.</CURRENT_LOCAL_PIVOT></COMPACTED_STATE>')
        ledger, _ = validate_ledger(proposal, goal, goal, 4096)
        self.assertEqual(ledger.goal, goal)

    def test_retry_explains_complete_replacement_and_preserves_validation(self):
        client = self.client()
        client.compaction_feedback = 'Summary must preserve the original goal exactly and populate each section'
        prompt = client.compaction_payload('Audit records.', [], None)['messages'][0]['content']
        self.assertIn('Generate a complete corrected replacement', prompt)
        bad = ('<COMPACTED_STATE><USER_GOAL>...</USER_GOAL>'
               '<GLOBAL_LESSON_LEDGER>Observed failure.</GLOBAL_LESSON_LEDGER>'
               '<DEAD_ENDS>- None.</DEAD_ENDS><CURRENT_LOCAL_PIVOT>...</CURRENT_LOCAL_PIVOT></COMPACTED_STATE>')
        with self.assertRaisesRegex(ValueError, 'populate every reflection section'):
            validate_ledger(bad, 'Audit records.', '', 4096)
        with self.assertRaisesRegex(ValueError, 'one complete COMPACTED_STATE'):
            validate_ledger(bad[:-18], 'Audit records.', '', 4096)
