"""Formatting normalization preserves content and rejects incomplete replacements."""
import unittest
from agent_python.context import validate_ledger


class CompactionNormalization(unittest.TestCase):
    def proposal(self, goal='Reworded goal', lessons='<lesson>Observed 10 records.</lesson><lesson>Normalize city names.</lesson>'):
        return ('<COMPACTED_STATE><USER_GOAL>'+goal+'</USER_GOAL>'
            '<GLOBAL_LESSON_LEDGER>'+lessons+'</GLOBAL_LESSON_LEDGER>'
            '<DEAD_ENDS><failure>Case-sensitive matching failed.</failure></DEAD_ENDS>'
            '<CURRENT_LOCAL_PIVOT><pivot>Verify normalized results.</pivot></CURRENT_LOCAL_PIVOT></COMPACTED_STATE>')

    def test_fenced_nested_summary_becomes_canonical_plain_text_with_immutable_goal(self):
        proposal = '```xml\n'+self.proposal()+'\n```'
        ledger, canonical = validate_ledger(proposal,'Original goal & constraints','10 records',4096)
        self.assertEqual(ledger.goal,'Original goal & constraints')
        self.assertEqual(ledger.model_claims,['Observed 10 records.','Normalize city names.'])
        self.assertNotIn('<lesson>',canonical)
        self.assertNotIn('```',canonical)
        again, rendered = validate_ledger(canonical,ledger.goal,'10 records',4096)
        self.assertEqual(rendered,canonical)
        self.assertEqual(again.model_claims,ledger.model_claims)

    def test_missing_duplicate_truncated_and_unsupported_content_rejected(self):
        good = self.proposal()
        for bad in (good[:-20], good.replace('</DEAD_ENDS>','</DEAD_ENDS><DEAD_ENDS>duplicate</DEAD_ENDS>'),
                    good.replace('<CURRENT_LOCAL_PIVOT>','<UNKNOWN>'),
                    '<!DOCTYPE x>'+good, good.replace('<lesson>','<lesson attr="x">'),
                    'extra prose\n'+good, '```xml\n'+good):
            with self.subTest(bad=bad[:80]), self.assertRaises(ValueError):
                validate_ledger(bad,'Original goal','10 records',4096)

    def test_unsupported_numbers_and_empty_reflection_still_rejected(self):
        for lessons in ('<lesson>Observed 999 records.</lesson>', '<lesson></lesson>'):
            with self.assertRaises(ValueError):
                validate_ledger(self.proposal(lessons=lessons),'Original goal','10 records',4096)
