"""Provider error visibility boundaries for contract 2.0.0."""
import json
import unittest

import httpx

from agent_python.config import ConfigRepository
from agent_python.llm import LlmClient
from agent_python.model_errors import classify_exchange, log_provider_failure
from agent_python.model_errors import ModelServiceError


class ProviderErrorLogging(unittest.IsolatedAsyncioTestCase):
    def test_operator_log_has_redacted_fields_and_public_error_stays_bounded(self):
        body = {'error': {
            'type': 'invalid_request_error',
            'code': 'invalid_tool_schema',
            'param': 'tools[0].function.parameters',
            'message': 'Invalid schema; api_key=sk-secret-value, Bearer abc.def. '
                       'See https://internal.example/debug and "private prompt".'}}

        with self.assertLogs('agent_python.model_provider', level='WARNING') as captured:
            log_provider_failure(400, body, 'req_12345')

        logged = captured.records[0].getMessage()
        event = json.loads(logged)
        self.assertEqual(event['event'], 'model_provider_failure')
        self.assertEqual(event['provider_error']['type'], 'invalid_request_error')
        self.assertEqual(event['provider_error']['code'], 'invalid_tool_schema')
        self.assertEqual(event['provider_error']['param'], 'tools[0].function.parameters')
        for secret in ['sk-secret-value', 'abc.def', 'https://internal.example', 'private prompt']:
            self.assertNotIn(secret, logged)

        public = classify_exchange(400, body)
        self.assertEqual(public['code'], 'model_http_error')
        self.assertEqual(public['message'], 'Model service returned HTTP 400.')
        self.assertNotIn('provider_error', public)
        self.assertNotIn('invalid_tool_schema', json.dumps(public))
        self.assertNotIn('private prompt', json.dumps(public))

    def test_message_echoing_request_content_is_redacted(self):
        with self.assertLogs('agent_python.model_provider', level='WARNING') as captured:
            log_provider_failure(400, {'error': {'message': 'messages[2].content: confidential request text'}})
        logged = captured.records[0].getMessage()
        self.assertNotIn('confidential request text', logged)
        self.assertIn('messages=[REDACTED]', logged)

    async def test_model_client_logs_details_without_returning_them(self):
        config = ConfigRepository().resolve('log-analysis.yaml').config
        client = LlmClient(config)
        body = {'error': {'type': 'invalid_request_error', 'code': 'bad_request',
                          'message': 'api_key=sk-private-value at https://private.example'}}
        client._transport = httpx.MockTransport(lambda request: httpx.Response(
            400, json=body, headers={'x-request-id': 'req_54321'}))
        payload = {'model': config.model, 'messages': [{'role': 'user', 'content': 'safe'}],
                   'tool_choice': 'auto', 'max_tokens': 8}

        with self.assertLogs('agent_python.model_provider', level='WARNING') as captured:
            with self.assertRaises(ModelServiceError) as raised:
                await client._post(payload)

        self.assertEqual(raised.exception.diagnostic['code'], 'model_http_error')
        self.assertNotIn('private-value', json.dumps(raised.exception.diagnostic))
        self.assertNotIn('private.example', json.dumps(raised.exception.diagnostic))
        logged = captured.records[0].getMessage()
        self.assertIn('bad_request', logged)
        self.assertIn('req_54321', logged)
        self.assertNotIn('private-value', logged)
        self.assertNotIn('private.example', logged)


if __name__ == '__main__':
    unittest.main()
