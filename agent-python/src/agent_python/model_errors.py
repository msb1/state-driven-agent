"""Bounded public model errors and redacted operator-only provider logs."""
from __future__ import annotations

import json
import logging
import re

import httpx


_logger = logging.getLogger('agent_python.model_provider')
_SAFE_FIELD = re.compile(r'^[A-Za-z0-9_.\[\]/:-]{1,160}$')
_SECRET_ASSIGNMENT = re.compile(
    r'(?i)\b(authorization|api[_-]?key|access[_-]?token|refresh[_-]?token|password|secret)\b\s*[:=]\s*[^\s,;]+'
)
_BEARER = re.compile(r'(?i)\bbearer\s+[A-Za-z0-9._~+/-]+=*')
_TOKEN = re.compile(r'(?i)\b(?:sk|pk|rk|xox[baprs])-[-A-Za-z0-9_]{8,}\b')
_URL = re.compile(r'https?://[^\s\])}>]+', re.IGNORECASE)
_REQUEST_ECHO = re.compile(r'(?i)\b(messages?|prompt|request|body|content)\b.*$')
_QUOTED = tuple(re.compile(pattern, re.DOTALL) for pattern in (
    r'"[^"\r\n]{0,2000}"', r"'[^'\r\n]{0,2000}'", r'`[^`\r\n]{0,2000}`'))


def _safe_provider_field(value):
    if not isinstance(value, str):
        return None
    value = value.strip()
    return value[:160] if _SAFE_FIELD.fullmatch(value) else None


def _safe_provider_message(value):
    if not isinstance(value, str):
        return None
    value = ' '.join(value.split())
    for pattern in _QUOTED:
        value = pattern.sub('[REDACTED]', value)
    value = _REQUEST_ECHO.sub(lambda match: f'{match.group(1)}=[REDACTED]', value)
    value = _SECRET_ASSIGNMENT.sub(lambda match: f'{match.group(1)}=[REDACTED]', value)
    value = _BEARER.sub('Bearer [REDACTED]', value)
    value = _TOKEN.sub('[TOKEN REDACTED]', value)
    value = _URL.sub('[URL REDACTED]', value)
    value = ''.join(char for char in value if char.isprintable())
    return value[:500] or None


def log_provider_failure(status, body, request_id=None):
    """Write bounded, redacted provider fields to the operator-only server log."""
    error = body.get('error') if isinstance(body, dict) else None
    event = {'event': 'model_provider_failure', 'http_status': status}
    if isinstance(error, dict):
        fields = {
            'type': _safe_provider_field(error.get('type')),
            'code': _safe_provider_field(error.get('code')),
            'param': _safe_provider_field(error.get('param')),
            'message': _safe_provider_message(error.get('message')),
        }
        event['provider_error'] = {key: value for key, value in fields.items() if value is not None}
    elif isinstance(error, str):
        message = _safe_provider_message(error)
        if message:
            event['provider_error'] = {'message': message}
    if request_id:
        event['provider_request_id'] = _safe_provider_field(request_id)
        if event['provider_request_id'] is None:
            event.pop('provider_request_id')
    _logger.warning('%s', json.dumps(event, ensure_ascii=True, separators=(',', ':')))


class ModelServiceError(Exception):
    def __init__(self, code: str, message: str, *, http_status=None, category='provider', details=None):
        super().__init__(message)
        self.diagnostic = {'code': code, 'message': message, 'recoverable': True,
                           'category': category, 'http_status': http_status, 'details': details or {}}


def classify_exchange(status, body):
    """Return bounded labels only, never raw provider bodies or credentials."""
    proxy_type = body.get('proxy_transport_error') if isinstance(body, dict) else None
    if proxy_type:
        timeout = isinstance(proxy_type, str) and 'Timeout' in proxy_type
        return {'code': 'model_timeout' if timeout else 'model_transport_error',
                'category': 'timeout' if timeout else 'transport', 'http_status': status,
                'message': 'Upstream model request timed out.' if timeout else 'Upstream model transport failed.',
                'details': {'source': 'benchmark_proxy', 'exception_type': proxy_type if proxy_type in {
                    'ReadTimeout', 'ConnectTimeout', 'WriteTimeout', 'PoolTimeout', 'ConnectError', 'ReadError', 'WriteError', 'RemoteProtocolError'} else 'HTTPError'}}
    if status is not None and status >= 400:
        error = body.get('error') if isinstance(body, dict) else None
        text = error.get('message', '') if isinstance(error, dict) else error if isinstance(error, str) else ''
        if 'single tool-call' in text.lower() or 'single tool call' in text.lower():
            return {'code': 'model_template_error', 'category': 'template', 'http_status': status,
                    'message': 'Model prompt template rejects multiple tool calls. Configure single-call wire history or use a compatible template.',
                    'details': {'reason': 'multiple_tool_calls_unsupported'}}
        if 'jinja' in text.lower() or 'rendering prompt' in text.lower():
            return {'code': 'model_template_error', 'category': 'template', 'http_status': status,
                    'message': 'Model prompt template could not render the request.', 'details': {'reason': 'prompt_render_failed'}}
        return {'code': 'model_http_error', 'category': 'provider', 'http_status': status,
                'message': f'Model service returned HTTP {status}.', 'details': {}}
    return None


def exception_diagnostic(error):
    if isinstance(error, ModelServiceError):
        return error.diagnostic
    if isinstance(error, httpx.HTTPStatusError):
        try:
            body = error.response.json()
        except ValueError:
            body = None
        return {**classify_exchange(error.response.status_code, body), 'recoverable': True}
    if isinstance(error, httpx.TimeoutException):
        return {'code': 'model_timeout', 'category': 'timeout', 'http_status': None,
                'message': 'Model request timed out.', 'details': {'exception_type': type(error).__name__}, 'recoverable': True}
    if isinstance(error, httpx.RequestError):
        return {'code': 'model_transport_error', 'category': 'transport', 'http_status': None,
                'message': 'Model transport failed.', 'details': {'exception_type': type(error).__name__}, 'recoverable': True}
    return {'code': 'invalid_model_response', 'category': 'response', 'http_status': None,
            'message': 'Model service returned invalid response data.', 'details': {}, 'recoverable': True}
