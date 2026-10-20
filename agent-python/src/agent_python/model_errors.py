"""Safe model failure diagnostics, shared by runtime and benchmark reports.

Provider bodies are classified, not copied into public session/SSE errors.
"""
from __future__ import annotations

import httpx


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
