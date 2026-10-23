"""Spark transport and default LLM port contracts; no external network."""
import asyncio
import base64
import hashlib
import hmac
import json
import os
import pathlib
import sys
import unittest
from datetime import datetime
from unittest.mock import Mock, patch
from urllib.parse import parse_qs, urlsplit

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
os.environ.setdefault('DATABASE_URL', 'sqlite+aiosqlite:///:memory:')
import regression_env
import modules.ai.spark_transport as transport
from common.llm import configure
from services import interview_agent, interview_planner


class SparkTransportTests(unittest.TestCase):
    def call(self, frames):
        socket = Mock()
        socket.recv.side_effect = [json.dumps(f) for f in frames]
        with patch.object(transport, '_spark_ok', True), patch.object(transport._ws, 'create_connection', return_value=socket) as connect:
            client = transport.SparkAPI()
            client.app_id, client.api_key, client.api_secret = 'app', 'key', 'secret'
            result = client.chat('prompt')
        return result, socket, connect

    def test_signature(self):
        with patch.object(transport, 'datetime') as clock:
            clock.utcnow.return_value = datetime(2026, 1, 2, 3, 4, 5)
            url = transport._make_url('example.com', '/v1/x1', 'key', 'secret')
        parts = urlsplit(url)
        query = parse_qs(parts.query)
        self.assertEqual((parts.scheme, parts.netloc, parts.path), ('wss', 'example.com', '/v1/x1'))
        date = 'Fri, 02 Jan 2026 03:04:05 GMT'
        self.assertEqual(query['date'], [date])
        self.assertEqual(query['host'], ['example.com'])
        signature = base64.b64encode(hmac.new(b'secret', f'host: example.com\ndate: {date}\nGET /v1/x1 HTTP/1.1'.encode(), hashlib.sha256).digest()).decode()
        self.assertEqual(base64.b64decode(query['authorization'][0]).decode(), f'api_key="key", algorithm="hmac-sha256", headers="host date request-line", signature="{signature}"')

    def test_frames_and_request(self):
        result, socket, connect = self.call([
            {'header': {'status': 0}, 'payload': {'choices': {'text': [{'reasoning_content': 'private'}]}}},
            {'header': {'status': 1}, 'payload': {'choices': {'text': [{'content': 'hello '}]}}},
            {'header': {'status': 2}, 'payload': {'choices': {'text': [{'content': 'world'}]}}},
        ])
        self.assertEqual(result, 'hello world')
        self.assertEqual(connect.call_args.kwargs, {'timeout': 60})
        self.assertEqual(json.loads(socket.send.call_args.args[0]), {
            'header': {'app_id': 'app', 'uid': 'user_001'},
            'parameter': {'chat': {'domain': 'x1', 'temperature': 0.5, 'max_tokens': 2048}},
            'payload': {'message': {'text': [{'role': 'user', 'content': 'prompt'}]}},
        })
        socket.close.assert_called_once()

    def test_reasoning_fallback_and_empty(self):
        result, _, _ = self.call([{'header': {'status': 2}, 'payload': {'choices': {'text': [{'reasoning_content': ' fallback '}]}}}])
        self.assertEqual(result, 'fallback')
        result, _, _ = self.call([{'header': {'status': 2}}])
        self.assertEqual(result, 'AI 回答为空')

    def test_vendor_error(self):
        result, socket, _ = self.call([{'header': {'code': 100, 'message': 'denied'}}])
        self.assertEqual(result, 'API调用失败: denied')
        socket.close.assert_called_once()

    def test_missing_dependency_and_connection_error(self):
        with patch.object(transport, '_spark_ok', False):
            self.assertEqual(transport.SparkAPI().chat('x'), 'SparkAPI not available')
        with patch.object(transport, '_spark_ok', True), patch.object(transport._ws, 'create_connection', side_effect=RuntimeError('offline')):
            self.assertEqual(transport.SparkAPI().chat('x'), 'API调用失败: offline')

    def test_async_bridge(self):
        client = transport.SparkAPI()
        with patch.object(client, 'chat', return_value='reply') as chat:
            self.assertEqual(asyncio.run(client.chat_async('prompt')), 'reply')
            chat.assert_called_once_with('prompt')

    def test_default_port_without_application_import(self):
        self.assertNotIn('main', sys.modules)
        configure(None)
        self.assertIsNone(interview_agent._default_spark())
        self.assertIsNone(interview_planner._default_spark())
        fake = object()
        try:
            configure(fake)
            self.assertIs(interview_agent._default_spark(), fake)
            self.assertIs(interview_planner._default_spark(), fake)
            self.assertNotIn('main', sys.modules)
        finally:
            configure(None)


if __name__ == '__main__':
    unittest.main()
