import asyncio
import json
import os
import unittest
from unittest.mock import patch

from mimo2api.auth import verify_ws_tunnel_request
from mimo2api.manager import get_bridge_code


class FakeWebSocket:
    def __init__(self, headers=None, query_params=None):
        self.headers = headers or {}
        self.query_params = query_params or {}


class WebSocketAuthTests(unittest.TestCase):
    def test_ws_auth_disabled_allows_connection(self):
        with patch.dict(os.environ, {"MIMO_WS_TUNNEL_KEY": ""}, clear=False):
            self.assertTrue(verify_ws_tunnel_request(FakeWebSocket()))

    def test_ws_auth_accepts_bearer_token(self):
        with patch.dict(os.environ, {"MIMO_WS_TUNNEL_KEY": "secret"}, clear=False):
            ws = FakeWebSocket(headers={"authorization": "Bearer secret"})
            self.assertTrue(verify_ws_tunnel_request(ws))

    def test_ws_auth_accepts_x_ws_token_header(self):
        with patch.dict(os.environ, {"MIMO_WS_TUNNEL_KEY": "secret"}, clear=False):
            ws = FakeWebSocket(headers={"x-ws-token": "secret"})
            self.assertTrue(verify_ws_tunnel_request(ws))

    def test_ws_auth_accepts_query_token(self):
        with patch.dict(os.environ, {"MIMO_WS_TUNNEL_KEY": "secret"}, clear=False):
            ws = FakeWebSocket(query_params={"token": "secret"})
            self.assertTrue(verify_ws_tunnel_request(ws))

    def test_ws_auth_rejects_missing_or_invalid_token(self):
        with patch.dict(os.environ, {"MIMO_WS_TUNNEL_KEY": "secret"}, clear=False):
            self.assertFalse(verify_ws_tunnel_request(FakeWebSocket()))
            self.assertFalse(
                verify_ws_tunnel_request(FakeWebSocket(headers={"authorization": "Bearer wrong"}))
            )

    def test_bridge_code_injects_ws_token(self):
        ws_url = 'ws://127.0.0.1:8000/ws?source="local"'
        ws_token = 'sec"ret\\value'
        with patch.dict(
            os.environ,
            {
                "MIMO2API_WS_URL": ws_url,
                "MIMO_WS_TUNNEL_KEY": ws_token,
            },
            clear=False,
        ):
            code = asyncio.run(get_bridge_code())

        self.assertIn(f"WS_TOKEN = {json.dumps(ws_token)}", code)
        self.assertIn(f"WS_URL = {json.dumps(ws_url)}", code)
        self.assertNotIn('"__WS_TOKEN__"', code)
        self.assertNotIn('"__WS_URL__"', code)


if __name__ == "__main__":
    unittest.main()
