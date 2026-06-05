import asyncio
import json
import os
import unittest
from unittest.mock import patch

from mimo2api import web_service
from mimo2api.auth import verify_ws_tunnel_request
from mimo2api.gateway_state import state
from mimo2api.manager import get_bridge_code
from mimo2api.metrics_store import record_attempt_finished, record_attempt_started
from mimo2api.web_service import cleanup_client_state, get_trusted_ws_node_id, replace_existing_node_connection


class FakeWebSocket:
    def __init__(self, headers=None, query_params=None):
        self.headers = headers or {}
        self.query_params = query_params or {}


class FakeClient:
    host = "127.0.0.1"
    port = 12345


class FakeGatewayWebSocket:
    def __init__(self, on_close=None):
        self.headers = {}
        self.query_params = {}
        self.client = FakeClient()
        self.closed_codes = []
        self.on_close = on_close

    async def close(self, code=None):
        if self.on_close is not None:
            self.on_close()
        self.closed_codes.append(code)


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

    def test_unauthorized_ws_rejection_log_is_throttled_by_host(self):
        old_interval = web_service.UNAUTHORIZED_WS_LOG_INTERVAL
        old_next_cleanup_at = web_service._unauthorized_ws_next_cleanup_at
        try:
            web_service.UNAUTHORIZED_WS_LOG_INTERVAL = 60
            web_service._unauthorized_ws_next_cleanup_at = 0.0
            web_service._unauthorized_ws_log_state.clear()

            self.assertEqual(web_service.should_log_unauthorized_ws_rejection("1.2.3.4", now=100.0), (True, 0))
            self.assertEqual(web_service.should_log_unauthorized_ws_rejection("1.2.3.4", now=101.0), (False, 1))
            self.assertEqual(web_service.should_log_unauthorized_ws_rejection("1.2.3.4", now=102.0), (False, 2))
            self.assertEqual(web_service.should_log_unauthorized_ws_rejection("1.2.3.4", now=161.0), (True, 2))
        finally:
            web_service.UNAUTHORIZED_WS_LOG_INTERVAL = old_interval
            web_service._unauthorized_ws_next_cleanup_at = old_next_cleanup_at
            web_service._unauthorized_ws_log_state.clear()

    def test_unauthorized_ws_rejection_log_state_is_capped(self):
        old_interval = web_service.UNAUTHORIZED_WS_LOG_INTERVAL
        old_max_size = web_service.UNAUTHORIZED_WS_LOG_STATE_MAX_SIZE
        old_next_cleanup_at = web_service._unauthorized_ws_next_cleanup_at
        try:
            web_service.UNAUTHORIZED_WS_LOG_INTERVAL = 60
            web_service.UNAUTHORIZED_WS_LOG_STATE_MAX_SIZE = 2
            web_service._unauthorized_ws_next_cleanup_at = 0.0
            web_service._unauthorized_ws_log_state.clear()

            for offset in range(4):
                self.assertEqual(
                    web_service.should_log_unauthorized_ws_rejection(f"10.0.0.{offset}", now=100.0 + offset),
                    (True, 0),
                )

            self.assertLessEqual(len(web_service._unauthorized_ws_log_state), 2)
            self.assertIn("10.0.0.2", web_service._unauthorized_ws_log_state)
            self.assertIn("10.0.0.3", web_service._unauthorized_ws_log_state)
        finally:
            web_service.UNAUTHORIZED_WS_LOG_INTERVAL = old_interval
            web_service.UNAUTHORIZED_WS_LOG_STATE_MAX_SIZE = old_max_size
            web_service._unauthorized_ws_next_cleanup_at = old_next_cleanup_at
            web_service._unauthorized_ws_log_state.clear()

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
        self.assertNotIn('"__NODE_ID__"', code)

    def test_bridge_code_injects_node_id(self):
        with patch.dict(
            os.environ,
            {
                "MIMO2API_WS_URL": "ws://127.0.0.1:8000/ws",
                "MIMO_WS_TUNNEL_KEY": "",
            },
            clear=False,
        ):
            code = asyncio.run(get_bridge_code("account:user-1"))

        self.assertIn('NODE_ID = "account:user-1"', code)
        self.assertIn('"x-node-started-at"', code)
        self.assertIn('"x-node-instance-id"', code)

    def test_node_id_is_ignored_when_ws_auth_is_disabled(self):
        ws = FakeWebSocket(headers={"x-node-id": "account:user-1"})
        with patch.dict(os.environ, {"MIMO_WS_TUNNEL_KEY": ""}, clear=False):
            self.assertIsNone(get_trusted_ws_node_id(ws))

    def test_node_id_is_trusted_when_ws_auth_is_enabled(self):
        ws = FakeWebSocket(headers={"x-node-id": "account:user-1"})
        with patch.dict(os.environ, {"MIMO_WS_TUNNEL_KEY": "secret"}, clear=False):
            self.assertEqual(get_trusted_ws_node_id(ws), "account:user-1")

    def test_replacing_same_node_connection_cleans_old_state(self):
        old_ws = FakeGatewayWebSocket()
        new_ws = FakeGatewayWebSocket()
        req_id = "req-1"
        queue = asyncio.Queue()

        try:
            state.active_clients.append(old_ws)
            state.ws_id_to_node_id[id(old_ws)] = "account:user-1"
            state.node_id_to_ws_id["account:user-1"] = id(old_ws)
            state.ws_id_to_node_started_at[id(old_ws)] = 100.0
            state.ws_id_to_node_instance_id[id(old_ws)] = "old-instance"
            state.ws_to_req_ids[id(old_ws)] = {req_id}
            state.req_id_to_ws_id[req_id] = id(old_ws)
            state.req_id_timestamps[req_id] = 1.0
            state.pending_queues[req_id] = queue
            new_ws.headers = {
                "x-node-started-at": "200.0",
                "x-node-instance-id": "new-instance",
            }

            self.assertTrue(asyncio.run(replace_existing_node_connection("account:user-1", new_ws)))

            self.assertEqual(old_ws.closed_codes, [web_service.NODE_REPLACED_CLOSE_CODE])
            self.assertNotIn(old_ws, state.active_clients)
            self.assertNotIn(id(old_ws), state.ws_id_to_node_id)
            self.assertNotIn(id(old_ws), state.ws_id_to_node_started_at)
            self.assertNotIn(id(old_ws), state.ws_id_to_node_instance_id)
            self.assertNotIn("account:user-1", state.node_id_to_ws_id)
            self.assertNotIn(req_id, state.pending_queues)
            self.assertNotIn(req_id, state.req_id_to_ws_id)
            self.assertNotIn(req_id, state.req_id_timestamps)
            self.assertEqual(queue.get_nowait()["body"], "节点连接已被新实例替换")
        finally:
            state.active_clients.clear()
            state.pending_queues.clear()
            state.ws_to_req_ids.clear()
            state.req_id_to_ws_id.clear()
            state.req_id_timestamps.clear()
            state.ws_id_to_node_id.clear()
            state.node_id_to_ws_id.clear()
            state.ws_id_to_node_started_at.clear()
            state.ws_id_to_node_instance_id.clear()
            state.client_cooldowns.clear()

    def test_older_same_node_connection_is_rejected(self):
        old_ws = FakeGatewayWebSocket()
        new_ws = FakeGatewayWebSocket()
        new_ws.headers = {
            "x-node-started-at": "100.0",
            "x-node-instance-id": "old-duplicate",
        }

        try:
            state.active_clients.append(old_ws)
            state.ws_id_to_node_id[id(old_ws)] = "account:user-1"
            state.node_id_to_ws_id["account:user-1"] = id(old_ws)
            state.ws_id_to_node_started_at[id(old_ws)] = 200.0
            state.ws_id_to_node_instance_id[id(old_ws)] = "current"

            self.assertFalse(asyncio.run(replace_existing_node_connection("account:user-1", new_ws)))

            self.assertIn(old_ws, state.active_clients)
            self.assertEqual(state.node_id_to_ws_id["account:user-1"], id(old_ws))
            self.assertEqual(old_ws.closed_codes, [])
            self.assertEqual(new_ws.closed_codes, [web_service.DUPLICATE_NODE_CLOSE_CODE])
        finally:
            state.active_clients.clear()
            state.pending_queues.clear()
            state.ws_to_req_ids.clear()
            state.req_id_to_ws_id.clear()
            state.req_id_timestamps.clear()
            state.ws_id_to_node_id.clear()
            state.node_id_to_ws_id.clear()
            state.ws_id_to_node_started_at.clear()
            state.ws_id_to_node_instance_id.clear()
            state.client_cooldowns.clear()

    def test_attempt_metrics_use_captured_node_key_after_cleanup(self):
        old_ws = FakeGatewayWebSocket()
        node_key = "account:user-1"

        try:
            state.ws_id_to_node_id[id(old_ws)] = node_key
            record_attempt_started(old_ws, node_key=node_key)
            cleanup_client_state(old_ws)
            record_attempt_finished(
                target_ws=old_ws,
                node_key=node_key,
                status_code=502,
                first_byte_latency_ms=10.0,
                success=False,
            )

            self.assertIn(node_key, state.metrics["nodes"])
            self.assertNotIn("127.0.0.1", state.metrics["nodes"])
            self.assertEqual(state.metrics["nodes"][node_key]["attempts_total"], 1)
            self.assertEqual(state.metrics["nodes"][node_key]["attempts_failed"], 1)
        finally:
            state.active_clients.clear()
            state.pending_queues.clear()
            state.ws_to_req_ids.clear()
            state.req_id_to_ws_id.clear()
            state.req_id_timestamps.clear()
            state.ws_id_to_node_id.clear()
            state.node_id_to_ws_id.clear()
            state.ws_id_to_node_started_at.clear()
            state.ws_id_to_node_instance_id.clear()
            state.client_cooldowns.clear()
            state.metrics = state._default_metrics()

    def test_replacing_same_node_connection_removes_old_before_close(self):
        close_checks = []

        def on_close():
            close_checks.append({
                "in_active_clients": old_ws in state.active_clients,
                "node_mapping": state.node_id_to_ws_id.get("account:user-1"),
            })

        old_ws = FakeGatewayWebSocket(on_close=on_close)
        new_ws = FakeGatewayWebSocket()

        try:
            state.active_clients.append(old_ws)
            state.ws_id_to_node_id[id(old_ws)] = "account:user-1"
            state.node_id_to_ws_id["account:user-1"] = id(old_ws)
            state.ws_id_to_node_started_at[id(old_ws)] = 100.0
            new_ws.headers = {"x-node-started-at": "200.0"}

            self.assertTrue(asyncio.run(replace_existing_node_connection("account:user-1", new_ws)))

            self.assertEqual(close_checks, [{"in_active_clients": False, "node_mapping": None}])
        finally:
            state.active_clients.clear()
            state.pending_queues.clear()
            state.ws_to_req_ids.clear()
            state.req_id_to_ws_id.clear()
            state.req_id_timestamps.clear()
            state.ws_id_to_node_id.clear()
            state.node_id_to_ws_id.clear()
            state.ws_id_to_node_started_at.clear()
            state.ws_id_to_node_instance_id.clear()
            state.client_cooldowns.clear()


if __name__ == "__main__":
    unittest.main()
