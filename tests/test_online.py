from __future__ import annotations

import json
import queue
import threading
import unittest
from unittest.mock import patch

from websockets.exceptions import ConnectionClosed
from websockets.sync.server import serve

from hermes_together.online import OnlineRelayClient, OnlineRelayError, websocket_url


class OnlineRelayClientTests(unittest.TestCase):
    def test_https_relay_url_maps_to_secure_websocket_endpoint(self) -> None:
        self.assertEqual(
            websocket_url("https://hermes-together-render-relay.onrender.com"),
            "wss://hermes-together-render-relay.onrender.com/ws",
        )

    def test_invalid_relay_acknowledgement_is_rejected(self) -> None:
        def relay_handler(connection) -> None:
            try:
                connection.recv(timeout=3)
                connection.send(json.dumps({
                    "type": "joined",
                    "participant_id": "participant-1",
                    "participants": [{"id": "participant-1", "name": "CLI-test"}],
                }))
                connection.recv(timeout=3)
                connection.send(json.dumps({
                    "type": "sent", "thread_id": "invalid!", "delivered_to": 1,
                }))
            except (ConnectionClosed, TimeoutError):
                return

        server = serve(relay_handler, "127.0.0.1", 0, compression=None)
        server_thread = threading.Thread(target=server.serve_forever, daemon=True)
        server_thread.start()
        client = None
        try:
            port = server.socket.getsockname()[1]
            client = OnlineRelayClient(
                "7yQs4-PkN9tB3vXw6cRa2LmF",
                "CLI-test",
                relay_url=f"http://127.0.0.1:{port}",
                on_message=lambda _frame: None,
            )
            with self.assertRaisesRegex(OnlineRelayError, "invalid message acknowledgement"):
                client.send("share this", "thread0001")
        finally:
            if client is not None:
                client.close()
            server.shutdown()
            server_thread.join(timeout=2)

    def test_http_loopback_url_maps_to_websocket_endpoint(self) -> None:
        self.assertEqual(websocket_url("http://127.0.0.1:9000"), "ws://127.0.0.1:9000/ws")

    def test_invalid_room_code_is_rejected_before_connecting(self) -> None:
        with self.assertRaises(OnlineRelayError):
            OnlineRelayClient("too-short", "CLI-test", on_message=lambda _frame: None)

    def test_missing_websockets_dependency_fails_only_when_online_mode_is_used(self) -> None:
        room_code = "7yQs4-PkN9tB3vXw6cRa2LmF"
        with patch("hermes_together.online.connect", None):
            with self.assertRaisesRegex(OnlineRelayError, "websockets"):
                OnlineRelayClient(room_code, "CLI-test", on_message=lambda _frame: None)

    def test_client_joins_sends_and_receives_using_relay_wire_protocol(self) -> None:
        room_code = "7yQs4-PkN9tB3vXw6cRa2LmF"
        observed_joins: queue.Queue[dict] = queue.Queue()
        delivered: queue.Queue[dict] = queue.Queue()

        def relay_handler(connection) -> None:
            try:
                join = json.loads(connection.recv(timeout=3))
                observed_joins.put(join)
                connection.send(json.dumps({
                    "type": "joined",
                    "participant_id": "participant-1",
                    "participants": [{"id": "participant-1", "name": "CLI-test"}],
                }))
                frame = json.loads(connection.recv(timeout=3))
                if frame.get("type") == "message":
                    connection.send(json.dumps({
                        "type": "message", "sender": "bad peer", "body": "invalid thread",
                        "thread_id": "bad", "created_at": "",
                    }))
                    connection.send(json.dumps({
                        "type": "message",
                        "message_id": "remote-message-1",
                        "thread_id": frame.get("thread_id", "thread0001"),
                        "sender_id": "peer-1",
                        "sender": "Peer",
                        "body": "online hello",
                        "created_at": "2026-10-13T10:00:00.000Z",
                    }))
                    connection.send(json.dumps({
                        "type": "sent",
                        "message_id": "local-message-1",
                        "thread_id": frame.get("thread_id", "thread0001"),
                        "delivered_to": 1,
                    }))
            except (ConnectionClosed, TimeoutError):
                return

        server = serve(relay_handler, "127.0.0.1", 0, compression=None)
        server_thread = threading.Thread(target=server.serve_forever, daemon=True)
        server_thread.start()
        client = None
        try:
            port = server.socket.getsockname()[1]
            client = OnlineRelayClient(
                room_code,
                "CLI-test",
                relay_url=f"http://127.0.0.1:{port}",
                on_message=delivered.put,
            )
            join = observed_joins.get(timeout=2)
            self.assertEqual(join, {"type": "join", "code": room_code, "name": "CLI-test"})
            self.assertTrue(client.connected)
            self.assertNotIn(room_code, repr(vars(client)), "joined clients must not retain the bearer code")
            self.assertEqual(client.participants, ("CLI-test",))

            result = client.send("share this", "thread0001")
            self.assertEqual(result["thread_id"], "thread0001")
            self.assertEqual(result["delivered_to"], 1)
            incoming = delivered.get(timeout=2)
            self.assertEqual(incoming["sender"], "Peer")
            self.assertEqual(incoming["body"], "online hello")
        finally:
            if client is not None:
                client.close()
            server.shutdown()
            server_thread.join(timeout=2)


if __name__ == "__main__":
    unittest.main()
