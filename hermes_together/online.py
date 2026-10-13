from __future__ import annotations

import json
import logging
import re
import threading
import unicodedata
from dataclasses import dataclass, field
from urllib.parse import urlsplit, urlunsplit
from typing import Any, Callable

try:
    from websockets.exceptions import ConnectionClosed
    from websockets.sync.client import connect
except ImportError:  # Local-only rooms remain usable if the optional online dependency is absent.
    ConnectionClosed = None
    connect = None

_LOG = logging.getLogger(__name__)
DEFAULT_RELAY_URL = "https://hermes-together-render-relay.onrender.com"
MAX_MESSAGE_CHARS = 4_000
MAX_FRAME_BYTES = 24 * 1024
ACK_TIMEOUT_SECONDS = 6.0
ROOM_CODE_RE = re.compile(r"[A-Za-z0-9_-]{24,128}\Z")
THREAD_ID_RE = re.compile(r"[A-Za-z0-9_-]{8,64}\Z")


class OnlineRelayError(ValueError):
    """A relay connection, room, or message operation failed."""


def websocket_url(relay_url: str = DEFAULT_RELAY_URL) -> str:
    """Convert the configured HTTP(S) relay origin into its native WebSocket URL."""
    if not isinstance(relay_url, str):
        raise OnlineRelayError("Relay URL must be text.")
    try:
        parsed = urlsplit(relay_url.strip())
        # Accessing port validates malformed port values in urllib.
        _ = parsed.port
    except ValueError as exc:
        raise OnlineRelayError("Relay URL is malformed.") from exc
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
    ):
        raise OnlineRelayError("Relay URL must be an HTTP(S) origin without a path or credentials.")
    scheme = "wss" if parsed.scheme == "https" else "ws"
    return urlunsplit((scheme, parsed.netloc, "/ws", "", ""))


def validate_room_code(room_code: str) -> str:
    if not isinstance(room_code, str) or not ROOM_CODE_RE.fullmatch(room_code):
        raise OnlineRelayError("Room code must be 24-128 URL-safe letters, digits, '_' or '-'.")
    return room_code


def _printable_name(value: str) -> str:
    name = " ".join(value.split()) if isinstance(value, str) else ""
    if not name or len(name) > 48 or not name.isprintable():
        raise OnlineRelayError("Participant name must contain 1-48 printable characters.")
    return name


def _is_safe_incoming_message(frame: dict[str, Any]) -> bool:
    body = frame.get("body")
    thread_id = frame.get("thread_id")
    sender = frame.get("sender")
    if (
        not isinstance(body, str)
        or not body.strip()
        or len(body) > MAX_MESSAGE_CHARS
        or not isinstance(thread_id, str)
        or not THREAD_ID_RE.fullmatch(thread_id)
    ):
        return False
    if any(unicodedata.category(char) == "Cc" and char not in "\n\t" for char in body):
        return False
    try:
        _printable_name(sender)
    except OnlineRelayError:
        return False
    return True


def _parse_frame(raw: str | bytes) -> dict[str, Any]:
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", errors="strict")
    frame = json.loads(raw)
    if not isinstance(frame, dict):
        raise OnlineRelayError("Relay sent an invalid frame.")
    return frame


@dataclass
class _PendingReply:
    ready: threading.Event = field(default_factory=threading.Event)
    result: dict[str, Any] | None = None
    error: OnlineRelayError | None = None


class OnlineRelayClient:
    """Synchronous WebSocket client for the temporary, in-memory Together relay."""

    def __init__(
        self,
        room_code: str,
        display_name: str,
        *,
        relay_url: str = DEFAULT_RELAY_URL,
        on_message: Callable[[dict[str, Any]], None],
    ) -> None:
        self._room_code = validate_room_code(room_code)
        self._display_name = _printable_name(display_name)
        self._url = websocket_url(relay_url)
        self._on_message = on_message
        self._connection: Any = None
        self._lock = threading.RLock()
        self._send_lock = threading.Lock()
        self._pending: _PendingReply | None = None
        self._participants: dict[str, str] = {}
        self._last_error: str | None = None
        self._stop = threading.Event()
        self._connected = threading.Event()
        self._worker: threading.Thread | None = None
        self._connect_and_join()

    @property
    def connected(self) -> bool:
        return self._connected.is_set()

    @property
    def participants(self) -> tuple[str, ...]:
        with self._lock:
            return tuple(self._participants.values())

    @property
    def last_error(self) -> str | None:
        with self._lock:
            return self._last_error

    def _connect_and_join(self) -> None:
        if connect is None:
            raise OnlineRelayError(
                "Online Together requires websockets>=14,<16 installed in the Python environment that runs Hermes."
            )
        connection: Any = None
        try:
            connection = connect(
                self._url,
                open_timeout=10,
                close_timeout=2,
                max_size=MAX_FRAME_BYTES,
                compression=None,
            )
            connection.send(json.dumps(
                {"type": "join", "code": self._room_code, "name": self._display_name},
                ensure_ascii=False,
                separators=(",", ":"),
            ))
            response = _parse_frame(connection.recv(timeout=8))
            if response.get("type") == "error":
                raise OnlineRelayError(self._safe_error(response.get("message")))
            if response.get("type") != "joined":
                raise OnlineRelayError("Relay did not confirm joining the room.")
            self._room_code = ""
            for participant in response.get("participants", []):
                if isinstance(participant, dict):
                    participant_id = participant.get("id")
                    participant_name = participant.get("name")
                    if isinstance(participant_id, str) and isinstance(participant_name, str):
                        try:
                            self._participants[participant_id] = _printable_name(participant_name)
                        except OnlineRelayError:
                            continue
            self._connection = connection
            self._connected.set()
            self._worker = threading.Thread(
                target=self._receive_loop,
                name="hermes-together-online",
                daemon=True,
            )
            self._worker.start()
        except TimeoutError:
            if connection is not None:
                try:
                    connection.close()
                except Exception:
                    pass
            raise OnlineRelayError("Relay did not respond in time; it may still be deploying. Try again shortly.") from None
        except OnlineRelayError:
            if connection is not None:
                try:
                    connection.close()
                except Exception:
                    pass
            raise
        except Exception as exc:
            if connection is not None:
                try:
                    connection.close()
                except Exception:
                    pass
            if ConnectionClosed is not None and isinstance(exc, ConnectionClosed):
                code = getattr(getattr(exc, "rcvd", None), "code", None)
                detail = f" (relay close code {code})" if code is not None else ""
                raise OnlineRelayError(f"Relay closed the connection while joining{detail}.") from None
            _LOG.warning("Together online connection failed: %s", type(exc).__name__)
            raise OnlineRelayError("Could not reach the Together relay; it may still be deploying. Try again shortly.") from None

    @staticmethod
    def _safe_error(value: Any) -> str:
        if not isinstance(value, str):
            return "The relay rejected the request."
        clean = " ".join(value.split())
        clean = "".join(char for char in clean if char.isprintable())
        return clean[:200] or "The relay rejected the request."

    def send(self, body: str, thread_id: str | None = None) -> dict[str, Any]:
        if not isinstance(body, str):
            raise OnlineRelayError("Message must be text.")
        body = body.replace("\r\n", "\n").replace("\r", "\n").strip()
        if not body or len(body) > MAX_MESSAGE_CHARS:
            raise OnlineRelayError(f"Message must contain 1-{MAX_MESSAGE_CHARS} characters.")
        if any(unicodedata.category(char) == "Cc" and char not in "\n\t" for char in body):
            raise OnlineRelayError("Control characters are not allowed in online messages.")
        if thread_id is not None and (not isinstance(thread_id, str) or not THREAD_ID_RE.fullmatch(thread_id)):
            raise OnlineRelayError("thread_id must contain 8-64 URL-safe characters.")

        with self._send_lock:
            connection = self._connection
            if not self.connected or connection is None:
                raise OnlineRelayError("Not connected to the online room; rejoin with /together-online <code>.")
            pending = _PendingReply()
            with self._lock:
                self._pending = pending
            payload: dict[str, Any] = {"type": "message", "body": body}
            if thread_id is not None:
                payload["thread_id"] = thread_id
            try:
                connection.send(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
            except Exception:
                with self._lock:
                    if self._pending is pending:
                        self._pending = None
                raise OnlineRelayError("Could not send to the online room; the relay connection closed.") from None
            if not pending.ready.wait(ACK_TIMEOUT_SECONDS):
                with self._lock:
                    if self._pending is pending:
                        self._pending = None
                raise OnlineRelayError("The online relay did not acknowledge the message in time.")
            if pending.error is not None:
                raise pending.error
            if pending.result is None:
                raise OnlineRelayError("The online relay returned an invalid acknowledgement.")
            return pending.result

    def _receive_loop(self) -> None:
        connection = self._connection
        if connection is None:
            return
        try:
            while not self._stop.is_set():
                try:
                    frame = _parse_frame(connection.recv(timeout=1))
                except TimeoutError:
                    continue
                frame_type = frame.get("type")
                if frame_type == "sent":
                    thread_id = frame.get("thread_id")
                    delivered_to = frame.get("delivered_to", 0)
                    with self._lock:
                        pending = self._pending
                        if pending is not None:
                            if (
                                not isinstance(thread_id, str)
                                or not THREAD_ID_RE.fullmatch(thread_id)
                                or type(delivered_to) is not int
                                or delivered_to < 0
                            ):
                                pending.error = OnlineRelayError("Relay returned an invalid message acknowledgement.")
                                self._last_error = str(pending.error)
                            else:
                                pending.result = {
                                    "thread_id": thread_id,
                                    "delivered_to": delivered_to,
                                }
                            pending.ready.set()
                            self._pending = None
                elif frame_type == "error":
                    message = self._safe_error(frame.get("message"))
                    error = OnlineRelayError(message)
                    with self._lock:
                        self._last_error = message
                        pending = self._pending
                        if pending is not None:
                            pending.error = error
                            pending.ready.set()
                            self._pending = None
                elif frame_type == "presence":
                    self._update_presence(frame)
                elif frame_type == "message":
                    if not _is_safe_incoming_message(frame):
                        _LOG.warning("Discarded malformed online Together message frame")
                        continue
                    frame["sender"] = _printable_name(frame["sender"])
                    try:
                        self._on_message(frame)
                    except Exception:
                        _LOG.exception("Together online message callback failed")
        except Exception as exc:
            if ConnectionClosed is None or not isinstance(exc, ConnectionClosed):
                if not self._stop.is_set():
                    _LOG.exception("Together online receive loop failed")
        finally:
            self._connected.clear()
            with self._lock:
                pending = self._pending
                self._pending = None
                if pending is not None:
                    pending.error = OnlineRelayError("Relay disconnected before acknowledging the message.")
                    pending.ready.set()

    def _update_presence(self, frame: dict[str, Any]) -> None:
        participant = frame.get("participant")
        if not isinstance(participant, dict):
            return
        participant_id = participant.get("id")
        if not isinstance(participant_id, str):
            return
        with self._lock:
            if frame.get("event") == "leave":
                self._participants.pop(participant_id, None)
            elif frame.get("event") == "join":
                name = participant.get("name")
                if isinstance(name, str):
                    try:
                        self._participants[participant_id] = _printable_name(name)
                    except OnlineRelayError:
                        return

    def close(self) -> None:
        self._stop.set()
        connection = self._connection
        if connection is not None:
            try:
                connection.close(code=1000, reason="leaving room")
            except Exception:
                pass
        worker = self._worker
        if worker is not None and worker is not threading.current_thread():
            worker.join(timeout=3)
        self._connected.clear()
