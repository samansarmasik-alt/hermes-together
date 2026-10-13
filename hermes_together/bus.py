from __future__ import annotations

import hashlib
import ntpath
import posixpath
import re
import secrets
import sqlite3
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Callable, Iterator


class BusError(ValueError):
    """A room or message operation was invalid or unavailable."""


class TogetherBus:
    """Small SQLite-backed mailbox shared by independent Hermes processes."""

    MAX_MESSAGE_CHARS = 4_000
    MAX_THREAD_MESSAGES = 8
    MAX_MESSAGES_PER_MINUTE = 20
    MAX_MESSAGES_PER_ROOM = 500
    MAX_THREADS_PER_ROOM = 10_000
    MAX_READ_MESSAGES = 20
    STALE_AFTER_SECONDS = 30.0
    _ROOM_RE = re.compile(r"[a-z0-9][a-z0-9._:-]{0,79}\Z")
    _PARTICIPANT_RE = re.compile(r"[A-Za-z0-9._-]{1,64}\Z")
    _THREAD_RE = re.compile(r"[A-Za-z0-9_-]{8,64}\Z")

    def __init__(
        self,
        db_path: str | Path | None = None,
        *,
        connection_factory: Callable[[], sqlite3.Connection] | None = None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        if (db_path is None) == (connection_factory is None):
            raise ValueError("provide exactly one of db_path or connection_factory")
        self._clock = clock
        self._managed_connection_factory = connection_factory is not None
        if connection_factory is not None:
            self._connection_factory = connection_factory
        else:
            path = Path(db_path).expanduser()
            path.parent.mkdir(parents=True, exist_ok=True)
            self._connection_factory = lambda: sqlite3.connect(path, timeout=5.0)
        self._initialize()

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        conn = self._connection_factory()
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout = 5000")
        try:
            yield conn
        finally:
            conn.close()

    def _initialize(self) -> None:
        with self._connection() as conn:
            if not self._managed_connection_factory:
                try:
                    conn.execute("PRAGMA journal_mode = WAL")
                except sqlite3.OperationalError:
                    # A competing first-time process may already have selected the mode.
                    pass
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS together_rooms (
                    room_id TEXT PRIMARY KEY,
                    created_at REAL NOT NULL,
                    last_activity REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS together_participants (
                    room_id TEXT NOT NULL,
                    participant_id TEXT NOT NULL,
                    display_name TEXT NOT NULL,
                    joined_at REAL NOT NULL,
                    last_seen REAL NOT NULL,
                    active INTEGER NOT NULL DEFAULT 1,
                    active_thread_id TEXT,
                    PRIMARY KEY (room_id, participant_id),
                    FOREIGN KEY (room_id) REFERENCES together_rooms(room_id)
                );
                CREATE TABLE IF NOT EXISTS together_messages (
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    room_id TEXT NOT NULL,
                    sender_id TEXT NOT NULL,
                    sender_name TEXT NOT NULL,
                    body TEXT NOT NULL,
                    thread_id TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    FOREIGN KEY (room_id) REFERENCES together_rooms(room_id)
                );
                CREATE TABLE IF NOT EXISTS together_thread_usage (
                    room_id TEXT NOT NULL,
                    thread_id TEXT NOT NULL,
                    message_count INTEGER NOT NULL,
                    created_at REAL NOT NULL,
                    last_activity REAL NOT NULL,
                    PRIMARY KEY (room_id, thread_id),
                    FOREIGN KEY (room_id) REFERENCES together_rooms(room_id)
                );
                CREATE INDEX IF NOT EXISTS together_messages_room_seq
                    ON together_messages(room_id, sequence);
                CREATE INDEX IF NOT EXISTS together_messages_room_thread
                    ON together_messages(room_id, thread_id, sequence);
                CREATE INDEX IF NOT EXISTS together_messages_sender_time
                    ON together_messages(room_id, sender_id, created_at);
                """
            )
            participant_columns = {
                row["name"] for row in conn.execute("PRAGMA table_info(together_participants)")
            }
            if "active_thread_id" not in participant_columns:
                try:
                    conn.execute("ALTER TABLE together_participants ADD COLUMN active_thread_id TEXT")
                except sqlite3.OperationalError as exc:
                    if "duplicate column name" not in str(exc).lower():
                        raise
            conn.execute(
                """INSERT OR IGNORE INTO together_thread_usage
                   (room_id, thread_id, message_count, created_at, last_activity)
                   SELECT room_id, thread_id, COUNT(*), MIN(created_at), MAX(created_at)
                   FROM together_messages GROUP BY room_id, thread_id"""
            )
            conn.execute(
                """UPDATE together_participants SET active_thread_id = (
                       SELECT thread_id FROM together_messages
                       WHERE together_messages.room_id = together_participants.room_id
                         AND together_messages.sender_id = together_participants.participant_id
                       ORDER BY sequence DESC LIMIT 1
                   ) WHERE active_thread_id IS NULL"""
            )
            conn.commit()

    @classmethod
    def _room_id(cls, room_id: str) -> str:
        if not isinstance(room_id, str):
            raise BusError("Room id must be text.")
        value = room_id.strip().lower()
        if not cls._ROOM_RE.fullmatch(value):
            raise BusError("Room id must be 1-80 letters, digits, '.', '_', ':', or '-'.")
        return value

    @classmethod
    def _participant_id(cls, participant_id: str) -> str:
        if not isinstance(participant_id, str) or not cls._PARTICIPANT_RE.fullmatch(participant_id):
            raise BusError("Participant id is invalid.")
        return participant_id

    @staticmethod
    def _display_name(display_name: str) -> str:
        if not isinstance(display_name, str):
            raise BusError("Display name must be text.")
        value = " ".join(display_name.split())
        if not value or len(value) > 48 or not value.isprintable():
            raise BusError("Display name must contain 1-48 printable characters.")
        return value

    @classmethod
    def _thread_id(cls, thread_id: str | None) -> str:
        if thread_id is None:
            return secrets.token_hex(8)
        if not isinstance(thread_id, str) or not cls._THREAD_RE.fullmatch(thread_id):
            raise BusError("Thread id must be 8-64 letters, digits, '_' or '-'.")
        return thread_id

    def join(self, room_id: str, participant_id: str, display_name: str) -> int:
        room_id = self._room_id(room_id)
        participant_id = self._participant_id(participant_id)
        display_name = self._display_name(display_name)
        now = self._clock()
        with self._connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                "INSERT OR IGNORE INTO together_rooms(room_id, created_at, last_activity) VALUES (?, ?, ?)",
                (room_id, now, now),
            )
            cursor = conn.execute(
                "SELECT COALESCE(MAX(sequence), 0) FROM together_messages WHERE room_id = ?",
                (room_id,),
            ).fetchone()[0]
            conn.execute(
                """INSERT INTO together_participants
                   (room_id, participant_id, display_name, joined_at, last_seen, active)
                   VALUES (?, ?, ?, ?, ?, 1)
                   ON CONFLICT(room_id, participant_id) DO UPDATE SET
                       display_name = excluded.display_name,
                       last_seen = excluded.last_seen,
                       active = 1,
                       active_thread_id = NULL""",
                (room_id, participant_id, display_name, now, now),
            )
            conn.execute("UPDATE together_rooms SET last_activity = ? WHERE room_id = ?", (now, room_id))
            conn.commit()
        return int(cursor)

    def leave(self, room_id: str, participant_id: str) -> bool:
        room_id = self._room_id(room_id)
        participant_id = self._participant_id(participant_id)
        with self._connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            result = conn.execute(
                "UPDATE together_participants SET active = 0 WHERE room_id = ? AND participant_id = ? AND active = 1",
                (room_id, participant_id),
            )
            conn.commit()
            return result.rowcount == 1

    def heartbeat(self, room_id: str, participant_id: str) -> bool:
        room_id = self._room_id(room_id)
        participant_id = self._participant_id(participant_id)
        now = self._clock()
        with self._connection() as conn:
            result = conn.execute(
                "UPDATE together_participants SET last_seen = ? WHERE room_id = ? AND participant_id = ? AND active = 1",
                (now, room_id, participant_id),
            )
            conn.commit()
            return result.rowcount == 1

    def rename(self, room_id: str, participant_id: str, display_name: str) -> None:
        room_id = self._room_id(room_id)
        participant_id = self._participant_id(participant_id)
        display_name = self._display_name(display_name)
        with self._connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            self._require_member(conn, room_id, participant_id)
            conn.execute(
                "UPDATE together_participants SET display_name = ?, last_seen = ? WHERE room_id = ? AND participant_id = ?",
                (display_name, self._clock(), room_id, participant_id),
            )
            conn.commit()

    def send(
        self,
        room_id: str,
        participant_id: str,
        body: str,
        *,
        thread_id: str | None = None,
    ) -> dict[str, str | int]:
        room_id = self._room_id(room_id)
        participant_id = self._participant_id(participant_id)
        if not isinstance(body, str) or not body.strip():
            raise BusError("Message must not be empty.")
        body = body.strip()
        if len(body) > self.MAX_MESSAGE_CHARS:
            raise BusError(f"Message exceeds {self.MAX_MESSAGE_CHARS} characters.")
        now = self._clock()
        with self._connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            member = self._require_member(conn, room_id, participant_id)
            if thread_id is None:
                thread_id = member["active_thread_id"]
            thread_id = self._thread_id(thread_id)
            usage = conn.execute(
                "SELECT message_count FROM together_thread_usage WHERE room_id = ? AND thread_id = ?",
                (room_id, thread_id),
            ).fetchone()
            if usage is not None and int(usage["message_count"]) >= self.MAX_THREAD_MESSAGES:
                raise BusError(
                    f"This exchange reached its {self.MAX_THREAD_MESSAGES}-message safety limit. "
                    "Start a new user-led exchange instead of continuing the loop."
                )
            if usage is None:
                thread_total = conn.execute(
                    "SELECT COUNT(*) FROM together_thread_usage WHERE room_id = ?", (room_id,)
                ).fetchone()[0]
                if thread_total >= self.MAX_THREADS_PER_ROOM:
                    raise BusError("This room reached its lifetime thread limit; use a new room.")
            minute_count = conn.execute(
                "SELECT COUNT(*) FROM together_messages WHERE room_id = ? AND sender_id = ? AND created_at >= ?",
                (room_id, participant_id, now - 60.0),
            ).fetchone()[0]
            if minute_count >= self.MAX_MESSAGES_PER_MINUTE:
                raise BusError("Message rate limit reached; wait a minute before sending again.")
            if usage is None:
                conn.execute(
                    """INSERT INTO together_thread_usage
                       (room_id, thread_id, message_count, created_at, last_activity)
                       VALUES (?, ?, 1, ?, ?)""",
                    (room_id, thread_id, now, now),
                )
            else:
                conn.execute(
                    "UPDATE together_thread_usage SET message_count = message_count + 1, last_activity = ? "
                    "WHERE room_id = ? AND thread_id = ?",
                    (now, room_id, thread_id),
                )
            conn.execute(
                "UPDATE together_participants SET active_thread_id = ?, last_seen = ? "
                "WHERE room_id = ? AND participant_id = ?",
                (thread_id, now, room_id, participant_id),
            )
            result = conn.execute(
                """INSERT INTO together_messages
                   (room_id, sender_id, sender_name, body, thread_id, created_at)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (room_id, participant_id, member["display_name"], body, thread_id, now),
            )
            sequence = int(result.lastrowid)
            conn.execute("UPDATE together_rooms SET last_activity = ? WHERE room_id = ?", (now, room_id))
            conn.execute(
                """DELETE FROM together_messages WHERE room_id = ? AND sequence NOT IN
                   (SELECT sequence FROM together_messages WHERE room_id = ? ORDER BY sequence DESC LIMIT ?)""",
                (room_id, room_id, self.MAX_MESSAGES_PER_ROOM),
            )
            conn.commit()
        return {"sequence": sequence, "thread_id": thread_id}

    def read(
        self,
        room_id: str,
        participant_id: str,
        after_sequence: int,
        *,
        limit: int = MAX_READ_MESSAGES,
    ) -> list[dict[str, str | int | float]]:
        room_id = self._room_id(room_id)
        participant_id = self._participant_id(participant_id)
        if type(after_sequence) is not int or after_sequence < 0:
            raise BusError("Message cursor must be a non-negative integer.")
        if type(limit) is not int or not 1 <= limit <= self.MAX_READ_MESSAGES:
            raise BusError(f"Read limit must be between 1 and {self.MAX_READ_MESSAGES}.")
        with self._connection() as conn:
            self._require_member(conn, room_id, participant_id)
            rows = conn.execute(
                """SELECT sequence, sender_id, sender_name AS sender, body, thread_id, created_at
                   FROM together_messages
                   WHERE room_id = ? AND sequence > ? AND sender_id <> ?
                   ORDER BY sequence ASC LIMIT ?""",
                (room_id, after_sequence, participant_id, limit),
            ).fetchall()
        return [dict(row) for row in rows]

    def peers(self, room_id: str, *, include_stale: bool = False) -> list[dict[str, str | float]]:
        room_id = self._room_id(room_id)
        cutoff = self._clock() - self.STALE_AFTER_SECONDS
        query = (
            "SELECT participant_id, display_name, last_seen FROM together_participants "
            "WHERE room_id = ? AND active = 1"
        )
        params: tuple[object, ...] = (room_id,)
        if not include_stale:
            query += " AND last_seen >= ?"
            params = (room_id, cutoff)
        query += " ORDER BY joined_at, participant_id"
        with self._connection() as conn:
            rows = conn.execute(query, params).fetchall()
        return [dict(row) for row in rows]

    def _require_member(self, conn: sqlite3.Connection, room_id: str, participant_id: str) -> sqlite3.Row:
        row = conn.execute(
            """SELECT display_name, active_thread_id FROM together_participants
               WHERE room_id = ? AND participant_id = ? AND active = 1""",
            (room_id, participant_id),
        ).fetchone()
        if row is None:
            raise BusError("This CLI is not an active member of that room. Run /together to join.")
        return row


def default_room_id(workspace: str | Path) -> str:
    """Return a stable, path-independent room key for CLIs in the same workspace."""
    raw = str(workspace).strip()
    if ntpath.splitdrive(raw)[0]:
        normalized = ntpath.normcase(ntpath.normpath(raw))
    else:
        normalized = posixpath.normpath(raw)
    digest = hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:20]
    return f"workspace:{digest}"


def new_participant_id() -> str:
    return uuid.uuid4().hex
