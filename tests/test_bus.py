from __future__ import annotations

import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from hermes_together.bus import BusError, TogetherBus, default_room_id


class TogetherBusTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "together.db"
        self.bus = TogetherBus(self.db_path)
        self.room = "default:workspace-a"
        self.alice_cursor = self.bus.join(self.room, "alice", "Alice")
        self.bob_cursor = self.bus.join(self.room, "bob", "Bob")

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_peers_receive_new_messages_without_replaying_their_own(self) -> None:
        sent = self.bus.send(self.room, "alice", "Ready to coordinate?")

        bob_messages = self.bus.read(self.room, "bob", self.bob_cursor)
        alice_messages = self.bus.read(self.room, "alice", self.alice_cursor)

        self.assertEqual(len(bob_messages), 1)
        self.assertEqual(bob_messages[0]["sender"], "Alice")
        self.assertEqual(bob_messages[0]["body"], "Ready to coordinate?")
        self.assertEqual(bob_messages[0]["thread_id"], sent["thread_id"])
        self.assertEqual(alice_messages, [])

    def test_new_member_does_not_receive_messages_from_before_joining(self) -> None:
        self.bus.send(self.room, "alice", "Earlier message")

        charlie_cursor = self.bus.join(self.room, "charlie", "Charlie")

        self.assertEqual(self.bus.read(self.room, "charlie", charlie_cursor), [])

    def test_room_membership_is_enforced_for_sending(self) -> None:
        with self.assertRaises(BusError):
            self.bus.send("default:other-room", "alice", "not allowed")

    def test_leave_revokes_read_and_write_access(self) -> None:
        self.assertTrue(self.bus.leave(self.room, "alice"))

        with self.assertRaises(BusError):
            self.bus.send(self.room, "alice", "after leave")
        with self.assertRaises(BusError):
            self.bus.read(self.room, "alice", self.alice_cursor)

    def test_exchange_allows_twenty_four_messages_then_stops(self) -> None:
        thread_id = "exchange01"
        for index in range(24):
            self.bus.send(self.room, "alice" if index % 2 == 0 else "bob", f"message {index}", thread_id=thread_id)
        with self.assertRaisesRegex(BusError, "24-message safety limit"):
            self.bus.send(self.room, "alice", "message 25", thread_id=thread_id)

    def test_replies_in_one_thread_have_a_hard_bound(self) -> None:
        first = self.bus.send(self.room, "alice", "Start a bounded exchange")
        thread_id = first["thread_id"]
        for index in range(self.bus.MAX_THREAD_MESSAGES - 1):
            sender = "bob" if index % 2 == 0 else "alice"
            self.bus.send(self.room, sender, f"reply {index}", thread_id=thread_id)

        with self.assertRaises(BusError):
            self.bus.send(self.room, "bob", "must be blocked", thread_id=thread_id)

    def test_omitted_thread_id_reuses_the_sender_current_thread(self) -> None:
        first = self.bus.send(self.room, "alice", "first")
        second = self.bus.send(self.room, "alice", "second")

        self.assertEqual(first["thread_id"], second["thread_id"])

    def test_thread_limit_survives_message_retention(self) -> None:
        thread_id = "retained01"
        with patch.object(TogetherBus, "MAX_MESSAGES_PER_ROOM", 1):
            for index in range(self.bus.MAX_THREAD_MESSAGES):
                self.bus.send(self.room, "alice", f"message {index}", thread_id=thread_id)
            with self.assertRaises(BusError):
                self.bus.send(self.room, "bob", "must be blocked", thread_id=thread_id)

    def test_legacy_database_migrates_thread_usage_and_sender_thread(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            path = Path(tempdir) / "legacy.db"
            conn = sqlite3.connect(path)
            try:
                conn.executescript(
                    """
                    CREATE TABLE together_rooms (
                        room_id TEXT PRIMARY KEY, created_at REAL NOT NULL, last_activity REAL NOT NULL
                    );
                    CREATE TABLE together_participants (
                        room_id TEXT NOT NULL, participant_id TEXT NOT NULL, display_name TEXT NOT NULL,
                        joined_at REAL NOT NULL, last_seen REAL NOT NULL, active INTEGER NOT NULL DEFAULT 1,
                        PRIMARY KEY (room_id, participant_id)
                    );
                    CREATE TABLE together_messages (
                        sequence INTEGER PRIMARY KEY AUTOINCREMENT, room_id TEXT NOT NULL,
                        sender_id TEXT NOT NULL, sender_name TEXT NOT NULL, body TEXT NOT NULL,
                        thread_id TEXT NOT NULL, created_at REAL NOT NULL
                    );
                    INSERT INTO together_rooms VALUES ('legacy-room', 1, 1);
                    INSERT INTO together_participants VALUES ('legacy-room', 'alice', 'Alice', 1, 1, 1);
                    INSERT INTO together_participants VALUES ('legacy-room', 'bob', 'Bob', 1, 1, 1);
                    INSERT INTO together_messages VALUES (1, 'legacy-room', 'alice', 'Alice', 'one', 'legacy001', 1);
                    INSERT INTO together_messages VALUES (2, 'legacy-room', 'bob', 'Bob', 'two', 'legacy001', 2);
                    """
                )
            finally:
                conn.close()

            migrated = TogetherBus(path)
            continued = migrated.send("legacy-room", "alice", "continues without id")
            self.assertEqual(continued["thread_id"], "legacy001")
            with patch.object(TogetherBus, "MAX_THREAD_MESSAGES", 3):
                with self.assertRaises(BusError):
                    migrated.send("legacy-room", "bob", "cap includes legacy messages")

    def test_concurrent_writers_get_unique_monotonic_sequences(self) -> None:
        workers = []
        errors: list[BaseException] = []
        sent_sequences: list[int] = []
        lock = threading.Lock()

        def write_batch(worker_id: int) -> None:
            try:
                local_bus = TogetherBus(self.db_path)
                member_id = f"worker-{worker_id}"
                local_bus.join(self.room, member_id, member_id)
                for index in range(12):
                    item = local_bus.send(
                        self.room, member_id, f"{worker_id}:{index}",
                        thread_id=f"thread-{worker_id}-{index}",
                    )
                    with lock:
                        sent_sequences.append(item["sequence"])
            except BaseException as exc:
                with lock:
                    errors.append(exc)

        for worker_id in range(4):
            thread = threading.Thread(target=write_batch, args=(worker_id,))
            workers.append(thread)
            thread.start()
        for thread in workers:
            thread.join(timeout=10)

        self.assertFalse(any(thread.is_alive() for thread in workers), "writer thread hung")
        self.assertEqual(errors, [])
        self.assertEqual(len(sent_sequences), 48)
        self.assertEqual(len(set(sent_sequences)), 48)
        self.assertEqual(sorted(sent_sequences), list(range(1, 49)))

    def test_managed_connection_factory_keeps_its_journal_mode(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            path = Path(tempdir) / "managed.db"
            bus = TogetherBus(connection_factory=lambda: sqlite3.connect(path, timeout=5))
            conn = sqlite3.connect(path)
            try:
                mode = conn.execute("PRAGMA journal_mode").fetchone()[0].lower()
            finally:
                conn.close()

        self.assertEqual(mode, "delete")
        self.assertIsNotNone(bus)

    def test_display_names_reject_terminal_control_characters(self) -> None:
        with self.assertRaises(BusError):
            self.bus.rename(self.room, "alice", "agent" + chr(27) + "\\x1b[31m")

    def test_default_room_preserves_case_for_posix_workspace_paths(self) -> None:
        self.assertNotEqual(default_room_id("/work/Project"), default_room_id("/work/project"))

    def test_default_room_is_stable_for_the_same_workspace_and_separate_for_another(self) -> None:
        self.assertEqual(default_room_id("C:/work/project"), default_room_id("c:\\WORK\\project"))
        self.assertNotEqual(default_room_id("C:/work/project"), default_room_id("C:/work/other"))


if __name__ == "__main__":
    unittest.main()
