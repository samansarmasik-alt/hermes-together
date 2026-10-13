from __future__ import annotations

import multiprocessing
import queue
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch

from hermes_together.bus import TogetherBus
from hermes_together.plugin import register


class ProcessContext:
    def __init__(self, incoming) -> None:
        self._manager = types.SimpleNamespace(_cli_ref=types.SimpleNamespace())
        self.commands = {}
        self.tools = {}
        self.hooks = {}
        self.incoming = incoming

    def register_command(self, name, handler, description="", args_hint="") -> None:
        self.commands[name] = handler

    def register_tool(self, name, toolset, schema, handler, check_fn=None) -> None:
        self.tools[name] = {"schema": schema, "handler": handler, "check_fn": check_fn}

    def register_hook(self, name, handler) -> None:
        self.hooks[name] = handler

    def inject_message(self, content, role="user", **kwargs) -> bool:
        self.incoming.put(content)
        return True


def _run_cli_process(index, db_path, barrier, incoming, result_queue) -> None:
    context = ProcessContext(incoming)
    try:
        with patch(
            "hermes_together.plugin.create_bus",
            side_effect=lambda: TogetherBus(db_path),
        ):
            register(context)
        joined = context.commands["together"]("join four-cli-test")
        result_queue.put(("joined", index, joined))
        barrier.wait(timeout=15)
        if index == 0:
            result = context.commands["together"]("send message from cli zero")
            result_queue.put(("sent", index, result))
        else:
            message = incoming.get(timeout=12)
            result_queue.put(("received", index, message))
    except BaseException as exc:
        result_queue.put(("error", index, f"{type(exc).__name__}: {exc}"))
    finally:
        try:
            context.commands.get("together", lambda _raw: None)("leave")
        except Exception:
            pass


class SeparateProcessPluginTests(unittest.TestCase):
    def test_four_independent_cli_processes_share_one_live_room(self) -> None:
        ctx = multiprocessing.get_context("spawn")
        with tempfile.TemporaryDirectory() as tempdir:
            db_path = str(Path(tempdir) / "shared.db")
            barrier = ctx.Barrier(4)
            result_queue = ctx.Queue()
            inboxes = [ctx.Queue() for _ in range(4)]
            processes = [
                ctx.Process(
                    target=_run_cli_process,
                    args=(index, db_path, barrier, inboxes[index], result_queue),
                )
                for index in range(4)
            ]
            for process in processes:
                process.start()
            for process in processes:
                process.join(timeout=25)

            self.assertFalse(any(process.is_alive() for process in processes), "CLI process did not stop")
            self.assertEqual([process.exitcode for process in processes], [0, 0, 0, 0])
            results = []
            for _ in range(8):
                try:
                    results.append(result_queue.get(timeout=1))
                except queue.Empty:
                    break

        errors = [item for item in results if item[0] == "error"]
        self.assertEqual(errors, [])
        joined = [item for item in results if item[0] == "joined"]
        received = [item for item in results if item[0] == "received"]
        sent = [item for item in results if item[0] == "sent"]
        self.assertEqual(len(joined), 4)
        self.assertEqual(len(received), 3)
        self.assertEqual(len(sent), 1)
        for _kind, index, text in received:
            self.assertNotEqual(index, 0)
            self.assertIn("message from cli zero", text)


if __name__ == "__main__":
    multiprocessing.freeze_support()
    unittest.main()
