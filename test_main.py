import asyncio
from pathlib import Path
import tempfile
import types
import unittest
from unittest.mock import AsyncMock, patch

import main


class FloodWaitError(Exception):
    seconds = 120


class StopTest(Exception):
    pass


class SenderTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.folder = Path(self.temp.name)
        (self.folder / "photos").mkdir()
        (self.folder / "chats.txt").write_text("@one\n@two\n@one\n# comment\n", encoding="utf-8")
        (self.folder / "message.txt").write_text("message", encoding="utf-8")
        for name in ("c.PNG", "a.jpg", "b.webp", "d.png"):
            (self.folder / "photos" / name).write_bytes(b"fake-photo")
        self.path = self.folder / "state.json"

    def test_materials_snapshot_and_deduplication(self):
        chats, message, photos = main.load_materials(self.folder)
        self.assertEqual(chats, ["@one", "@two"])
        self.assertEqual(message, "message")
        self.assertEqual([photo.name for photo in photos], ["a.jpg", "b.webp", "c.PNG"])
        (self.folder / "photos" / "a.jpg").unlink()
        self.assertEqual(photos[0].getvalue(), b"fake-photo")

    def test_material_updates(self):
        main.load_materials(self.folder)
        (self.folder / "message.txt").write_text("new", encoding="utf-8")
        (self.folder / "chats.txt").write_text("@new", encoding="utf-8")
        chats, message, _ = main.load_materials(self.folder)
        self.assertEqual((chats, message), (["@new"], "new"))

    def test_missing_photos_rejected(self):
        for path in list((self.folder / "photos").iterdir())[:2]:
            path.unlink()
        with self.assertRaises(ValueError):
            main.load_materials(self.folder)

    def test_state_roundtrip_and_permissions(self):
        state = main.new_state()
        state["sent"] = ["@one"]
        main.save_state(self.path, state)
        self.assertEqual(main.load_state(self.path), state)
        self.assertEqual(self.path.stat().st_mode & 0o777, 0o600)

    def test_corrupt_state_fails_closed(self):
        self.path.write_text('{"version": 99}', encoding="utf-8")
        with self.assertRaises(ValueError):
            main.load_state(self.path)

    def test_lock_rejects_second_process(self):
        with main.exclusive_lock(self.folder):
            with self.assertRaises(RuntimeError):
                with main.exclusive_lock(self.folder):
                    pass

    def run_sender(self, client):
        errors = types.ModuleType("telethon.errors")
        setattr(errors, "FloodWaitError", FloodWaitError)
        original_loader = main.load_materials
        with patch.dict("sys.modules", {"telethon.errors": errors}), \
                patch.object(main, "load_materials", side_effect=lambda: original_loader(self.folder)), \
                patch.object(main, "notify", new=AsyncMock()), \
                patch.object(main.time, "time", return_value=1000), \
                patch.object(main.random, "randint", return_value=3600), \
                patch.object(main.asyncio, "sleep", new=AsyncMock(side_effect=StopTest)):
            asyncio.run(main.run_sender(client, self.path))

    def test_success_saves_single_pause(self):
        client = types.SimpleNamespace(send_file=AsyncMock())
        with self.assertRaises(StopTest):
            self.run_sender(client)
        client.send_file.assert_awaited_once()
        state = main.load_state(self.path)
        self.assertEqual(state["sent"], ["@one"])
        self.assertEqual(state["next_at"], 4600)
        self.assertIsNone(state["in_flight"])

    def test_restart_skips_delivered_chat(self):
        state = main.new_state()
        state["sent"] = ["@one"]
        main.save_state(self.path, state)
        client = types.SimpleNamespace(send_file=AsyncMock())
        with self.assertRaises(StopTest):
            self.run_sender(client)
        self.assertEqual(client.send_file.await_args.args[0], "@two")
        state = main.load_state(self.path)
        self.assertEqual(state["next_at"], 1000 + main.CYCLE_SLEEP)
        self.assertEqual(state["sent"], [])

    def test_network_failure_keeps_marker_and_blocks_restart(self):
        client = types.SimpleNamespace(send_file=AsyncMock(side_effect=OSError("network")))
        with self.assertRaises(RuntimeError):
            self.run_sender(client)
        self.assertEqual(main.load_state(self.path)["in_flight"], "@one")
        client.send_file.reset_mock()
        with self.assertRaises(RuntimeError):
            self.run_sender(client)
        client.send_file.assert_not_awaited()

    def test_flood_wait_is_persisted(self):
        client = types.SimpleNamespace(send_file=AsyncMock(side_effect=FloodWaitError()))
        with self.assertRaises(StopTest):
            self.run_sender(client)
        state = main.load_state(self.path)
        self.assertEqual(state["next_at"], 1120)
        self.assertEqual(state["sent"], [])
        self.assertIsNone(state["in_flight"])

    def test_missing_materials_does_not_send(self):
        (self.folder / "message.txt").unlink()
        client = types.SimpleNamespace(send_file=AsyncMock())
        with self.assertRaises(StopTest):
            self.run_sender(client)
        client.send_file.assert_not_awaited()

    def test_login_never_starts_sender(self):
        client = types.SimpleNamespace(start=AsyncMock(), disconnect=AsyncMock())
        with patch.object(main, "make_client", return_value=client), \
                patch.object(main, "run_sender", new=AsyncMock()) as sender:
            asyncio.run(main.telegram_command("login"))
        client.start.assert_awaited_once()
        client.disconnect.assert_awaited_once()
        sender.assert_not_awaited()

    def test_run_requires_existing_authorization(self):
        client = types.SimpleNamespace(
            connect=AsyncMock(), is_user_authorized=AsyncMock(return_value=False),
            disconnect=AsyncMock(), start=AsyncMock()
        )
        with patch.object(main, "make_client", return_value=client), \
                patch.object(main, "run_sender", new=AsyncMock()) as sender:
            with self.assertRaises(RuntimeError):
                asyncio.run(main.telegram_command("run"))
        client.start.assert_not_awaited()
        client.disconnect.assert_awaited_once()
        sender.assert_not_awaited()

    def test_resolve_sent_saves_progress_without_telegram(self):
        state = main.new_state()
        state["in_flight"] = "@one"
        main.save_state(self.path, state)
        with patch.object(main, "DATA", self.folder), \
                patch("sys.argv", ["main.py", "resolve", "--outcome", "sent"]), \
                patch("builtins.print"), patch.object(main, "make_client") as factory:
            main.main()
        factory.assert_not_called()
        state = main.load_state(self.path)
        self.assertEqual(state["sent"], ["@one"])
        self.assertIsNone(state["in_flight"])

    def test_resolve_retry_keeps_chat_pending(self):
        state = main.new_state()
        state["in_flight"] = "@one"
        main.save_state(self.path, state)
        with patch.object(main, "DATA", self.folder), \
                patch("sys.argv", ["main.py", "resolve", "--outcome", "retry"]), \
                patch("builtins.print"):
            main.main()
        state = main.load_state(self.path)
        self.assertEqual(state["sent"], [])
        self.assertIsNone(state["in_flight"])


if __name__ == "__main__":
    unittest.main()
