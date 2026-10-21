"""Pruebas de verificación del grupo requerido y cancelación de tareas al salir."""
from __future__ import annotations

import asyncio
import os
import sys
import unittest
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

os.environ.update({
    "API_ID": "12345",
    "API_HASH": "test-hash",
    "BOT_TOKEN": "123:test",
})

from config import CONFIG  # noqa: E402
from modules.handlers import group_access  # noqa: E402
from modules import notifier  # noqa: E402
from modules.handlers.group_access import (  # noqa: E402
    _is_private_chat,
    handle_group_member_update,
    is_user_in_group,
    verify_group_join,
)


class FakeClient:
    def __init__(self, member=None, error=None):
        self.member = member
        self.error = error
        self.request = None
        self.sent_messages = []

    async def get_chat_member(self, chat_id, user_id):
        self.request = (chat_id, user_id)
        if self.error:
            raise self.error
        return self.member

    async def send_message(self, chat_id, text, **kwargs):
        self.sent_messages.append((chat_id, text, kwargs))
        return SimpleNamespace(id=len(self.sent_messages))


class GroupAccessTests(unittest.TestCase):
    def test_group_member_statuses(self):
        self.assertTrue(is_user_in_group(SimpleNamespace(name="MEMBER")))
        self.assertTrue(is_user_in_group(SimpleNamespace(name="ADMINISTRATOR")))
        self.assertTrue(is_user_in_group(SimpleNamespace(name="OWNER")))
        self.assertTrue(is_user_in_group(SimpleNamespace(name="RESTRICTED"), True))
        self.assertFalse(is_user_in_group(SimpleNamespace(name="RESTRICTED"), False))
        self.assertFalse(is_user_in_group(SimpleNamespace(name="LEFT")))
        self.assertFalse(is_user_in_group(SimpleNamespace(name="BANNED")))

    def test_hardcoded_group_configuration(self):
        CONFIG.validate_required_group()
        self.assertEqual(CONFIG.required_group_id, -1003941841527)
        self.assertEqual(CONFIG.required_group_url, "https://t.me/todusupload")

    def test_only_private_chats_are_handled(self):
        private = SimpleNamespace(type=SimpleNamespace(name="PRIVATE"))
        group = SimpleNamespace(type=SimpleNamespace(name="SUPERGROUP"))
        self.assertTrue(_is_private_chat(private))
        self.assertFalse(_is_private_chat(group))

    def test_missing_required_group_fails_validation(self):
        incomplete = replace(CONFIG, required_group_id=None)
        with self.assertRaisesRegex(ValueError, "REQUIRED_GROUP_ID"):
            incomplete.validate_required_group()

    def test_group_verification_passes_joined_user(self):
        client = FakeClient(SimpleNamespace(status=SimpleNamespace(name="MEMBER")))
        joined, verified = asyncio.run(verify_group_join(client, 456))
        self.assertTrue(joined)
        self.assertTrue(verified)
        self.assertEqual(client.request, (-1003941841527, 456))

    def test_group_verification_blocks_user_who_left(self):
        client = FakeClient(SimpleNamespace(status=SimpleNamespace(name="LEFT")))
        joined, verified = asyncio.run(verify_group_join(client, 456))
        self.assertFalse(joined)
        self.assertTrue(verified)

    def test_group_verification_treats_not_participant_as_confirmed_absence(self):
        error_type = type("UserNotParticipant", (RuntimeError,), {})
        client = FakeClient(error=error_type())
        joined, verified = asyncio.run(verify_group_join(client, 456))
        self.assertFalse(joined)
        self.assertTrue(verified)

    def test_group_verification_fails_closed_on_other_telegram_error(self):
        client = FakeClient(error=RuntimeError("verification unavailable"))
        joined, verified = asyncio.run(verify_group_join(client, 456))
        self.assertFalse(joined)
        self.assertFalse(verified)

    def test_leaving_required_group_cancels_tasks_and_notifies_privately(self):
        user = SimpleNamespace(id=456)
        event = SimpleNamespace(
            chat=SimpleNamespace(id=-1003941841527, username="todusupload"),
            old_chat_member=SimpleNamespace(
                status=SimpleNamespace(name="MEMBER"), user=user
            ),
            new_chat_member=SimpleNamespace(
                status=SimpleNamespace(name="LEFT"), user=user
            ),
        )
        client = FakeClient()
        with patch.object(group_access.QUEUE, "cancel_user", return_value=2) as cancel:
            count = asyncio.run(handle_group_member_update(client, event))

        self.assertEqual(count, 2)
        cancel.assert_called_once_with(456, reason=group_access.EXIT_REASON)
        self.assertEqual(client.sent_messages[0][0], 456)
        self.assertIn("Marqué 2 tareas", client.sent_messages[0][1])
        self.assertIn("@todusupload", client.sent_messages[0][1])

    def test_no_cancel_for_other_group_or_still_joined_user(self):
        user = SimpleNamespace(id=456)
        client = FakeClient()
        other_group_event = SimpleNamespace(
            chat=SimpleNamespace(id=-1001111111111, username="another_group"),
            old_chat_member=SimpleNamespace(
                status=SimpleNamespace(name="MEMBER"), user=user
            ),
            new_chat_member=SimpleNamespace(
                status=SimpleNamespace(name="LEFT"), user=user
            ),
        )
        still_joined_event = SimpleNamespace(
            chat=SimpleNamespace(id=-1003941841527, username="todusupload"),
            old_chat_member=SimpleNamespace(
                status=SimpleNamespace(name="MEMBER"), user=user
            ),
            new_chat_member=SimpleNamespace(
                status=SimpleNamespace(name="ADMINISTRATOR"), user=user
            ),
        )
        with patch.object(group_access.QUEUE, "cancel_user", return_value=2) as cancel:
            self.assertEqual(asyncio.run(handle_group_member_update(client, other_group_event)), 0)
            self.assertEqual(asyncio.run(handle_group_member_update(client, still_joined_event)), 0)
        cancel.assert_not_called()
        self.assertEqual(client.sent_messages, [])

    def test_notifier_never_sends_status_to_group_chat(self):
        from modules.queue import UploadTask

        task = UploadTask(
            user_id=456,
            chat_id=-1003941841527,
            message_id=10,
            filename="video.mp4",
            file_size=100,
            source="url",
            url="https://example.com/video.mp4",
        )
        task.status_message_id = 20
        client = FakeClient()
        with patch.object(notifier, "_client", client):
            edited = asyncio.run(notifier.edit_status(task, "test"))
            asyncio.run(notifier.send_status(task, "test"))
        self.assertFalse(edited)
        self.assertEqual(client.sent_messages, [])


if __name__ == "__main__":
    unittest.main(verbosity=2)
