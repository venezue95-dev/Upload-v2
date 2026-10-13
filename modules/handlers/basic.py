"""
basic.py
========
Comandos generales: /start, /menu, /help, /status y avisos de acceso.
"""

from __future__ import annotations

import logging
import time
from typing import Dict

from pyrogram import Client, filters
from pyrogram.types import Message

from .. import storage, views
from .common import AUTHORIZED, BANNED, DENIED, is_owner, reply, spawn, username_of

log = logging.getLogger("s3_bot.basic")

_DENIED_NOTICE: Dict[int, float] = {}


def register(app: Client) -> None:
    @app.on_message(filters.command(["start", "menu"]) & AUTHORIZED)
    async def _start(client: Client, message: Message):
        user = message.from_user
        storage.touch_user(user.id, username_of(user))
        name = await views.owner_display_name(client)
        await reply(message, *views.home_view(user.id, is_owner_user=is_owner(user.id),
                                              admin_name=name))

    @app.on_message(filters.command("help") & AUTHORIZED)
    async def _help(client: Client, message: Message):
        await reply(message, *views.help_view(1))

    @app.on_message(filters.command("status") & AUTHORIZED)
    async def _status(client: Client, message: Message):
        async def work():
            note = await reply(message, "🔍 <b>Comprobando el sistema…</b>")
            view = await views.status_view(client)
            if note is not None:
                text, keyboard = view
                await note.edit_text(text, reply_markup=keyboard,
                                     disable_web_page_preview=True)

        spawn(work())     # el sondeo a toDus puede tardar hasta 12 s

    @app.on_message(BANNED)
    async def _banned(client: Client, message: Message):
        await reply(
            message,
            "⛔️ <b>Tu cuenta está bloqueada en este bot</b>\n\n"
            "Si crees que es un error, contacta con el administrador.",
        )

    @app.on_message(DENIED)
    async def _denied(client: Client, message: Message):
        """Bot privado: se avisa (como mucho una vez por minuto) en vez de ignorar."""
        user_id = message.from_user.id
        now = time.time()
        if now - _DENIED_NOTICE.get(user_id, 0) < 60:
            return
        _DENIED_NOTICE[user_id] = now
        await reply(message, "🔒 <b>Este bot es privado</b>\n\n"
                             "Pide acceso al administrador y dale tu ID: "
                             f"<code>{user_id}</code>")
