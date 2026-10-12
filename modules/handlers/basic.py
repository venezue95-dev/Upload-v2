"""
basic.py
========
Comandos generales: /start, /menu, /help, /status, /ping.
"""

from __future__ import annotations

import asyncio
import logging
import time

from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pyrogram.types import Message

from config import CONFIG
from .. import texts as T
from ..queue import QUEUE
from ..runtime_settings import RUNTIME_SETTINGS
from .common import AUTHORIZED, BANNED, is_owner, reply

log = logging.getLogger("s3_bot.basic")

_start_ts = time.time()


def register(app: Client) -> None:
    @app.on_message(filters.command(["start", "menu"]) & AUTHORIZED)
    async def _start(client: Client, message: Message):
        user = message.from_user
        is_own = is_owner(user.id)
        admin_name = str(CONFIG.owner_id or "no configurado")
        if CONFIG.owner_id:
            try:
                owner = await client.get_users(CONFIG.owner_id)
                admin_name = (
                    f"@{owner.username}" if owner.username
                    else (owner.first_name or admin_name)
                )
            except Exception:
                pass
        await reply(
            message,
            T.welcome_text(
                user.id,
                public=not CONFIG.allowed_users,
                is_owner=is_own,
                cfg=CONFIG,
                settings=RUNTIME_SETTINGS,
                admin_name=admin_name,
            ),
            keyboard=T.main_menu_keyboard(is_owner_user=is_own),
        )

    @app.on_message(filters.command("help") & AUTHORIZED)
    async def _help(client: Client, message: Message):
        await reply(message, T.help_text(1), keyboard=T.help_keyboard(1))

    @app.on_message(BANNED)
    async def _banned(client: Client, message: Message):
        """Aviso a usuarios bloqueados (una vez por mensaje, sin más trámite)."""
        await reply(
            message,
            "⛔️ <b>Tu cuenta está bloqueada en este bot</b>\n\n"
            "Si crees que es un error, contacta con el administrador.",
        )

    @app.on_message(filters.command("ping") & AUTHORIZED)
    async def _ping(client: Client, message: Message):
        start = time.perf_counter()
        sent = await message.reply_text("🏓 ...")
        elapsed = int((time.perf_counter() - start) * 1000)
        counts = QUEUE.counts()
        await sent.edit_text(
            T.ping_text(elapsed, counts),
            parse_mode=ParseMode.HTML,
            reply_markup=T.main_menu_keyboard(is_owner_user=is_owner(message.from_user.id)),
        )

    @app.on_message(filters.command("status") & AUTHORIZED)
    async def _status(client: Client, message: Message):
        note = await message.reply_text(
            "🔍 <b>Comprobando el sistema…</b>",
            reply_markup=T.main_menu_keyboard(is_owner_user=is_owner(message.from_user.id)),
        )

        try:
            me = await client.get_me()
            bot_line = f"✅ {T.code('@' + (me.username or str(me.id)))} en línea"
        except Exception as exc:  # noqa: BLE001
            bot_line = f"⚠️ {T.esc(T.shorten(str(exc), 80))}"

        ok, detail = await _ping_storage()
        storage_line = f"{'✅' if ok else '❌'} {T.esc(T.shorten(detail, 90))}"

        counts = QUEUE.counts()
        from ..speedstats import SPEEDS

        await note.edit_text(
            T.status_text(
                bot_line=bot_line,
                storage_line=storage_line,
                settings=RUNTIME_SETTINGS,
                counts=counts,
                paused=QUEUE.paused,
                speeds_line=SPEEDS.summary(),
            ),
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
            reply_markup=T.main_menu_keyboard(is_owner_user=is_owner(message.from_user.id)),
        )


async def _ping_storage(timeout: float = 12.0):
    """Comprueba el endpoint de toDus sin bloquear el event loop."""
    try:
        from ..pipeline import s3

        return await asyncio.wait_for(
            asyncio.to_thread(lambda: s3().ping()), timeout=timeout
        )
    except asyncio.TimeoutError:
        return False, "el almacenamiento no respondió a tiempo"
    except Exception as exc:  # noqa: BLE001
        return False, str(exc)
