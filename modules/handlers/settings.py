"""
settings.py
===========
/settings — panel de configuración en caliente (2 páginas).

El owner cambia los valores con un toque (ciclan al siguiente valor); el
resto de usuarios solo pueden verlos. Todo se guarda en settings.json y se
aplica a las tareas nuevas al instante (la cola lee los valores en vivo).
"""

from __future__ import annotations

from typing import Tuple

from pyrogram import Client, filters
from pyrogram.types import Message

from .. import texts as T
from ..runtime_settings import (
    CYCLE_LABELS,
    CYCLE_UNITS,
    RUNTIME_SETTINGS,
    SETTINGS_PAGES,
)
from .common import AUTHORIZED, is_owner, reply


def build_settings_view(page: int, *, is_owner_user: bool) -> Tuple[str, object]:
    pages = len(SETTINGS_PAGES)
    page = max(1, min(page, pages))
    keys = SETTINGS_PAGES[page - 1]
    values = RUNTIME_SETTINGS.as_dict()
    text = T.settings_text(
        values, page, pages,
        is_owner_user=is_owner_user,
        keys=keys,
        labels=CYCLE_LABELS,
        units=CYCLE_UNITS,
    )
    keyboard = T.settings_keyboard(
        keys, values, CYCLE_LABELS, CYCLE_UNITS,
        page, pages, is_owner_user=is_owner_user,
    )
    return text, keyboard


def register(app: Client) -> None:
    @app.on_message(filters.command("settings") & AUTHORIZED)
    async def _settings(client: Client, message: Message):
        is_own = is_owner(message.from_user.id)
        text, keyboard = build_settings_view(1, is_owner_user=is_own)
        await reply(message, text, keyboard)
