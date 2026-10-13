"""
settings.py
===========
/settings — panel de configuración en caliente (la pantalla vive en ``views``).

El owner cambia los valores con un toque (ciclan al siguiente valor); el resto
de usuarios solo los ve, sin botones de edición. Todo se guarda en
settings.json y se aplica a las tareas nuevas al instante.
"""

from __future__ import annotations

from pyrogram import Client, filters
from pyrogram.types import Message

from .. import views
from .common import AUTHORIZED, is_owner, reply


def register(app: Client) -> None:
    @app.on_message(filters.command("settings") & AUTHORIZED)
    async def _settings(client: Client, message: Message):
        text, keyboard = views.settings_view(1, is_owner_user=is_owner(message.from_user.id))
        await reply(message, text, keyboard)
