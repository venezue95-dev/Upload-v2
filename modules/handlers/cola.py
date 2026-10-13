"""
cola.py
=======
Comandos de la cola y el historial del usuario:

- /cola        → tus tareas (toca una para ver sus acciones).
- /cancelar    → cancela todas tus tareas (con confirmación).
- /historial   → tus subidas completadas.
- /buscar      → busca en tu historial por nombre.
- /reintentar  → reintenta una tarea fallida o cancelada.
- /enlaces     → reenvía la lista de enlaces de una tarea.

Las pantallas las construye ``views``; son las mismas que usan los botones.
"""

from __future__ import annotations

import logging

from pyrogram import Client, filters
from pyrogram.types import Message

from .. import actions, storage, views
from .. import texts as T
from ..notifier import send_links_txt
from .common import AUTHORIZED, is_owner, reply

log = logging.getLogger("s3_bot.cola")


def _arg(message: Message) -> str:
    return (message.text or "").partition(" ")[2].strip()


def register(app: Client) -> None:
    @app.on_message(filters.command(["cola", "queue"]) & AUTHORIZED)
    async def _cola(client: Client, message: Message):
        user_id = message.from_user.id
        text, keyboard = views.queue_view(user_id, "m", 1, is_owner_user=is_owner(user_id))
        await reply(message, text, keyboard)

    @app.on_message(filters.command(["historial", "hist"]) & AUTHORIZED)
    async def _historial(client: Client, message: Message):
        text, keyboard = views.history_view(message.from_user.id, 1)
        await reply(message, text, keyboard)

    @app.on_message(filters.command(["cancelar", "cancel"]) & AUTHORIZED)
    async def _cancelar(client: Client, message: Message):
        view = views.cancel_all_view(message.from_user.id, "m", is_owner_user=False)
        if view is None:
            await reply(message, "📭 No tienes tareas en curso.")
            return
        await reply(message, *view)

    @app.on_message(filters.command("reintentar") & AUTHORIZED)
    async def _reintentar(client: Client, message: Message):
        user_id = message.from_user.id
        task_id = _arg(message).split(" ")[0]
        task = (actions.find_task(task_id, user_id, manage=is_owner(user_id)) if task_id
                else actions.last_user_task(user_id, only_retryable=True))
        if not task:
            await reply(
                message,
                "ℹ️ No encontré ninguna tarea que reintentar.\n"
                f"Uso: {T.code('/reintentar 1a2b3c4d5e')} (sin id = la última fallida o cancelada).\n"
                "También puedes pulsar <b>🔁 Reintentar</b> en el mensaje de la tarea.",
            )
            return
        ok, detail = await actions.retry_task(client, task)
        if not ok:
            await reply(message, T.esc(detail))

    @app.on_message(filters.command(["buscar", "search"]) & AUTHORIZED)
    async def _buscar(client: Client, message: Message):
        query = _arg(message)
        if not query:
            await reply(
                message,
                "Uso: " + T.code("/buscar pelicula") + "\n"
                + T.i("Busca en tu historial por nombre de archivo."),
            )
            return
        if not storage.search_user_history(message.from_user.id, query, limit=1):
            await reply(message, f"🔍 Sin resultados para {T.code(query)}.")
            return
        await reply(message, *views.search_view(message.from_user.id, query))

    @app.on_message(filters.command(["enlaces", "links"]) & AUTHORIZED)
    async def _enlaces(client: Client, message: Message):
        user_id = message.from_user.id
        task_id = _arg(message).split(" ")[0]
        task = (actions.find_task(task_id, user_id, manage=is_owner(user_id)) if task_id
                else actions.last_user_task(user_id, only_with_links=True))
        if not task or not task.has_links:
            await reply(message, "ℹ️ No encontré enlaces para esa tarea.")
            return
        await send_links_txt(client, message.chat.id, task)
