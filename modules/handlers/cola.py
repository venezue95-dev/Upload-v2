"""
cola.py
=======
Vista de la cola y acciones del usuario:

- /cola        → tus tareas, con posición, ETA, progreso y botones.
- /cancelar    → cancela todas tus tareas (con confirmación).
- /historial   → tus últimas subidas completadas (con enlaces).
- /reintentar  → reintenta una tarea fallida o cancelada.
- /enlaces     → reenvía la lista de enlaces de una tarea.
"""

from __future__ import annotations

import logging
from typing import Optional, Tuple

from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pyrogram.types import Message

from config import CONFIG
from .. import storage, texts as T
from ..queue import QUEUE, UploadTask
from .common import AUTHORIZED, is_owner, reply

log = logging.getLogger("s3_bot.cola")

PER_PAGE = 6
HIST_PER_PAGE = 5


# ----------------------------------------------------------------------
# Construcción de vistas (usadas también por los botones).
# ----------------------------------------------------------------------
def build_queue_view(user_id: int, page: int, *, is_owner_user: bool,
                     global_view: bool = False) -> Tuple[str, object]:
    if global_view:
        tasks = QUEUE.all_tasks(limit=120)
    else:
        tasks = QUEUE.user_tasks(user_id, limit=60)
    text = T.queue_text(user_id, tasks, page, PER_PAGE, global_view=global_view)
    keyboard = T.queue_keyboard(tasks, page, PER_PAGE,
                                is_owner_user=is_owner_user,
                                global_view=global_view)
    return text, keyboard


def build_history_view(user_id: int, page: int) -> Tuple[str, object]:
    rows = storage.user_history(user_id, limit=100)
    pages = max(1, (len(rows) + HIST_PER_PAGE - 1) // HIST_PER_PAGE)
    page = max(1, min(page, pages))
    text = T.history_text(rows, page, HIST_PER_PAGE)
    keyboard = T.history_keyboard(page, pages, rows)
    return text, keyboard


async def retry_task(client, task: UploadTask, user_id: int) -> Tuple[bool, str]:
    """Reintenta una tarea fallida/cancelada creando una copia nueva."""
    if not task.is_final:
        return False, "ℹ️ Esa tarea todavía está en curso."

    if task.attempts >= 3:
        return False, (
            "⚠️ Esa tarea ya se reintentó 3 veces sin éxito.\n"
            "Revisa el detalle del error o vuelve a enviar el archivo."
        )

    if task.source == "telegram":
        if not task.message_id:
            return False, "⚠️ No se puede reintentar: falta el mensaje original."
    elif not task.url:
        return False, "⚠️ No se puede reintentar: la tarea no tiene URL."

    clone = UploadTask(
        user_id=task.user_id,
        chat_id=task.chat_id,
        message_id=task.message_id,
        filename=task.filename,
        file_size=task.file_size,
        source=task.source,
        url=task.url,
        username=task.username,
        priority=task.priority,
        max_attempts=1 + int(CONFIG.queue.auto_retries),
    )
    clone.attempts = task.attempts + 1

    ok, reason = QUEUE.submit(clone)
    if not ok:
        return False, reason

    msg = await client.send_message(
        task.chat_id or user_id,
        f"♻️ <b>Reintento encolado</b>\n"
        f"📄 {T.code(T.shorten(task.filename, 44))}\n"
        f"🆔 {T.code(clone.task_id)} · intento {clone.attempts}/3",
        parse_mode=ParseMode.HTML,
    )
    clone.status_message_id = msg.id
    return True, f"♻️ Reintento encolado ({clone.attempts}/3): {clone.task_id}"


def find_user_task(user_id: int, task_id: Optional[str]) -> Optional[UploadTask]:
    """Busca una tarea por id (o la última del usuario si no se da id)."""
    if task_id:
        task = QUEUE.get(task_id)
        if task and task.user_id == user_id:
            return task
        return None
    tasks = QUEUE.user_tasks(user_id, limit=30)
    for task in reversed(tasks):
        if task.is_final:
            return task
    return None


# ----------------------------------------------------------------------
# Handlers.
# ----------------------------------------------------------------------
def register(app: Client) -> None:
    @app.on_message(filters.command(["cola", "queue"]) & AUTHORIZED)
    async def _cola(client: Client, message: Message):
        is_own = is_owner(message.from_user.id)
        text, keyboard = build_queue_view(message.from_user.id, 1, is_owner_user=is_own)
        await reply(message, text, keyboard)

    @app.on_message(filters.command(["historial", "hist"]) & AUTHORIZED)
    async def _historial(client: Client, message: Message):
        text, keyboard = build_history_view(message.from_user.id, 1)
        await reply(message, text, keyboard)

    @app.on_message(filters.command(["cancelar", "cancel"]) & AUTHORIZED)
    async def _cancelar(client: Client, message: Message):
        pending = QUEUE.user_active_tasks(message.from_user.id)
        if not pending:
            await reply(message, "📭 No tienes tareas en curso.")
            return
        await reply(
            message,
            f"<b>🛑 ¿Cancelar tus {len(pending)} tarea(s) en curso?</b>\n\n"
            + "\n".join(
                f"• {T.code(T.shorten(t.filename, 38))} — {T.esc(T.STATUS_LABEL[t.status])}"
                for t in pending[:10]
            ),
            keyboard=T.cancel_all_keyboard(),
        )

    @app.on_message(filters.command("reintentar") & AUTHORIZED)
    async def _reintentar(client: Client, message: Message):
        parts = (message.text or "").split()
        task_id = parts[1].strip() if len(parts) > 1 else None
        task = find_user_task(message.from_user.id, task_id)
        if not task:
            await reply(
                message,
                "ℹ️ No encontré esa tarea.\n"
                f"Uso: {T.code('/reintentar 1a2b3c4d5e')} (o sin id = la última que falló).\n"
                "También puedes pulsar <b>🔁 Reintentar</b> en /cola.",
            )
            return
        ok, detail = await retry_task(client, task, message.from_user.id)
        await reply(message, T.esc(detail) if not ok else T.esc(detail))

    @app.on_message(filters.command(["enlaces", "links"]) & AUTHORIZED)
    async def _enlaces(client: Client, message: Message):
        from ..notifier import send_links_txt

        parts = (message.text or "").split()
        task_id = parts[1].strip() if len(parts) > 1 else None
        task = find_user_task(message.from_user.id, task_id)
        if not task or not task.urls:
            await reply(message, "ℹ️ No encontré enlaces para esa tarea.")
            return
        await send_links_txt(client, message.chat.id, task)
