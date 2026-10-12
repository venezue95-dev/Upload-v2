"""
common.py
=========
Piezas compartidas por todos los handlers: permisos, filtros, envío de
mensajes y la función de encolado (que aplica cupos, límites y responde
con el mensaje de cola).
"""

from __future__ import annotations

import logging
from typing import Optional, Tuple

from pyrogram import filters
from pyrogram.enums import ParseMode
from pyrogram.types import InlineKeyboardMarkup, Message

from config import CONFIG
from .. import texts as T
from ..queue import QUEUE, UploadTask
from ..runtime_settings import RUNTIME_SETTINGS

log = logging.getLogger("s3_bot.handlers")


# ----------------------------------------------------------------------
# Permisos.
# ----------------------------------------------------------------------
def is_authorized(user_id: int) -> bool:
    if not CONFIG.allowed_users:
        return True
    return user_id in CONFIG.allowed_users


def is_owner(user_id: int) -> bool:
    return CONFIG.owner_id is not None and user_id == CONFIG.owner_id


def can_manage(user_id: int) -> bool:
    """Puede gestionar la cola global: únicamente el owner configurado."""
    return is_owner(user_id)


def _authorized_filter():
    async def _flt(_, __, message: Message) -> bool:
        user = getattr(message, "from_user", None)
        return bool(user and is_authorized(user.id))

    return filters.create(_flt, "AuthorizedFilter")


def _owner_filter():
    async def _flt(_, __, message: Message) -> bool:
        user = getattr(message, "from_user", None)
        return bool(user and can_manage(user.id))

    return filters.create(_flt, "OwnerFilter")


AUTHORIZED = _authorized_filter()
OWNER = _owner_filter()


def username_of(user) -> str:
    if user is None:
        return ""
    return user.username or user.first_name or str(user.id)


# ----------------------------------------------------------------------
# Envío de mensajes.
# ----------------------------------------------------------------------
async def reply(message: Message, text: str, keyboard: Optional[InlineKeyboardMarkup] = None) -> Optional[Message]:
    try:
        return await message.reply_text(
            text,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
            reply_markup=keyboard,
        )
    except Exception as exc:  # noqa: BLE001
        log.warning("No se pudo responder a %s: %s", message.chat.id, exc)
        return None


async def edit(message: Message, text: str, keyboard: Optional[InlineKeyboardMarkup] = None) -> bool:
    try:
        await message.edit_text(
            text,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
            reply_markup=keyboard,
        )
        return True
    except Exception as exc:  # noqa: BLE001
        if "not modified" in str(exc).lower():
            return True
        log.debug("Edición fallida: %s", exc)
        return False


# ----------------------------------------------------------------------
# Encolado de tareas.
# ----------------------------------------------------------------------
async def check_limits(user_id: int, message: Message) -> bool:
    """Anti-spam y cupos básicos antes de crear tareas. True = continuar."""
    remaining = QUEUE.check_rate_limit(user_id)
    if remaining is not None:
        await reply(
            message,
            f"🐢 <b>Ve más despacio</b>\n\n"
            f"Espera {T.code(str(remaining) + ' s')} antes de enviar otra tarea.",
        )
        return False

    active, waiting = QUEUE.user_load(user_id)
    if active + waiting >= RUNTIME_SETTINGS.max_active_per_user + RUNTIME_SETTINGS.max_queue_per_user:
        await reply(
            message,
            f"🚦 <b>Límite alcanzado</b>\n\n"
            f"Ya tienes {T.b(active + waiting)} tareas "
            f"({active} activas + {waiting} en espera).\n"
            f"Espera a que terminen o usa /cancelar.",
        )
        return False
    return True


async def enqueue(
    client,
    message: Message,
    *,
    filename: str,
    file_size: int,
    source: str,
    url: Optional[str] = None,
    silent: bool = False,
) -> Tuple[Optional[UploadTask], Optional[str]]:
    """
    Crea la tarea, la entrega a la cola y publica el mensaje de estado.

    Devuelve ``(tarea, None)`` si entró, o ``(None, motivo)`` si fue rechazada
    (el motivo ya viene formateado en HTML para responder al usuario).
    """
    user = message.from_user
    task = UploadTask(
        user_id=user.id,
        chat_id=message.chat.id,
        message_id=message.id,
        filename=filename,
        file_size=file_size,
        source=source,
        url=url,
        username=username_of(user),
        priority=CONFIG.queue.owner_priority if is_owner(user.id) else 0,
        max_attempts=1 + int(CONFIG.queue.auto_retries),
    )

    ok, reason = QUEUE.submit(task)
    if not ok:
        return None, reason

    msg = await reply(
        message,
        T.queued_text(task),
        keyboard=T.progress_keyboard(task.task_id),
    )
    if msg is not None:
        task.status_message_id = msg.id
    return task, None
