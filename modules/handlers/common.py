"""
common.py
=========
Piezas compartidas por todos los handlers: permisos, filtros, envío de
mensajes y la función de encolado (que aplica cupos, límites y responde
con el mensaje de cola).
"""

from __future__ import annotations

import asyncio
import logging
from typing import Optional, Set, Tuple

from pyrogram import filters
from pyrogram.enums import ParseMode
from pyrogram.types import InlineKeyboardMarkup, Message

from config import CONFIG
from .. import keyboards as K
from .. import storage
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
        # Los usuarios baneados no pasan ningún filtro (ni comandos ni envíos).
        return bool(user and is_authorized(user.id) and not storage.is_banned(user.id))

    return filters.create(_flt, "AuthorizedFilter")


def _banned_filter():
    async def _flt(_, __, message: Message) -> bool:
        user = getattr(message, "from_user", None)
        return bool(user and storage.is_banned(user.id))

    return filters.create(_flt, "BannedFilter")


def _denied_filter():
    """Mensajes privados de usuarios que NO están en BOT_ALLOWED_USERS (y no baneados)."""
    async def _flt(_, __, message: Message) -> bool:
        user = getattr(message, "from_user", None)
        chat = getattr(message, "chat", None)
        is_private = getattr(getattr(chat, "type", None), "name", "PRIVATE") == "PRIVATE"
        return bool(
            user and is_private and not is_authorized(user.id) and not storage.is_banned(user.id)
        )

    return filters.create(_flt, "DeniedFilter")


def _owner_filter():
    async def _flt(_, __, message: Message) -> bool:
        user = getattr(message, "from_user", None)
        return bool(user and can_manage(user.id))

    return filters.create(_flt, "OwnerFilter")


AUTHORIZED = _authorized_filter()
OWNER = _owner_filter()
BANNED = _banned_filter()
DENIED = _denied_filter()


# ----------------------------------------------------------------------
# Trabajo en segundo plano.
# ----------------------------------------------------------------------
_BACKGROUND: Set[asyncio.Task] = set()


def spawn(coro) -> asyncio.Task:
    """
    Lanza una corrutina sin bloquear al trabajador de Pyrogram.

    Cada handler ocupa un trabajador hasta que termina; si 12 usuarios pegan
    enlaces a la vez y cada handler espera 25 s de sondeo, el bot dejaría de
    responder (incluidos los botones). Los handlers lentos se delegan aquí.
    """
    task = asyncio.get_running_loop().create_task(coro)
    _BACKGROUND.add(task)

    def _done(t: asyncio.Task) -> None:
        _BACKGROUND.discard(t)
        if not t.cancelled() and t.exception():
            log.error("Tarea en segundo plano falló: %r", t.exception())

    task.add_done_callback(_done)
    return task


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
            T.premiumize(text),
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
            T.premiumize(text),
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
async def check_limits(user_id: int, message: Message,
                       incoming_bytes: int = 0) -> bool:
    """
    Puertas de entrada antes de crear una tarea: mantenimiento, baneo y
    anti-spam. La reserva atómica de cuota y los cupos de cola se aplican al
    crear/admitir la tarea.
    True = puede continuar.
    """
    owner = can_manage(user_id)
    if RUNTIME_SETTINGS.get("maintenance", False) and not owner:
        await reply(
            message,
            "🛠 <b>Mantenimiento en curso</b>\n\n"
            "El bot está bloqueando envíos nuevos unos minutos.\n"
            f"{T.i('Vuelve a intentarlo más tarde 🙏')}",
        )
        return False

    if storage.is_banned(user_id):
        await reply(
            message,
            "⛔️ <b>Tu cuenta está bloqueada</b>\n\n"
            "Contacta con el administrador del bot si crees que es un error.",
        )
        return False

    remaining = None if owner else QUEUE.check_rate_limit(user_id)
    if remaining is not None:
        await reply(
            message,
            f"🐢 <b>Ve más despacio</b>\n\n"
            f"Espera {T.code(str(remaining) + ' s')} antes de enviar otra tarea.",
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
    media_format: str = "mp4",
    media_quality: str = "720",
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
    task.media_format = media_format
    task.media_quality = media_quality
    task.requested_chunk_mb = int(storage.get_user_chunk_size(user.id) or 0)

    allowed, used, limit = storage.reserve_quota(
        user.id, task.task_id, file_size, owner=is_owner(user.id)
    )
    if not allowed:
        from ..progress import human_size
        return None, (
            "📊 <b>Cuota diaria superada</b>\n\n"
            f"• Usado o reservado: {T.b(human_size(used))}\n"
            f"• Límite diario: {T.b(human_size(limit))}\n"
            f"• Este archivo: {T.b(human_size(file_size))}\n\n"
            f"{T.i('La cuota se libera al completar 24 horas.')}"
        )

    ok, reason = QUEUE.submit(task)
    if not ok:
        storage.release_quota(task.task_id)
        return None, reason

    msg = await reply(
        message,
        T.queued_text(task),
        keyboard=K.task_message_keyboard(task),
    )
    if msg is not None:
        task.status_message_id = msg.id
    return task, None
