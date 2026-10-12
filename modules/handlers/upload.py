"""
upload.py
=========
Handler de archivos enviados por Telegram (documento, vídeo, audio, voz).

Ya NO descarga ni sube nada aquí: solo valida, comprueba cupos y encola.
El trabajo lo hacen los trabajadores de la cola (modules/pipeline.py), de
modo que el handler termina en milisegundos y el bot sigue respondiendo a
todos los usuarios a la vez.

Soporta además **álbumes**: Pyrogram entrega un mensaje por archivo, así que
se agrupan por ``media_group_id`` y se encolan juntos.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Dict, List

from pyrogram import Client, filters
from pyrogram.types import Message

from .. import texts as T
from .common import AUTHORIZED, check_limits, enqueue, reply
from ..runtime_settings import RUNTIME_SETTINGS

log = logging.getLogger("s3_bot.upload")

MEDIA_FILTER = (
    filters.document
    | filters.video
    | filters.audio
    | filters.voice
    | filters.video_note
    | filters.photo          # fotos comprimidas: se sube la resolución mayor
)

# Agrupación de álbumes: media_group_id -> lista de mensajes + tarea de volcado.
_GROUPS: Dict[str, List[Message]] = {}
_GROUP_TASKS: Dict[str, asyncio.Task] = {}
_GROUP_LOCK = asyncio.Lock()
GROUP_WAIT = 1.5  # segundos de espera para juntar los archivos del álbum


def _media_of(message: Message):
    if message.photo:
        # Las fotos llegan como varias resoluciones: se usa la más grande.
        return max(message.photo.sizes, key=lambda size: getattr(size, "file_size", 0) or 0)
    return (
        message.document
        or message.video
        or message.audio
        or message.voice
        or message.video_note
    )


def _filename_of(message: Message) -> str:
    media = _media_of(message)
    name = getattr(media, "file_name", None)
    if name:
        return name
    if message.photo:
        return f"foto_{message.id}.jpg"
    if message.voice:
        return f"nota_de_voz_{message.id}.ogg"
    if message.video_note:
        return f"videonota_{message.id}.mp4"
    if message.video:
        return f"video_{message.id}.mp4"
    if message.audio:
        title = getattr(media, "title", None) or getattr(media, "performer", None)
        return f"{title or 'audio'}_{message.id}.mp3"
    return f"archivo_{message.id}.bin"


def register(app: Client) -> None:
    @app.on_message(MEDIA_FILTER & AUTHORIZED)
    async def _media_handler(client: Client, message: Message):
        # --- Álbum: agrupamos y esperamos un instante a que lleguen todos ---
        group_id = message.media_group_id
        if group_id:
            async with _GROUP_LOCK:
                _GROUPS.setdefault(group_id, []).append(message)
                task = _GROUP_TASKS.get(group_id)
                if task and not task.done():
                    task.cancel()
                _GROUP_TASKS[group_id] = asyncio.create_task(_flush_group(client, group_id))
            return

        await _handle_one(client, message)

    async def _flush_group(client: Client, group_id: str) -> None:
        try:
            await asyncio.sleep(GROUP_WAIT)
            async with _GROUP_LOCK:
                messages = _GROUPS.pop(group_id, [])
                _GROUP_TASKS.pop(group_id, None)
            if not messages:
                return
            messages.sort(key=lambda m: m.id)
            await _enqueue_batch(client, messages)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            log.exception("Álbum %s falló: %s", group_id, exc)


async def _handle_one(client: Client, message: Message) -> None:
    user_id = message.from_user.id
    media = _media_of(message)
    size = int(getattr(media, "file_size", 0) or 0)
    name = _filename_of(message)

    if not await check_limits(user_id, message, size):
        return

    if size and size > RUNTIME_SETTINGS.max_file_bytes:
        await reply(
            message,
            f"<b>❌ Archivo demasiado grande</b>\n\n"
            f"📄 {T.code(T.shorten(name, 48))}\n"
            f"📦 {T.esc(_size(size))} · 📏 máximo {T.esc(_size(RUNTIME_SETTINGS.max_file_bytes))}\n\n"
            f"{T.i('Pide al owner que suba el límite con /settings.')}",
        )
        return

    task, error = await enqueue(
        client, message, filename=name, file_size=size, source="telegram"
    )
    if error:
        await reply(message, error)
        return

    _mark_rate(user_id)
    log.info(
        "[%s] encolada por %s | %s | %.1f MB",
        task.task_id, user_id, name, size / 1024 / 1024,
    )


async def _enqueue_batch(client: Client, messages: List[Message]) -> None:
    """Encola todos los archivos de un álbum con un solo mensaje-resumen."""
    first = messages[0]
    user_id = first.from_user.id
    total = sum(
        int(getattr(_media_of(m), "file_size", 0) or 0) for m in messages
    )
    if not await check_limits(user_id, first, total):
        return

    accepted: List[str] = []
    rejected: List[str] = []
    total_bytes = 0

    for message in messages:
        media = _media_of(message)
        size = int(getattr(media, "file_size", 0) or 0)
        name = _filename_of(message)
        if size and size > RUNTIME_SETTINGS.max_file_bytes:
            rejected.append(f"📄 {T.code(T.shorten(name, 32))} ({_size(size)})")
            continue
        task, error = await enqueue(
            client, message, filename=name, file_size=size, source="telegram"
        )
        if error:
            rejected.append(f"📄 {T.code(T.shorten(name, 32))} — {T.i(error)}")
            continue
        accepted.append(f"• {T.code(T.shorten(name, 36))} ({_size(size)})")
        total_bytes += size

    if accepted:
        _mark_rate(user_id)
        body = [
            f"<b>📚 Álbum recibido · {len(accepted)} archivo(s) en cola</b>",
            f"📦 Total: {T.esc(_size(total_bytes))}",
            "",
            *accepted[:20],
        ]
        if len(accepted) > 20:
            body.append(T.i(f"… y {len(accepted) - 20} más"))
        body += ["", T.i("Usa /cola para seguir el progreso de cada uno.")]
        await reply(first, "\n".join(body))

    if rejected:
        await reply(
            first,
            "<b>⚠️ Algunos archivos no se pudieron encolar</b>\n\n" + "\n".join(rejected[:10]),
        )


def _mark_rate(user_id: int) -> None:
    from ..queue import QUEUE

    QUEUE.touch_rate_limit(user_id)


def _size(n: int) -> str:
    from ..progress import human_size

    return human_size(n)
