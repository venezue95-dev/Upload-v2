"""
media.py
========
Comando explícito para descargas multimedia mediante yt-dlp.

Sintaxis:
    /descargar URL
    /descargar URL mp4 1080
    /descargar URL mp3

Los enlaces normales siguen usando el descargador HTTP directo y no cambian.
"""

from __future__ import annotations

import logging
import re
from typing import Optional

from pyrogram import Client, filters
from pyrogram.types import Message

from .. import texts as T
from ..media_downloader import is_blocked_media_url, is_media_url, normalize_options
from .common import AUTHORIZED, check_limits, enqueue, reply

log = logging.getLogger("s3_bot.media_handler")
_URL_RE = re.compile(r"https?://[^\s<>\"')]+", re.IGNORECASE)


def register(app: Client) -> None:
    @app.on_message(filters.command(["descargar", "download"]) & AUTHORIZED)
    async def _download(client: Client, message: Message):
        args = list(message.command or [])[1:]
        url = next((x.rstrip(".,;:)") for x in args if _URL_RE.fullmatch(x)), None)
        if not url:
            await reply(message, _usage())
            return
        extra = [x.lower() for x in args if x != url]
        media_format = next((x for x in extra if x in {"mp4", "mp3", "m4a", "opus", "wav"}), "mp4")
        quality = next((x.rstrip("p") for x in extra if x.rstrip("p") in {
            "144", "240", "360", "480", "720", "1080", "1440", "2160", "max"
        }), "720")
        try:
            media_format, quality = normalize_options(media_format, quality)
        except ValueError as exc:
            await reply(message, f"⛔️ {T.esc(str(exc))}\n\n{_usage()}")
            return
        if is_blocked_media_url(url):
            await reply(message, "ℹ️ Las descargas de YouTube están desactivadas temporalmente.")
            return
        if not is_media_url(url):
            await reply(message, "⛔️ Ese dominio no está habilitado para descargas multimedia.")
            return
        user_id = message.from_user.id
        if not await check_limits(user_id, message):
            return
        task, error = await enqueue(
            client,
            message,
            filename=f"descarga_{media_format}",
            file_size=0,
            source="media",
            url=url,
            media_format=media_format,
            media_quality=quality,
        )
        if error:
            await reply(message, error)
            return
        from ..queue import QUEUE
        QUEUE.touch_rate_limit(user_id)
        log.info(
            "[%s] descarga multimedia encolada por %s | %s | %s %s",
            task.task_id, user_id, url, media_format, quality,
        )


def _usage() -> str:
    return (
        "📥 <b>Descarga multimedia</b>\n\n"
        "Uso:\n"
        "<code>/descargar URL</code>\n"
        "<code>/descargar URL mp4 1080</code>\n"
        "<code>/descargar URL mp3</code>\n\n"
        "Formatos: mp4, mp3, m4a, opus o wav.\n"
        "Calidades de vídeo: 360, 480, 720, 1080, 1440, 2160 o max.\n"
        "La calidad exacta depende del contenido y de la plataforma."
    )
