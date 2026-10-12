"""
url.py
======
Handler de enlaces http(s).

- Acepta **varios enlaces en un mismo mensaje**: se encolan todos.
- Evita duplicados (mismo usuario + misma URL activa).
- Valida el esquema/host (bloquea red interna por seguridad).
"""

from __future__ import annotations

import logging
import re
from typing import List

from pyrogram import Client, filters
from pyrogram.types import Message

from .. import texts as T
from ..downloader import check_url, probe_url, url_to_filename
from ..queue import QUEUE
from ..runtime_settings import RUNTIME_SETTINGS
from .common import AUTHORIZED, check_limits, enqueue, reply

log = logging.getLogger("s3_bot.url")

URL_RE = re.compile(r"https?://[^\s<>\"')]+", re.IGNORECASE)

URL_FILTER = filters.regex(URL_RE) & ~filters.command(
    ["start", "help", "status", "ping", "cola", "cancelar", "historial", "settings",
     "stats", "usuarios", "anuncio", "pausar", "reanudar", "sistema", "limpiar",
     "reintentar", "menu", "enlaces"]
)


def extract_urls(text: str) -> List[str]:
    seen = set()
    urls = []
    for match in URL_RE.findall(text or ""):
        url = match.rstrip(".,;:)")
        if url not in seen:
            seen.add(url)
            urls.append(url)
    return urls[:10]  # como mucho 10 enlaces por mensaje


def register(app: Client) -> None:
    @app.on_message(URL_FILTER & AUTHORIZED)
    async def _url_handler(client: Client, message: Message):
        urls = extract_urls(message.text or "")
        if not urls:
            return

        user_id = message.from_user.id
        if not await check_limits(user_id, message):
            return

        if len(urls) == 1:
            await _enqueue_single(client, message, urls[0])
        else:
            await _enqueue_many(client, message, urls)


async def _enqueue_single(client: Client, message: Message, url: str) -> None:
    user_id = message.from_user.id

    ok, why = check_url(url)
    if not ok:
        await reply(message, f"<b>⛔️ Enlace no válido</b>\n\n{T.esc(why)}")
        return

    duplicate = QUEUE.find_duplicate_url(user_id, url)
    if duplicate:
        await reply(
            message,
            f"<b>ℹ️ Ya estás subiendo ese enlace</b>\n\n"
            f"🆔 {T.code(duplicate.task_id)} · estado: {T.esc(duplicate.status.value)}\n"
            f"Usa /cola para verlo o espera a que termine.",
        )
        return

    # Consultamos metadatos (tamaño/nombre) para avisar de límites cuanto antes.
    info = await _probe(url)
    size = int(info.get("size") or 0)
    name = info.get("filename") or url_to_filename(url)

    if not await check_limits(user_id, message, size):
        return

    if size and size > RUNTIME_SETTINGS.max_file_bytes:
        await reply(
            message,
            f"<b>❌ Archivo demasiado grande</b>\n\n"
            f"🔗 {T.code(T.shorten(url, 60))}\n"
            f"📦 {T.esc(_size(size))} · 📏 máximo {T.esc(_size(RUNTIME_SETTINGS.max_file_bytes))}",
        )
        return

    task, error = await enqueue(
        client, message, filename=name, file_size=size, source="url", url=url
    )
    if error:
        await reply(message, error)
        return

    QUEUE.touch_rate_limit(user_id)
    log.info("[%s] encolada URL por %s | %s", task.task_id, user_id, url)


async def _enqueue_many(client: Client, message: Message, urls: List[str]) -> None:
    user_id = message.from_user.id
    accepted: List[str] = []
    rejected: List[str] = []

    for url in urls:
        ok, why = check_url(url)
        if not ok:
            rejected.append(f"⛔️ {T.code(T.shorten(url, 44))} — {T.i(T.shorten(why, 40))}")
            continue
        if QUEUE.find_duplicate_url(user_id, url):
            rejected.append(f"♻️ {T.code(T.shorten(url, 44))} — {T.i('ya en cola')}")
            continue
        info = await _probe(url)
        size = int(info.get("size") or 0)
        name = info.get("filename") or url_to_filename(url)
        if size and not await check_limits(user_id, message, size):
            break
        if size and size > RUNTIME_SETTINGS.max_file_bytes:
            rejected.append(f"📏 {T.code(T.shorten(url, 44))} — {T.i('supera el máximo')}")
            continue

        task, error = await enqueue(
            client, message, filename=name, file_size=size, source="url", url=url
        )
        if error:
            rejected.append(f"🚦 {T.code(T.shorten(url, 44))} — {T.i(T.shorten(error, 40))}")
            continue
        accepted.append(f"• {T.code(T.shorten(name, 36))} — {T.esc(_size(size) if size else 'tamaño desconocido')}")

    if accepted:
        QUEUE.touch_rate_limit(user_id)
        body = [
            f"<b>🌐 {len(accepted)} enlace(s) en cola</b>",
            "",
            *accepted[:15],
            "",
            T.i("Usa /cola para ver el progreso."),
        ]
        await reply(message, "\n".join(body))

    if rejected:
        await reply(
            message,
            "<b>⚠️ Enlaces descartados</b>\n\n" + "\n".join(rejected[:10]),
        )


async def _probe(url: str) -> dict:
    import asyncio

    try:
        return await asyncio.wait_for(asyncio.to_thread(probe_url, url), timeout=25)
    except Exception as exc:  # noqa: BLE001
        log.debug("probe falló para %s: %s", url, exc)
        return {}


def _size(n: int) -> str:
    from ..progress import human_size

    return human_size(n)
