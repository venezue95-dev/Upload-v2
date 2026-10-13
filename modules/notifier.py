"""
notifier.py
===========
Actualiza los mensajes de Telegram de TODAS las tareas activas desde un
único bucle asíncrono (en vez de un bucle por tarea, como hacía la versión
anterior). Esto es lo que permite aguantar 40 usuarios sin inundar la API
de Telegram ni el event loop.

Responsabilidades:
- refrescar el mensaje de progreso (posición en cola, %, velocidad, ETA),
- enviar el mensaje final (enlaces), de error o de cancelación,
- enviar la lista de enlaces como archivo .txt cuando el usuario lo pide,
- avisar al owner cuando una tarea falla.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Dict, Optional

from pyrogram.enums import ParseMode

from config import CONFIG
from . import texts as T
from . import views
from .downloader import local_path_for
from .queue import QUEUE, TaskStatus, UploadTask
from .runtime_settings import RUNTIME_SETTINGS

log = logging.getLogger("s3_bot.notifier")

_started = False
_client = None
_loop_task: Optional[asyncio.Task] = None
_last_text: Dict[str, str] = {}
_last_edit: Dict[str, float] = {}
_edit_gate: Optional[asyncio.Semaphore] = None
_owner_alert_at: float = 0.0
_flood_until: float = 0.0       # no se edita nada hasta este instante (FloodWait)


# ----------------------------------------------------------------------
# Ciclo de vida.
# ----------------------------------------------------------------------
async def start(client) -> None:
    global _started, _client, _loop_task, _edit_gate
    if _started:
        return
    _client = client
    _edit_gate = asyncio.Semaphore(6)
    _started = True
    _loop_task = asyncio.create_task(_loop(), name="notifier")
    log.info("Notificador de progreso iniciado")


async def stop() -> None:
    global _started
    _started = False
    if _loop_task:
        _loop_task.cancel()


async def _loop() -> None:
    while _started:
        try:
            await asyncio.sleep(0.5)
            await _tick()
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            log.exception("Error en el notificador: %s", exc)
            await asyncio.sleep(1)


async def _tick() -> None:
    now = time.time()
    if now < _flood_until:
        return
    interval = float(RUNTIME_SETTINGS.progress_interval)
    pending = []

    for task in QUEUE.active_tasks():
        if task.is_final or not task.status_message_id:
            continue
        _sync_disk_progress(task)
        if task.status.is_active:
            task.sample_speed()

        text, keyboard = views.task_message(task)
        # Las tareas en cola se refrescan más despacio (solo cambia la posición).
        min_interval = interval if task.status.is_active else max(3.0, interval)
        if text == _last_text.get(task.task_id) and (now - _last_edit.get(task.task_id, 0)) < 20:
            continue
        if (now - _last_edit.get(task.task_id, 0)) < min_interval:
            continue
        pending.append((task, text, keyboard))

    if not pending:
        return

    async def worker(task: UploadTask, text: str, keyboard) -> None:
        assert _edit_gate is not None
        async with _edit_gate:
            ok = await edit_status(task, text, keyboard=keyboard)
        if ok:
            _last_text[task.task_id] = text
            _last_edit[task.task_id] = time.time()

    await asyncio.gather(*(worker(t, txt, kb) for t, txt, kb in pending), return_exceptions=True)
    # Limpieza de cachés de tareas que ya no existen.
    if len(_last_text) > 500:
        alive = {t.task_id for t in QUEUE.all_tasks(limit=200)}
        for key in list(_last_text):
            if key not in alive:
                _last_text.pop(key, None)
                _last_edit.pop(key, None)


def _sync_disk_progress(task: UploadTask) -> None:
    """
    Red de seguridad: recupera los bytes realmente escritos en disco.

    Si el callback del origen no se dispara (algunos servidores o ciertos
    fallos de Pyrogram), el progreso quedaría congelado. Aquí se comprueba el
    tamaño real de los temporales y se usa como suelo del progreso.
    """
    if task.status != TaskStatus.DOWNLOADING:
        return

    user_dir = os.path.join(CONFIG.download_dir, str(task.user_id))

    if task.mode == "stream":
        # Cada parte en vuelo tiene su propio temporal: se suma y se asigna a
        # su número de parte (el total ya incluye las partes terminadas).
        chunks = os.path.join(user_dir, "chunks")
        if not os.path.isdir(chunks):
            return
        for name in os.listdir(chunks):
            if not (name.startswith(task.task_id) and name.endswith(".tmp")):
                continue
            # formato: <task_id>_p001.tmp
            try:
                part_number = int(name.rsplit("_p", 1)[1].split(".")[0])
            except (IndexError, ValueError):
                continue
            path = os.path.join(chunks, name)
            try:
                size = os.path.getsize(path)
            except OSError:
                continue
            task.note_fetched(part_number, size)
        return

    # Modo clásico: un único archivo con el nombre saneado.
    path = local_path_for(task)
    try:
        size = os.path.getsize(path)
    except OSError:
        return
    if size > task.bytes_fetched:
        task.set_fetched(size)


# ----------------------------------------------------------------------
# Edición de mensajes.
# ----------------------------------------------------------------------
async def edit_status(task: UploadTask, text: str, keyboard=None) -> bool:
    if not (_client and task.chat_id and task.status_message_id):
        return False
    try:
        await _client.edit_message_text(
            task.chat_id,
            task.status_message_id,
            text,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
            reply_markup=keyboard,
        )
        return True
    except Exception as exc:  # noqa: BLE001
        msg = str(exc)
        if "MESSAGE_NOT_MODIFIED" in msg or "not modified" in msg.lower():
            return True
        if "MESSAGE_ID_INVALID" in msg or "message to edit not found" in msg.lower():
            log.debug("[%s] el mensaje de estado ya no existe", task.task_id)
            task.status_message_id = None
            return False
        # Flood control: esperamos y reintentamos una vez en el próximo tick.
        if "FLOOD_WAIT" in msg:
            global _flood_until
            wait = int(getattr(exc, "value", 0) or 0) or 10
            _flood_until = time.time() + wait + 1
            log.warning("[%s] flood control de Telegram: pausa de %ds en las ediciones",
                        task.task_id, wait)
            return False
        log.debug("[%s] no se pudo editar el progreso: %s", task.task_id, exc)
        return False


async def send_status(task: UploadTask, text: str, keyboard=None) -> None:
    """Envía un mensaje nuevo de estado (si el anterior ya no sirve)."""
    if not (_client and task.chat_id):
        return
    try:
        msg = await _client.send_message(
            task.chat_id,
            text,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
            reply_markup=keyboard,
        )
        task.status_message_id = msg.id
    except Exception as exc:  # noqa: BLE001
        log.warning("[%s] no se pudo enviar el estado: %s", task.task_id, exc)


# ----------------------------------------------------------------------
# Final de tarea.
# ----------------------------------------------------------------------
def on_task_finished(task: UploadTask) -> None:
    """Llamado por la cola cuando una tarea termina (estamos en el loop)."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:  # pragma: no cover
        return
    loop.create_task(_finalize(task))


async def _finalize(task: UploadTask) -> None:
    _last_text.pop(task.task_id, None)
    text, keyboard = views.task_message(task)

    ok = await edit_status(task, text, keyboard=keyboard)
    if not ok:
        await send_status(task, text, keyboard=keyboard)

    if task.status == TaskStatus.COMPLETED:
        await _log_to_channel(task)

    if task.status == TaskStatus.FAILED and CONFIG.owner_id and task.user_id != CONFIG.owner_id:
        await _alert_owner(task)


async def _log_to_channel(task: UploadTask) -> None:
    """Copia un resumen de cada subida completada al canal de registro."""
    channel = CONFIG.log_channel_id
    if not channel or not _client:
        return
    try:
        who = f"@{task.username}" if task.username else str(task.user_id)
        from . import progress as P

        await _client.send_message(
            channel,
            f"📤 <b>Nueva subida</b>\n"
            f"👤 {T.esc(who)} ({T.code(str(task.user_id))})\n"
            f"📄 {T.code(T.shorten(task.filename, 50))}\n"
            f"📦 {P.human_size(task.file_size)} · 🧩 {task.parts_total} parte(s)\n"
            f"⏱️ {P.human_time(task.duration())} · media "
            f"{P.human_speed(task.avg_speed())}\n"
            f"🆔 {T.code(task.task_id)}",
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )
    except Exception as exc:  # noqa: BLE001
        log.debug("No se pudo registrar en el canal: %s", exc)


async def _alert_owner(task: UploadTask) -> None:
    global _owner_alert_at
    now = time.time()
    if now - _owner_alert_at < 60:      # como máximo un aviso por minuto
        return
    _owner_alert_at = now
    if not _client:
        return
    who = f"@{task.username}" if task.username else str(task.user_id)
    try:
        await _client.send_message(
            CONFIG.owner_id,
            f"⚠️ <b>Tarea fallida</b>\n"
            f"👤 {T.esc(who)} ({task.user_id})\n"
            f"📄 {T.code(T.shorten(task.filename, 40))}\n"
            f"🆔 {T.code(task.task_id)}\n"
            f"❗️ {T.esc(T.shorten(task.error or 'sin detalle', 200))}",
            parse_mode=ParseMode.HTML,
        )
    except Exception as exc:  # noqa: BLE001
        log.debug("No se pudo avisar al owner: %s", exc)


# ----------------------------------------------------------------------
# Lista de enlaces en .txt
# ----------------------------------------------------------------------
def links_txt_content(task: UploadTask) -> str:
    lines = [
        "Enlaces de descarga directa — toDus S3",
        "=" * 48,
        f"Archivo : {task.filename}",
        f"Tamaño  : {task.file_size} bytes",
        f"Partes  : {task.parts_total}",
        f"Fecha   : {time.strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "Descarga todas las partes en la misma carpeta y únelas en orden.",
        "7-Zip: Add to archive → Split.  Linux/macOS: cat parte.* > archivo",
        "",
        "-" * 48,
    ]
    for i, url in enumerate(task.urls, start=1):
        label = f"parte {i:03d}" if task.parts_total > 1 else "archivo"
        lines.append(f"[{label}] {url}")
    lines.append("-" * 48)
    return "\n".join(lines)


async def send_links_txt(client, chat_id: int, task: UploadTask) -> None:
    """Envía los enlaces como documento .txt."""
    if not task.urls:
        return
    tmp_dir = os.path.join(CONFIG.download_dir, "_txt")
    os.makedirs(tmp_dir, exist_ok=True)
    stem = os.path.splitext(task.filename)[0][:60] or "enlaces"
    path = os.path.join(tmp_dir, f"{stem}-enlaces.txt")
    try:
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(links_txt_content(task))
        await client.send_document(
            chat_id,
            path,
            caption=(
                f"📄 <b>Enlaces ({len(task.urls)})</b>\n"
                f"{T.code(T.shorten(task.filename, 40))}\n"
                f"{T.i('Cada línea es una parte, en orden. Únelas para reconstruir el archivo.')}"
            ),
            parse_mode=ParseMode.HTML,
        )
    except Exception as exc:  # noqa: BLE001
        log.warning("No se pudo enviar el .txt de enlaces: %s", exc)
    finally:
        try:
            os.remove(path)
        except OSError:
            pass
