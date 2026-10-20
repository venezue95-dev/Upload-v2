"""
actions.py
==========
Operaciones sobre tareas que comparten los comandos (/reintentar, /enlaces…) y
los botones: buscar una tarea, reintentarla (reanudando lo ya subido) y borrar
sus archivos de toDus. Sin Telegram aquí salvo el aviso de "reintento en cola".
"""

from __future__ import annotations

import asyncio
import os
from typing import Dict, Optional, Tuple

from pyrogram.enums import ParseMode

from config import CONFIG
from . import keyboards as K
from . import storage
from . import texts as T
from .queue import QUEUE, UploadTask
from .s3_client import get_client, part_number_from_key

MAX_MANUAL_ATTEMPTS = 3


# ----------------------------------------------------------------------
# Buscar una tarea.
# ----------------------------------------------------------------------
def find_task(task_id: str, viewer_id: int, *, manage: bool = False) -> Optional[UploadTask]:
    """
    Devuelve la tarea por id: primero en memoria y, si ya no está (pasó una
    hora o se reinició el bot), reconstruida desde el historial. Así los
    botones de mensajes antiguos siguen funcionando.

    Un usuario solo ve sus tareas; el owner (``manage``) ve todas. Una tarea
    ajena se trata como "no encontrada" para no revelar que existe.
    """
    task = QUEUE.get(task_id)
    if task is not None:
        return task if (manage or task.user_id == viewer_id) else None
    row = storage.get_task(task_id, None if manage else viewer_id)
    return UploadTask.from_history(row) if row else None


def last_user_task(user_id: int, *, only_retryable: bool = False,
                   only_with_links: bool = False) -> Optional[UploadTask]:
    """Última tarea del usuario que cumpla el filtro (para /reintentar y /enlaces sin id)."""
    for task in reversed(QUEUE.user_tasks(user_id, limit=60)):
        if only_retryable and not (task.is_retryable and not task.retried_as):
            continue
        if only_with_links and not task.has_links:
            continue
        if task.is_final:
            return task
    if only_with_links:
        for row in storage.user_history(user_id, limit=20):
            if row.get("urls"):
                return UploadTask.from_history(row)
    return None


# ----------------------------------------------------------------------
# Reintentar (reanudando las partes ya subidas).
# ----------------------------------------------------------------------
def resume_state(task: UploadTask) -> Tuple[Optional[str], Dict[int, str]]:
    """
    Devuelve (carpeta, {nº de parte: clave}) de las partes ya subidas.

    Se usan los datos en memoria y, si la tarea viene de un reinicio, los del
    historial. El número de parte se lee del nombre de la clave
    (``x.part003.mkv``): con subidas en paralelo las partes terminan
    desordenadas y fallar puede dejar huecos, así que NO vale numerar la lista.
    """
    if task.keys_by_part:
        parts = {n: k for n, k in task.keys_by_part.items() if k}
        if parts:
            return _folder_of(task.s3_base, next(iter(parts.values()))), parts

    row = storage.get_task(task.task_id) if task.task_id else None
    if row and row.get("keys"):
        parts: Dict[int, str] = {}
        for key in row["keys"]:
            number = part_number_from_key(key)
            if number:
                parts[number] = key
        if parts:
            return _folder_of(row.get("base"), next(iter(parts.values()))), parts
    return None, {}


def _folder_of(base: Optional[str], sample_key: str) -> str:
    """Nombre de la carpeta de la subida ('20260101-120000-ab12cd')."""
    return base or os.path.basename(os.path.dirname(sample_key))


async def retry_task(client, task: UploadTask) -> Tuple[bool, str]:
    """
    Reintenta una tarea fallida/cancelada creando una copia nueva.

    Reanuda las partes que ya se habían subido (no hay que volver a subirlas ni,
    en modo streaming, volver a descargarlas). Devuelve (ok, mensaje).
    """
    if not task.is_retryable:
        return False, "ℹ️ Esa tarea no se puede reintentar (en curso o ya completada)."
    if task.retried_as:
        return False, "ℹ️ Esa tarea ya se reintentó: mira /cola."
    if task.attempts >= MAX_MANUAL_ATTEMPTS:
        return False, (
            f"⚠️ Ya se intentó {MAX_MANUAL_ATTEMPTS} veces sin éxito. "
            "Revisa el error o vuelve a enviar el archivo."
        )
    if task.source == "telegram":
        if not (task.chat_id and task.message_id):
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
    clone.media_format = getattr(task, "media_format", "mp4")
    clone.media_quality = getattr(task, "media_quality", "720")
    clone.chunk_size = task.chunk_size
    clone.parts_total = task.parts_total

    folder, done = resume_state(task)
    resumed = 0
    if done and folder and task.parts_total > 1:
        clone.resume_folder = folder
        clone.keys_by_part = dict(done)
        clone.resumed_parts = sorted(done)
        clone.parts_done = len(done)
        clone.bytes_transferred = sum(clone.expected_part_length(n) for n in clone.resumed_parts)
        resumed = len(done)

    allowed, used, limit = storage.reserve_quota(
        clone.user_id,
        clone.task_id,
        clone.file_size,
        owner=(CONFIG.owner_id is not None and clone.user_id == CONFIG.owner_id),
    )
    if not allowed:
        from .progress import human_size
        return False, (
            "📊 <b>Cuota diaria superada</b>\n\n"
            f"• Usado o reservado: {T.b(human_size(used))}\n"
            f"• Límite diario: {T.b(human_size(limit))}\n"
            f"• Este reintento: {T.b(human_size(clone.file_size))}\n\n"
            f"{T.i('Espera a que termine otra tarea o libere su reserva.')}"
        )

    ok, reason = QUEUE.submit(clone)
    if not ok:
        storage.release_quota(clone.task_id)
        return False, reason

    task.retried_as = clone.task_id
    msg = await client.send_message(
        task.chat_id or task.user_id,
        T.premiumize(T.retry_queued_text(clone, resumed)),
        parse_mode=ParseMode.HTML,
        reply_markup=K.task_message_keyboard(clone),
    )
    clone.status_message_id = msg.id
    return True, f"♻️ Reintento encolado ({clone.attempts}/{MAX_MANUAL_ATTEMPTS})"


# ----------------------------------------------------------------------
# Borrar de toDus.
# ----------------------------------------------------------------------
async def delete_task_files(task: UploadTask) -> Tuple[int, int]:
    """
    Borra de toDus los archivos de una tarea. Devuelve (borrados, fallidos).

    Solo se olvidan los enlaces de lo que se borró de verdad: si alguna parte
    falla, sus claves y enlaces se conservan para poder reintentar el borrado.
    """
    keys = list(task.keys)
    if not keys:
        return 0, 0
    deleted, failed = await asyncio.to_thread(get_client().delete_keys, keys)
    gone = set(deleted)

    if len(task.urls) == len(task.keys):
        pairs = [(k, u) for k, u in zip(task.keys, task.urls) if k not in gone]
        task.keys = [k for k, _ in pairs]
        task.urls = [u for _, u in pairs]
    else:
        task.keys = [k for k in task.keys if k not in gone]
        if not task.keys:
            task.urls = []

    if not task.keys:
        task.deleted_from_s3 = True
        task.urls = []
        storage.mark_deleted(task.task_id)
    else:
        storage.update_links(task.task_id, task.keys, task.urls)
    return len(deleted), len(failed)
