"""
pipeline.py
===========
Ejecutor de una tarea: descarga → divide en trozos → sube a toDus → finaliza.

Dos modos de trabajo (ajustable en /settings):

- **stream** (por defecto cuando el origen lo permite): descarga y sube
  *a la vez*. Cada trozo se descarga a un temporal pequeño y se sube
  enseguida, así el disco usado es de unos pocos trozos y el tiempo total
  baja mucho (descarga y subida se solapan).
- **classic**: descarga todo el archivo y luego lo sube por trozos. Necesita
  espacio para el archivo completo, pero es el modo más simple y sirve de
  respaldo cuando el origen no soporta descargas parciales.

En ambos modos el trabajo pesado de subida ocurre en hilos del pool global, y
la subida simultánea de partes se limita con un semáforo dinámico
(``global_chunk_workers``) para que 40 usuarios no lancen 300 PUT a la vez.
"""

from __future__ import annotations

import asyncio
import logging
import os
import shutil
from typing import Callable, List, Optional, Sequence

from config import CONFIG
from . import notifier
from . import texts as T
from .downloader import (
    check_url,
    download_telegram_full,
    download_telegram_range,
    download_url_range,
    ensure_dir,
    ensure_free_space,
    probe_url,
    safe_local_name,
    url_to_filename,
)
from .limiter import DynamicSemaphore
from .queue import QUEUE, TaskCancelled, TaskStatus, UploadTask
from .runtime_settings import RUNTIME_SETTINGS
from .s3_client import ChunkUploadError, S3Client, S3Error, plan_parts, pool

log = logging.getLogger("s3_bot.pipeline")

# Semáforo global de subidas (límite ajustable en caliente desde /settings).
GLOBAL_UPLOAD_GATE = DynamicSemaphore(6, limit_fn=lambda: RUNTIME_SETTINGS.global_chunk_workers)

_S3: Optional[S3Client] = None
_MI_B = 1024 * 1024


def s3() -> S3Client:
    global _S3
    if _S3 is None:
        _S3 = S3Client(CONFIG.s3)
    return _S3


# ----------------------------------------------------------------------
# Punto de entrada (lo llama la cola).
# ----------------------------------------------------------------------
async def run_task(client, task: UploadTask) -> None:
    task.max_attempts = max(task.max_attempts, 1 + int(CONFIG.queue.auto_retries))

    while True:
        try:
            await _run_once(client, task)
            return
        except TaskCancelled:
            raise
        except Exception as exc:  # noqa: BLE001
            # Reintento automático solo si no se subió nada todavía.
            if task.attempts < task.max_attempts and task.parts_done == 0:
                task.attempts += 1
                log.warning(
                    "[%s] intento %d/%d falló (%s); reintentando la tarea",
                    task.task_id, task.attempts - 1, task.max_attempts, exc,
                )
                task.error = str(exc)
                await _safe_status(
                    task,
                    f"♻️ <b>Reintentando</b> ({task.attempts}/{task.max_attempts})\n"
                    f"{T.code(T.shorten(task.filename, 40))}\n"
                    f"{T.i(T.shorten(str(exc), 120))}",
                )
                await asyncio.sleep(3)
                continue
            raise
        finally:
            _cleanup_task_files(task)


async def _run_once(client, task: UploadTask) -> None:
    settings = RUNTIME_SETTINGS
    max_bytes = settings.max_file_bytes

    # ---------------- 1. Origen ----------------
    task.status = TaskStatus.DOWNLOADING
    task.phase = "Preparando"
    source = await _resolve_source(client, task)
    size = source["size"]
    name = safe_local_name(source["name"])
    task.filename = name

    if size and size > max_bytes:
        task.mark_finished(
            TaskStatus.FAILED,
            f"Archivo demasiado grande: {_size(size)} (máximo {_size(max_bytes)})",
        )
        await _final_status(
            task,
            f"<b>❌ Archivo demasiado grande</b>\n\n"
            f"📄 {T.code(T.shorten(name, 48))}\n"
            f"📦 {T.esc(_size(size))} · 📏 máximo {T.esc(_size(max_bytes))}\n\n"
            f"{T.i('Pide al owner que suba el límite en /settings.')}",
        )
        return

    task.file_size = size

    # ---------------- 2. Modo de trabajo ----------------
    chunk_bytes = max(_MI_B, settings.chunk_bytes)
    mode = _choose_mode(settings.pipeline_mode, source, size, chunk_bytes)
    task.mode = mode

    # ---------------- 3. Espacio en disco ----------------
    user_dir = os.path.join(CONFIG.download_dir, str(task.user_id))
    ensure_dir(user_dir)
    chunks_dir = os.path.join(user_dir, "chunks")
    ensure_dir(chunks_dir)

    per_task = max(1, int(settings.max_parallel))
    need = chunk_bytes * per_task * 2 + _MI_B * 8
    if mode == "classic" and size:
        need += size
    elif not size:
        need += 2 * 1024 ** 3   # tamaño desconocido: reservamos 2 GB
    ok, detail = ensure_free_space(CONFIG.download_dir, need)
    if not ok:
        task.mark_finished(TaskStatus.FAILED, detail)
        await _final_status(
            task,
            f"<b>❌ Sin espacio en disco</b>\n\n{T.esc(detail)}\n\n"
            f"{T.i('Avisa al owner: hay que liberar espacio en el servidor.')}",
        )
        return

    # ---------------- 4. Trabajo ----------------
    if mode == "stream":
        await _run_stream(client, task, source, chunk_bytes, per_task)
    else:
        await _run_classic(client, task, source, chunk_bytes, per_task)

    # ---------------- 5. Resultado ----------------
    # Ordenar las claves por número de parte (las subidas terminan desordenadas).
    task.keys = [task.keys_by_part[n] for n in sorted(task.keys_by_part)]
    if not task.keys:
        raise S3Error("no se generó ninguna clave en el almacenamiento")

    urls: List[str] = []
    client_s3 = s3()
    for key in task.keys:
        urls.append(client_s3.build_url(key, task))
    task.urls = urls
    task.s3_base = task.s3_base or ""
    if not task.file_size:
        task.file_size = task.bytes_transferred
    task.mark_finished(TaskStatus.COMPLETED)
    task.phase = "Listo"


# ----------------------------------------------------------------------
# Resolución del origen.
# ----------------------------------------------------------------------
async def _resolve_source(client, task: UploadTask) -> dict:
    if task.source == "url":
        url = (task.url or "").strip()
        ok, why = check_url(url)
        if not ok:
            raise RuntimeError(f"URL rechazada: {why}")
        info = await asyncio.to_thread(probe_url, url)
        name = info.get("filename") or url_to_filename(url)
        return {
            "kind": "url",
            "url": url,
            "size": int(info.get("size") or task.file_size or 0),
            "name": name,
            "ranges": bool(info.get("ranges")),
            "media": None,
        }

    # Origen Telegram: recuperamos el mensaje (por si el bot se reinició).
    message = await client.get_messages(task.chat_id, task.message_id)
    if message is None:
        raise RuntimeError("el mensaje original ya no está disponible en Telegram")
    media = message.document or message.video or message.audio or message.voice
    if media is None:
        raise RuntimeError("el mensaje ya no contiene un archivo descargable")
    size = int(getattr(media, "file_size", 0) or 0)
    name = getattr(media, "file_name", None) or f"{task.source}_{task.task_id}.bin"
    return {
        "kind": "telegram",
        "url": None,
        "size": size,
        "name": name,
        "ranges": True,          # siempre podemos pedir rangos a Telegram
        "media": media,
    }


def _choose_mode(pipeline_mode: str, source: dict, size: int, chunk_bytes: int) -> str:
    if pipeline_mode == "stream":
        return "stream" if size else "classic"
    if pipeline_mode == "classic":
        return "classic"
    # auto: fases separadas para que el usuario vea claramente descarga → subida.
    # El streaming sigue disponible de forma explícita desde /settings.
    return "classic"


# ----------------------------------------------------------------------
# Modo streaming: descarga y sube trozo a trozo.
# ----------------------------------------------------------------------
async def _run_stream(client, task: UploadTask, source: dict, chunk_bytes: int,
                      per_task: int) -> None:
    size = task.file_size
    base_name, parts = plan_parts(task.user_id, task.filename, size, chunk_bytes)
    task.s3_base = base_name
    task.plan_parts(chunk_bytes, len(parts))
    task.phase = "Descargando y subiendo"

    log.info(
        "[%s] modo stream | %d partes de %.0f MB",
        task.task_id, len(parts), chunk_bytes / 1024 / 1024,
    )

    async def process(part):
        tmp = _tmp_path(task, part.number)
        try:
            await _fetch_part(client, task, source, part, tmp)
            await _upload_part(task, part, tmp)
        finally:
            _remove(tmp)

    await _run_parts(task, parts, process, per_task)


async def _fetch_part(client, task: UploadTask, source: dict, part, tmp: str) -> None:
    last = {"n": 0}

    def cb(written: int) -> None:
        delta = written - last["n"]
        if delta > 0:
            last["n"] = written
            task.add_fetched(delta)

    if source["kind"] == "telegram":
        await download_telegram_range(
            client,
            source["media"],
            tmp,
            task,
            offset=part.offset,
            length=part.length,
            file_size=task.file_size,
            progress=cb,
        )
    else:
        await asyncio.to_thread(
            download_url_range,
            source["url"],
            tmp,
            task,
            offset=part.offset,
            length=part.length,
            expected_size=part.length,
            progress=cb,
        )


# ----------------------------------------------------------------------
# Modo clásico: descarga completa y luego sube por trozos.
# ----------------------------------------------------------------------
async def _run_classic(client, task: UploadTask, source: dict, chunk_bytes: int,
                       per_task: int) -> None:
    user_dir = os.path.join(CONFIG.download_dir, str(task.user_id))
    local_path = os.path.join(user_dir, safe_local_name(task.filename))
    task.phase = "Descargando"

    last = {"n": 0}

    def cb(written: int) -> None:
        delta = written - last["n"]
        if delta > 0:
            last["n"] = written
            task.add_fetched(delta)

    if source["kind"] == "telegram":
        await download_telegram_full(client, source["media"], local_path, task, progress=cb)
    else:
        await asyncio.to_thread(
            download_url_range, source["url"], local_path, task, progress=cb
        )

    if task.is_cancelled:
        raise TaskCancelled("cancelada por el usuario")

    size = os.path.getsize(local_path)
    task.file_size = size
    max_bytes = RUNTIME_SETTINGS.max_file_bytes
    if size > max_bytes:
        raise RuntimeError(
            f"el archivo descargado ({_size(size)}) supera el máximo ({_size(max_bytes)})"
        )
    task.set_fetched(size)

    base_name, parts = plan_parts(task.user_id, task.filename, size, chunk_bytes)
    task.s3_base = base_name
    task.plan_parts(chunk_bytes, len(parts))
    task.status = TaskStatus.UPLOADING
    task.phase = "Subiendo"

    log.info(
        "[%s] modo clásico | %.1f MB en %d partes",
        task.task_id, size / 1024 / 1024, len(parts),
    )

    async def process(part):
        tmp = _tmp_path(task, part.number)
        try:
            await asyncio.to_thread(
                s3().slice_to_file, local_path, part.offset, part.length, tmp
            )
            await _upload_part(task, part, tmp)
        finally:
            _remove(tmp)

    try:
        await _run_parts(task, parts, process, per_task)
    finally:
        _remove(local_path)


# ----------------------------------------------------------------------
# Subida de una parte (con límite global + reintentos + cancelación).
# ----------------------------------------------------------------------
async def _upload_part(task: UploadTask, part, tmp_path: str) -> None:
    if task.is_cancelled:
        raise TaskCancelled("cancelada por el usuario")

    task.part_started(part.number)
    loop = asyncio.get_running_loop()
    try:
        async with GLOBAL_UPLOAD_GATE:
            await loop.run_in_executor(
                pool(),
                _upload_sync,
                tmp_path,
                part.key,
                part.part_name,
                task,
                part.label,
                part.number,
            )
    except TaskCancelled:
        task.part_failed(part.number)
        raise
    except ChunkUploadError:
        task.part_failed(part.number)
        task.failed_part = part.label
        raise
    except Exception as exc:  # noqa: BLE001
        task.part_failed(part.number)
        task.failed_part = part.label
        raise ChunkUploadError(part.label, 1, 1, str(exc)) from exc

    task.part_finished(part.number, part.length)
    QUEUE.note_bytes(part.length)
    log.debug("[%s] %s subida (%.1f MB)", task.task_id, part.label, part.length / 1024 / 1024)


def _upload_sync(tmp_path: str, key: str, filename: str, task: UploadTask,
                 label: str, part_number: int) -> None:
    """Se ejecuta en un hilo del pool: sube el trozo y guarda su clave."""
    s3().upload_part(tmp_path, key, filename, task, label)
    with task._lock:
        task.keys_by_part[part_number] = key


# ----------------------------------------------------------------------
# Motor de partes en paralelo controlado.
# ----------------------------------------------------------------------
async def _run_parts(task: UploadTask, parts: Sequence, process: Callable,
                     per_task: int) -> None:
    """Ejecuta `process(part)` manteniendo como máximo `per_task` en vuelo."""
    running: set = set()
    first_error: Optional[BaseException] = None

    async def collect(done: set) -> None:
        nonlocal first_error
        for fut in done:
            exc = fut.exception()
            if exc and first_error is None:
                first_error = exc

    for part in parts:
        if task.is_cancelled:
            first_error = first_error or TaskCancelled("cancelada por el usuario")
            break
        while len(running) >= per_task:
            done, running = await asyncio.wait(running, return_when=asyncio.FIRST_COMPLETED)
            await collect(done)
            if first_error is not None:
                break
        if first_error is not None:
            break
        running.add(asyncio.create_task(process(part), name=f"part-{part.number}"))

    if running:
        done, _ = await asyncio.wait(running)
        await collect(done)

    if first_error is not None:
        raise first_error


# ----------------------------------------------------------------------
# Mensajes auxiliares.
# ----------------------------------------------------------------------
async def _safe_status(task: UploadTask, text: str) -> None:
    await notifier.edit_status(task, text, keyboard=T.progress_keyboard(task.task_id))


async def _final_status(task: UploadTask, text: str) -> None:
    if not await notifier.edit_status(task, text):
        await notifier.send_status(task, text)


# ----------------------------------------------------------------------
# Helpers de archivos.
# ----------------------------------------------------------------------
def _tmp_path(task: UploadTask, part_number: int) -> str:
    directory = os.path.join(CONFIG.download_dir, str(task.user_id), "chunks")
    ensure_dir(directory)
    return os.path.join(directory, f"{task.task_id}_p{part_number:03d}.tmp")


def _remove(path: Optional[str]) -> None:
    if not path:
        return
    try:
        if os.path.exists(path):
            os.remove(path)
    except OSError:
        pass


def _cleanup_task_files(task: UploadTask) -> None:
    """Borra los temporales de la tarea (archivo completo y trozos)."""
    user_dir = os.path.join(CONFIG.download_dir, str(task.user_id))
    if os.path.isdir(user_dir):
        for entry in os.scandir(user_dir):
            if entry.is_file() and task.task_id in entry.name:
                _remove(entry.path)
    chunks = os.path.join(user_dir, "chunks")
    if os.path.isdir(chunks):
        for entry in os.scandir(chunks):
            if entry.name.startswith(task.task_id) and entry.is_file():
                _remove(entry.path)
        try:
            if not os.listdir(chunks):
                os.rmdir(chunks)
        except OSError:
            pass


def _size(n: int) -> str:
    from .progress import human_size

    return human_size(n)


def cleanup_downloads_root() -> None:
    """Limpia restos de ejecuciones anteriores (se llama al arrancar)."""
    try:
        root = CONFIG.download_dir
        if not os.path.isdir(root):
            return
        for entry in os.scandir(root):
            if entry.is_dir():
                shutil.rmtree(entry.path, ignore_errors=True)
            else:
                os.remove(entry.path)
        log.info("Carpeta de descargas limpiada")
    except Exception as exc:  # pragma: no cover
        log.debug("No se pudo limpiar %s: %s", CONFIG.download_dir, exc)
