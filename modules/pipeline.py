"""
pipeline.py
===========
Ejecutor de una tarea: descarga → divide en trozos → sube a toDus → finaliza.

Dos modos de trabajo (ajustable en /settings):

- **classic** (modo ``auto`` por defecto): descarga todo el archivo y luego lo
  sube por trozos. Necesita espacio para el archivo completo, pero las fases
  se ven separadas (descarga → subida) y es el más robusto.
- **stream** (``pipeline_mode=stream``): descarga y sube *a la vez*. Cada trozo
  se descarga a un temporal pequeño y se sube enseguida, así el disco usado es
  de unos pocos trozos. Solo se usa si el origen permite descargas parciales
  (Telegram siempre; URLs con ``Accept-Ranges``); si no, cae a classic.

En ambos modos el trabajo pesado de subida ocurre en hilos del pool global, y
la subida simultánea de partes se limita con un semáforo dinámico
(``global_chunk_workers``) para que 40 usuarios no lancen 300 PUT a la vez.
"""

from __future__ import annotations

import asyncio
import logging
import os
import shutil
import time
from typing import Callable, List, Optional, Sequence

from config import CONFIG
from . import keyboards as K
from . import notifier
from . import storage
from .media_downloader import download_media, is_media_url, probe_media
from .playstore_downloader import download_play_store, package_from_url
from . import texts as T
from .downloader import (
    acheck_url,
    download_telegram_full,
    download_telegram_range,
    download_url_range,
    ensure_dir,
    ensure_free_space,
    local_path_for,
    probe_url,
    safe_local_name,
    url_to_filename,
)
from .limiter import DynamicSemaphore
from .queue import QUEUE, TaskCancelled, TaskStatus, UploadTask
from .runtime_settings import RUNTIME_SETTINGS
from .s3_client import ChunkUploadError, S3Client, S3Error, get_client, plan_parts, pool
from .speedstats import SPEEDS

log = logging.getLogger("s3_bot.pipeline")

# Semáforo global de subidas (límite ajustable en caliente desde /settings).
GLOBAL_UPLOAD_GATE = DynamicSemaphore(6, limit_fn=lambda: RUNTIME_SETTINGS.global_chunk_workers)

_MI_B = 1024 * 1024


def s3() -> S3Client:
    return get_client()


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
                task.reset_for_retry()
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
            f"Archivo demasiado grande: {_size(size)} (máximo {_size(max_bytes)}). "
            "Pide al owner que suba el límite en /settings.",
        )
        return

    task.file_size = size

    # Para medios cuyo tamaño no estaba disponible al encolar, la cuota se
    # reserva aquí con el tamaño real obtenido por yt-dlp. La operación es
    # idempotente para tareas que ya tenían una reserva inicial.
    allowed, used, limit = storage.reserve_quota(
        task.user_id,
        task.task_id,
        size,
        owner=(CONFIG.owner_id is not None and task.user_id == CONFIG.owner_id),
    )
    if not allowed:
        task.mark_finished(
            TaskStatus.FAILED,
            f"Cuota diaria insuficiente: usados/reservados {used} de {limit} bytes.",
        )
        return

    # ---------------- 2. Modo de trabajo ----------------
    user_chunk = int(getattr(task, "requested_chunk_mb", 0) or 0)
    chunk_bytes = max(_MI_B, user_chunk * _MI_B if user_chunk else settings.chunk_bytes)
    if task.resume_folder and task.chunk_size:
        # Reanudación: las partes ya subidas se cortaron con el tamaño de trozo
        # de entonces. Si el owner lo cambió entretanto, hay que conservarlo o
        # el archivo reconstruido saldría corrupto.
        chunk_bytes = task.chunk_size
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
        task.mark_finished(
            TaskStatus.FAILED,
            f"Sin espacio en disco: {detail}. Avisa al owner para que libere espacio.",
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
    if task.source == "playstore":
        package = package_from_url(task.url or "")
        if not package:
            raise RuntimeError("enlace de Google Play inválido")
        return {
            "kind": "playstore",
            "url": task.url,
            "size": 0,
            "name": f"{package}.apk",
            "ranges": False,
            "media": None,
        }
    if task.source == "media":
        url = (task.url or "").strip()
        ok, why = await acheck_url(url)
        if not ok:
            raise RuntimeError(f"URL rechazada: {why}")
        if not is_media_url(url):
            raise RuntimeError("el dominio no está habilitado para descargas multimedia")
        info = await asyncio.to_thread(
            probe_media, url, task.media_format, task.media_quality
        )
        return {
            "kind": "media",
            "url": url,
            "size": int(info.get("size") or 0),
            "name": info.get("filename") or task.filename,
            "ranges": False,
            "media": None,
        }
    if task.source == "url":
        url = (task.url or "").strip()
        ok, why = await acheck_url(url)
        if not ok:
            raise RuntimeError(f"URL rechazada: {why}")
        info = await asyncio.to_thread(probe_url, url)
        name = info.get("filename") or task.filename or url_to_filename(url)
        return {
            "kind": "url",
            "url": info.get("source_url") or url,
            "resolved_url": info.get("url") or url,
            "size": int(info.get("size") or task.file_size or 0),
            "name": name,
            "ranges": bool(info.get("ranges")),
            "media": None,
        }

    # Origen Telegram: recuperamos el mensaje (por si el bot se reinició).
    message = await client.get_messages(task.chat_id, task.message_id)
    if message is None or getattr(message, "empty", False):
        raise RuntimeError("el mensaje original ya no está disponible en Telegram")
    media = (
        message.document or message.video or message.audio or message.voice
        or message.video_note or message.photo
    )
    if media is None:
        raise RuntimeError("el mensaje ya no contiene un archivo descargable")
    size = int(getattr(media, "file_size", 0) or 0)
    # El nombre que vio el usuario al enviarlo (foto_123.jpg, nota_de_voz…) se
    # conserva si Telegram no trae ``file_name``.
    name = getattr(media, "file_name", None) or task.filename or f"archivo_{task.task_id}.bin"
    return {
        "kind": "telegram",
        "url": None,
        "size": size,
        "name": name,
        "ranges": True,          # siempre podemos pedir rangos a Telegram
        "media": media,
    }


def _choose_mode(pipeline_mode: str, source: dict, size: int, chunk_bytes: int) -> str:
    """
    ``stream`` solo si el usuario lo pidió Y el origen permite descargas
    parciales Y se conoce el tamaño; en cualquier otro caso, ``classic``.
    ``auto`` equivale a classic (fases separadas y fáciles de seguir).
    """
    if pipeline_mode == "stream" and size and source.get("ranges"):
        return "stream"
    return "classic"


# ----------------------------------------------------------------------
# Modo streaming: descarga y sube trozo a trozo.
# ----------------------------------------------------------------------
async def _run_stream(client, task: UploadTask, source: dict, chunk_bytes: int,
                      per_task: int) -> None:
    size = task.file_size
    base_name, parts = plan_parts(task.user_id, task.filename, size, chunk_bytes,
                                 folder=task.resume_folder)
    task.s3_base = base_name
    task.plan_parts(chunk_bytes, len(parts))
    task.phase = "Descargando y subiendo" if not task.resumed_parts else "Reanudando"

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

    await _run_parts(task, parts, process, per_task, skip=set(task.resumed_parts))


async def _fetch_part(client, task: UploadTask, source: dict, part, tmp: str) -> None:
    def cb(written: int) -> None:
        # Progreso por parte (monótono) → la barra de descarga no retrocede.
        task.note_fetched(part.number, written)

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
    local_path = local_path_for(task)
    ensure_dir(os.path.dirname(local_path))
    task.phase = "Descargando"

    last = {"n": 0}

    def cb(written: int) -> None:
        delta = written - last["n"]
        if delta > 0:
            last["n"] = written
            task.add_fetched(delta)

    download_started = time.monotonic()
    if source["kind"] == "telegram":
        await download_telegram_full(client, source["media"], local_path, task, progress=cb)
    elif source["kind"] == "media":
        generated = await asyncio.to_thread(
            download_media,
            source["url"],
            os.path.join(os.path.dirname(local_path), task.task_id),
            task,
            task.media_format,
            task.media_quality,
        )
        if os.path.abspath(generated) != os.path.abspath(local_path):
            os.replace(generated, local_path)
    elif source["kind"] == "playstore":
        generated = await download_play_store(
            source["url"],
            os.path.join(os.path.dirname(local_path), task.task_id),
            task,
        )
        if os.path.abspath(generated) != os.path.abspath(local_path):
            os.replace(generated, local_path)
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
    allowed, used, limit = storage.reserve_quota(
        task.user_id,
        task.task_id,
        size,
        owner=(CONFIG.owner_id is not None and task.user_id == CONFIG.owner_id),
    )
    if not allowed:
        raise RuntimeError(
            f"cuota diaria insuficiente después de descargar: {used} de {limit} bytes"
        )
    task.set_fetched(size)
    # Aprendemos la velocidad real de bajada para futuras estimaciones.
    SPEEDS.note_download(size, time.monotonic() - download_started)

    base_name, parts = plan_parts(task.user_id, task.filename, size, chunk_bytes,
                                 folder=task.resume_folder)
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
        await _run_parts(task, parts, process, per_task, skip=set(task.resumed_parts))
    finally:
        _remove(local_path)


# ----------------------------------------------------------------------
# Subida de una parte (con límite global + reintentos + cancelación).
# ----------------------------------------------------------------------
async def _upload_part(task: UploadTask, part, tmp_path: str) -> None:
    if task.is_cancelled:
        raise TaskCancelled("cancelada por el usuario")

    task.part_started(part.number)
    started = time.monotonic()
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
                part.length,
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

    task.part_finished(part.number, part.length, time.monotonic() - started)
    QUEUE.note_bytes(part.length)
    log.debug("[%s] %s subida (%.1f MB)", task.task_id, part.label, part.length / 1024 / 1024)


def _upload_sync(tmp_path: str, key: str, filename: str, task: UploadTask,
                 label: str, part_number: int, expected_size: int) -> None:
    """
    Se ejecuta en un hilo del pool: sube el trozo, lo verifica y guarda su clave.

    La verificación (HEAD) detecta subidas truncadas: el PUT puede responder
    "OK" y haber guardado menos bytes de los previstos. Si pasa, se repite la
    subida una vez antes de dar la parte por fallida.
    """
    verify = RUNTIME_SETTINGS.get("verify_upload", True)
    for attempt in (1, 2):
        s3().upload_part(tmp_path, key, filename, task, label)
        if not verify:
            break
        ok, detail = s3().verify_part(key, expected_size)
        if ok:
            break
        if attempt == 2:
            raise ChunkUploadError(label, 2, 2, f"verificación fallida: {detail}")
        log.warning("[%s] %s: verificación fallida (%s); se repite la subida",
                    task.task_id, label, detail)

    with task._lock:
        task.keys_by_part[part_number] = key


# ----------------------------------------------------------------------
# Motor de partes en paralelo controlado.
# ----------------------------------------------------------------------
async def _run_parts(task: UploadTask, parts: Sequence, process: Callable,
                     per_task: int, skip: Optional[set] = None) -> None:
    """
    Ejecuta `process(part)` manteniendo como máximo `per_task` en vuelo.

    `skip` contiene números de parte ya subidas en un intento anterior: no se
    vuelven a descargar ni a subir (reanudación de tareas fallidas).
    """
    running: set = set()
    first_error: Optional[BaseException] = None
    skip = skip or set()

    if skip:
        pendientes = [p for p in parts if p.number not in skip]
        log.info(
            "[%s] reanudando: %d de %d partes ya estaban subidas",
            task.task_id, len(parts) - len(pendientes), len(parts),
        )
        parts = pendientes

    async def collect(done: set) -> None:
        nonlocal first_error
        for fut in done:
            if fut.cancelled():
                exc: Optional[BaseException] = TaskCancelled("tarea interrumpida")
            else:
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
    await notifier.edit_status(task, text, keyboard=K.task_message_keyboard(task))


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
        task_media_dir = os.path.join(user_dir, task.task_id)
        if os.path.isdir(task_media_dir):
            shutil.rmtree(task_media_dir, ignore_errors=True)
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
