"""
downloader.py
=============
Descargas de los dos orígenes soportados:

- **Telegram**: descarga normal o por rangos (``offset``/``length``) usando
  ``Client.get_file``. Descargar por rangos permite que el bot suba un trozo
  a toDus mientras descarga el siguiente (modo streaming), con lo que usa
  mucho menos disco y termina antes.
- **URL http(s)**: ``HEAD`` para conocer tamaño/nombre y descarga por rangos
  (si el servidor soporta ``Accept-Ranges``) o en streaming normal.

Todas las funciones respetan la cancelación de la tarea y comprueban el
espacio libre en disco antes de empezar.
"""

from __future__ import annotations

import ipaddress
import logging
import os
import re
import shutil
import socket
import time
from typing import Callable, Optional, Tuple
from urllib.parse import unquote, urlparse

import requests

from config import CONFIG
from .queue import TaskCancelled, UploadTask

log = logging.getLogger("s3_bot.downloader")

MI_B = 1024 * 1024          # Pyrogram trabaja con bloques de 1 MiB al pedir rangos
HTTP_TIMEOUT = (15, 60)
USER_AGENT = "todus-bot/2.0 (+telegram)"


# ----------------------------------------------------------------------
# Utilidades básicas.
# ----------------------------------------------------------------------
def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def disk_free(path: str) -> int:
    try:
        return int(shutil.disk_usage(path).free)
    except Exception:  # pragma: no cover
        return 0


def ensure_free_space(path: str, needed_bytes: int) -> Tuple[bool, str]:
    """Comprueba que hay espacio para `needed_bytes` + reserva mínima."""
    ensure_dir(path)
    free = disk_free(path)
    reserve = int(CONFIG.min_free_gb * 1024 ** 3)
    if free - needed_bytes < reserve:
        return False, (
            f"espacio insuficiente en disco (libre {free / 1024**3:.1f} GB, "
            f"necesario {needed_bytes / 1024**3:.1f} GB + reserva "
            f"{CONFIG.min_free_gb:.1f} GB)"
        )
    return True, "ok"


def safe_local_name(name: str, fallback: str = "descarga.bin") -> str:
    name = os.path.basename(name or "").strip() or fallback
    name = re.sub(r'[\\/:*?"<>|\r\n\t]+', "_", name)
    name = name.strip(" .") or fallback
    return name[:180]


def url_to_filename(url: str, fallback: str = "descarga_web.bin") -> str:
    parsed = urlparse(url)
    name = unquote(os.path.basename(parsed.path))
    return safe_local_name(name, fallback)


# ----------------------------------------------------------------------
# Validación de URLs (SSRF básico).
# ----------------------------------------------------------------------
def check_url(url: str) -> Tuple[bool, str]:
    """Valida esquema y (opcionalmente) que no apunte a red interna."""
    try:
        parsed = urlparse(url)
    except Exception:
        return False, "URL inválida"

    if parsed.scheme not in ("http", "https"):
        return False, "solo se aceptan enlaces http o https"
    if not parsed.hostname:
        return False, "la URL no tiene host"

    if CONFIG.allow_private_urls:
        return True, "ok"

    try:
        infos = socket.getaddrinfo(parsed.hostname, None)
    except Exception:
        return False, "no se pudo resolver el dominio"

    for info in infos:
        try:
            ip = ipaddress.ip_address(info[4][0])
        except ValueError:
            continue
        if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved:
            return False, "el enlace apunta a una red interna (bloqueado por seguridad)"
    return True, "ok"


# ----------------------------------------------------------------------
# Metadatos de una URL.
# ----------------------------------------------------------------------
def probe_url(url: str, session: Optional[requests.Session] = None) -> dict:
    """HEAD (con fallback a GET) para conocer tamaño, nombre y rangos."""
    session = session or requests.Session()
    info = {"size": 0, "filename": "", "ranges": False, "content_type": ""}
    headers = {"User-Agent": USER_AGENT}

    try:
        r = session.head(url, allow_redirects=True, timeout=HTTP_TIMEOUT, headers=headers)
        if r.status_code >= 400 or "content-length" not in r.headers:
            r = session.get(url, stream=True, allow_redirects=True,
                            timeout=HTTP_TIMEOUT, headers=headers)
        info["size"] = int(r.headers.get("content-length") or 0)
        info["ranges"] = "bytes" in (r.headers.get("accept-ranges") or "").lower()
        info["content_type"] = r.headers.get("content-type", "")

        # Nombre sugerido por el servidor (Content-Disposition).
        disp = r.headers.get("content-disposition") or ""
        match = re.search(r"filename\*?=(?:UTF-8'')?\"?([^\";]+)", disp, re.I)
        if match:
            info["filename"] = safe_local_name(unquote(match.group(1)))

        # Algunos servidores no envían Accept-Ranges pero sí soportan Range.
        if not info["ranges"]:
            try:
                r2 = session.get(url, headers={**headers, "Range": "bytes=0-0"},
                                 stream=True, timeout=HTTP_TIMEOUT)
                info["ranges"] = r2.status_code == 206
                r2.close()
            except Exception:
                pass
        r.close()
    except Exception as exc:
        log.debug("probe_url(%s) falló: %s", url, exc)
    return info


# ----------------------------------------------------------------------
# Descarga de URLs.
# ----------------------------------------------------------------------
def download_url_range(
    url: str,
    dest_path: str,
    task: UploadTask,
    *,
    offset: Optional[int] = None,
    length: Optional[int] = None,
    expected_size: int = 0,
    progress: Optional[Callable[[int], None]] = None,
) -> int:
    """
    Descarga (total o parcialmente) una URL a ``dest_path``.

    Devuelve los bytes escritos. Lanza ``TaskCancelled`` si el usuario cancela.
    """
    headers = {"User-Agent": USER_AGENT}
    if offset is not None and length is not None:
        headers["Range"] = f"bytes={offset}-{offset + length - 1}"

    written = 0
    with requests.get(url, stream=True, timeout=HTTP_TIMEOUT, headers=headers,
                      allow_redirects=True) as r:
        if offset is not None and r.status_code not in (200, 206):
            raise RuntimeError(f"el servidor rechazó la descarga parcial (HTTP {r.status_code})")

        r.raise_for_status()
        total = int(r.headers.get("content-length") or 0)
        limit = length if length is not None else total or None

        with open(dest_path, "wb") as fh:
            for block in r.iter_content(chunk_size=512 * 1024):
                if task.is_cancelled:
                    raise TaskCancelled("cancelada por el usuario")
                if not block:
                    continue
                if limit is not None:
                    remaining = limit - written
                    if remaining <= 0:
                        break
                    block = block[:remaining]
                fh.write(block)
                written += len(block)
                if progress:
                    progress(written)

    if expected_size and written < expected_size:
        raise RuntimeError(
            f"descarga incompleta: {written} de {expected_size} bytes"
        )
    return written


# ----------------------------------------------------------------------
# Descarga de Telegram.
# ----------------------------------------------------------------------
async def download_telegram_range(
    client,
    media,
    dest_path: str,
    task: UploadTask,
    *,
    offset: int = 0,
    length: Optional[int] = None,
    file_size: int = 0,
    progress: Optional[Callable[[int], None]] = None,
) -> int:
    """
    Descarga un rango [offset, offset+length) de un archivo de Telegram.

    ``offset`` debe ser múltiplo de 1 MiB (los trozos son múltiplos, así que
    siempre lo es). Devuelve los bytes escritos.
    """
    from pyrogram.file_id import FileId

    file_id = FileId.decode(media.file_id)
    if offset % MI_B != 0:
        raise RuntimeError("el offset de descarga debe ser múltiplo de 1 MiB")

    start_chunk = offset // MI_B
    if length is None:
        chunks = 0                      # 0 = hasta el final
    else:
        chunks = (length + MI_B - 1) // MI_B

    written = 0
    limit = length if length is not None else None

    with open(dest_path, "wb") as fh:
        async for block in client.get_file(
            file_id=file_id,
            file_size=file_size,
            limit=chunks,
            offset=start_chunk,
        ):
            if task.is_cancelled:
                raise TaskCancelled("cancelada por el usuario")
            if limit is not None:
                remaining = limit - written
                if remaining <= 0:
                    break
                block = block[:remaining]
            fh.write(block)
            written += len(block)
            if progress:
                progress(written)

    if length is not None and written < length:
        raise RuntimeError(
            f"descarga incompleta de Telegram: {written} de {length} bytes"
        )
    return written


async def download_telegram_full(
    client,
    media,
    dest_path: str,
    task: UploadTask,
    *,
    progress: Optional[Callable[[int], None]] = None,
) -> int:
    """Descarga completa por bloques de 1 MiB desde el evento asíncrono."""
    from pyrogram.file_id import FileId

    file_id = FileId.decode(media.file_id)
    file_size = int(getattr(media, "file_size", 0) or 0)
    written = 0
    with open(dest_path, "wb") as fh:
        async for block in client.get_file(file_id=file_id, file_size=file_size):
            if task.is_cancelled:
                raise TaskCancelled("cancelada por el usuario")
            fh.write(block)
            written += len(block)
            if progress:
                progress(written)
    return written


# ----------------------------------------------------------------------
# Limpieza de temporales.
# ----------------------------------------------------------------------
def cleanup_user_dir(path: str, keep_days: float = 1.0) -> int:
    """Borra archivos temporales huérfanos de una carpeta."""
    removed = 0
    now = time.time()
    if not os.path.isdir(path):
        return 0
    for entry in os.scandir(path):
        try:
            if entry.is_file() and (now - entry.stat().st_mtime) > keep_days * 86400:
                os.remove(entry.path)
                removed += 1
            elif entry.is_dir() and entry.name == "chunks":
                shutil.rmtree(entry.path, ignore_errors=True)
        except OSError:
            pass
    return removed


def cleanup_all_temp(base_dir: str) -> None:
    """Limpia la carpeta de descargas al arrancar (no debe quedar nada)."""
    try:
        for entry in os.scandir(base_dir):
            if entry.is_dir():
                shutil.rmtree(entry.path, ignore_errors=True)
            else:
                os.remove(entry.path)
    except FileNotFoundError:
        pass
    except OSError as exc:  # pragma: no cover
        log.debug("cleanup_all_temp: %s", exc)
