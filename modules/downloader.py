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

import asyncio
import ipaddress
import logging
import os
import re
import shutil
import socket
from typing import Callable, Optional, Tuple
from urllib.parse import unquote, urljoin, urlparse

import requests

from config import CONFIG
from .queue import TaskCancelled, UploadTask

log = logging.getLogger("s3_bot.downloader")

MI_B = 1024 * 1024          # Pyrogram trabaja con bloques de 1 MiB al pedir rangos
HTTP_TIMEOUT = (15, 60)
USER_AGENT = "todus-bot/2.0 (+telegram)"
TELEGRAM_DOWNLOAD_RETRIES = 3


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


def local_path_for(task: UploadTask) -> str:
    """
    Ruta del archivo completo en modo clásico.

    Lleva el id de la tarea para que dos tareas del mismo usuario con el mismo
    nombre (p. ej. dos "video.mp4") no se pisen el temporal.
    """
    return os.path.join(
        CONFIG.download_dir, str(task.user_id),
        f"{task.task_id}_{safe_local_name(task.filename)}",
    )


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


async def acheck_url(url: str) -> Tuple[bool, str]:
    """``check_url`` sin bloquear el event loop (resuelve DNS en un hilo)."""
    return await asyncio.to_thread(check_url, url)


MAX_REDIRECTS = 5
_REDIRECT_CODES = (301, 302, 303, 307, 308)


def _safe_request(session: requests.Session, method: str, url: str, **kwargs):
    """
    Petición que sigue las redirecciones a mano validando CADA salto.

    Con ``allow_redirects=True`` un servidor público podía redirigir a
    ``http://127.0.0.1/...`` o a la red interna y saltarse ``check_url``.
    Bloqueante: llamar siempre desde un hilo.
    """
    for _ in range(MAX_REDIRECTS + 1):
        ok, why = check_url(url)
        if not ok:
            raise RuntimeError(f"URL rechazada: {why}")
        resp = session.request(method, url, allow_redirects=False, **kwargs)
        location = resp.headers.get("location")
        if resp.status_code in _REDIRECT_CODES and location:
            resp.close()
            url = urljoin(url, location)
            continue
        return resp
    raise RuntimeError("demasiadas redirecciones")


# ----------------------------------------------------------------------
# Metadatos de una URL.
# ----------------------------------------------------------------------
def probe_url(url: str, session: Optional[requests.Session] = None) -> dict:
    """HEAD (con fallback a GET) para conocer tamaño, nombre y rangos."""
    session = session or requests.Session()
    original_url = url
    try:
        from .filehost import resolve_download_url
        url = resolve_download_url(url, session)
    except Exception as exc:  # noqa: BLE001
        log.debug("resolve_download_url(%s) falló: %s", url, exc)
        raise
    info = {"size": 0, "filename": "", "ranges": False, "content_type": "", "url": url}
    headers = {"User-Agent": USER_AGENT, "Accept-Encoding": "identity"}
    r = None

    try:
        r = _safe_request(session, "HEAD", url, timeout=HTTP_TIMEOUT, headers=headers)
        if r.status_code >= 400 or "content-length" not in r.headers:
            r.close()
            r = _safe_request(session, "GET", url, stream=True,
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
            r2 = None
            try:
                r2 = _safe_request(session, "GET", url, timeout=HTTP_TIMEOUT, stream=True,
                                   headers={**headers, "Range": "bytes=0-0"})
                info["ranges"] = r2.status_code == 206
            except Exception:  # noqa: BLE001
                pass
            finally:
                if r2 is not None:
                    r2.close()
    except Exception as exc:  # noqa: BLE001
        log.debug("probe_url(%s) falló: %s", url, exc)
    finally:
        if r is not None:
            r.close()
    info["source_url"] = original_url
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

    - Se pide ``Accept-Encoding: identity``: con gzip, ``Content-Length`` es el
      tamaño COMPRIMIDO y el archivo salía truncado.
    - En descargas parciales con ``offset > 0`` se exige HTTP 206. Si el
      servidor ignora ``Range`` y devuelve el archivo entero, el trozo
      contendría los bytes del principio y el archivo final quedaría corrupto.
    """
    headers = {"User-Agent": USER_AGENT, "Accept-Encoding": "identity"}
    original_url = url
    ranged = offset is not None and length is not None
    if ranged:
        headers["Range"] = f"bytes={offset}-{offset + length - 1}"

    written = 0
    with requests.Session() as session:
        from .filehost import is_filehost_url, resolve_download_url
        if is_filehost_url(url):
            url = resolve_download_url(url, session)
        r = _safe_request(session, "GET", url, stream=True,
                          timeout=HTTP_TIMEOUT, headers=headers)
        try:
            if ranged and (offset or 0) > 0 and r.status_code != 206:
                raise RuntimeError(
                    f"el servidor no admite descargas parciales (HTTP {r.status_code})"
                )
            r.raise_for_status()
            if is_filehost_url(original_url) and "text/html" in (r.headers.get("content-type") or "").lower():
                raise RuntimeError("el alojamiento devolvió una página HTML en vez del archivo")
            total = int(r.headers.get("content-length") or 0)
            limit = length if ranged else None

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
        finally:
            r.close()

    wanted = expected_size or (length if ranged else total)
    if wanted and written < wanted:
        raise RuntimeError(f"descarga incompleta: {written} de {wanted} bytes")
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

    expected = length if length is not None else max(0, file_size - offset)
    # Pyrogram puede absorber ciertos errores de GetFile y devolver un
    # generador vacío. Repetimos la parte completa ante fallos temporales.
    for attempt in range(1, TELEGRAM_DOWNLOAD_RETRIES + 1):
        written = 0
        limit = length if length is not None else None
        try:
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

            if not expected or written >= expected:
                return written
        except TaskCancelled:
            raise
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "descarga Telegram falló en intento %d/%d (offset=%d): %s",
                attempt, TELEGRAM_DOWNLOAD_RETRIES, offset, exc,
            )

        if attempt < TELEGRAM_DOWNLOAD_RETRIES:
            await asyncio.sleep(min(2 * attempt, 5))

    raise RuntimeError(
        f"descarga incompleta de Telegram: {written} de {expected or length} bytes "
        f"tras {TELEGRAM_DOWNLOAD_RETRIES} intentos"
    )


async def download_telegram_full(
    client,
    media,
    dest_path: str,
    task: UploadTask,
    *,
    progress: Optional[Callable[[int], None]] = None,
) -> int:
    """Descarga completa por bloques de 1 MiB desde el evento asíncrono."""
    file_size = int(getattr(media, "file_size", 0) or 0)
    return await download_telegram_range(
        client,
        media,
        dest_path,
        task,
        offset=0,
        length=file_size or None,
        file_size=file_size,
        progress=progress,
    )
