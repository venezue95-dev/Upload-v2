"""
s3_client.py
============
Adaptador de la librería ``todus`` (``todus.S3Client``, v3.x) con:

- reintentos por trozo con backoff exponencial y espera cancelable,
- cancelación cooperativa desde el botón del bot,
- verificación opcional de tamaño (HEAD) tras cada subida,
- borrado de claves y utilidades para trocear archivos.

Este módulo NO descarga nada: recibe archivos locales (o trozos) y los sube.
El límite global de subidas simultáneas (DynamicSemaphore) vive en pipeline.py.
"""

from __future__ import annotations

import logging
import os
import random
import re
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import List, Optional, Tuple

from config import CONFIG
from .queue import TaskCancelled, UploadTask

log = logging.getLogger("s3_bot.s3")


# ----------------------------------------------------------------------
# Excepciones.
# ----------------------------------------------------------------------
class S3Error(RuntimeError):
    """Error genérico del almacenamiento."""


class ChunkUploadError(S3Error):
    """Una parte agotó todos sus reintentos."""

    def __init__(self, part_label: str, attempts: int, max_attempts: int, error: str):
        self.part_label = part_label
        self.attempts = attempts
        self.max_attempts = max_attempts
        self.error = error
        super().__init__(f"Parte {part_label} falló tras {attempts}/{max_attempts} intentos: {error}")


# Compatibilidad con el nombre anterior.
UploadCancelled = TaskCancelled


# ----------------------------------------------------------------------
# Pool de hilos dedicado (no bloquea el executor por defecto de asyncio).
# ----------------------------------------------------------------------
_POOL: Optional[ThreadPoolExecutor] = None
_POOL_LOCK = threading.Lock()


def pool() -> ThreadPoolExecutor:
    global _POOL
    if _POOL is None:
        with _POOL_LOCK:
            if _POOL is None:
                size = max(4, int(CONFIG.s3.global_workers) + 4)
                _POOL = ThreadPoolExecutor(max_workers=size, thread_name_prefix="s3up")
                log.info("Pool de subidas: %d hilos", size)
    return _POOL


def shutdown_pool() -> None:
    global _POOL
    if _POOL is not None:
        _POOL.shutdown(wait=False, cancel_futures=True)
        _POOL = None


# ----------------------------------------------------------------------
# Descripción de una parte del archivo.
# ----------------------------------------------------------------------
@dataclass
class PartSpec:
    number: int          # 1-based
    offset: int
    length: int
    part_name: str       # nombre visible del trozo
    key: str             # key final en el bucket
    label: str           # "parte 3" (para mensajes de error)


def safe_filename(name: str) -> str:
    """Nombre sin caracteres problemáticos para S3/Content-Disposition."""
    name = os.path.basename(name or "archivo.bin")
    name = re.sub(r'[\\/:*?"<>|\r\n\t]+', "_", name).strip(" .")
    return name or "archivo.bin"


_PART_RE = re.compile(r"\.part(\d{3,})(?:\.[^./]*)?$")


def part_number_from_key(key: str) -> Optional[int]:
    """'.../peli.part007.mkv' -> 7. Devuelve None si la clave no es de una parte."""
    match = _PART_RE.search(os.path.basename(key or ""))
    return int(match.group(1)) if match else None


def pretty_key(user_id: int) -> Tuple[str, str]:
    """
    Devuelve (nombre_base_visible, carpeta_base_en_el_bucket).

    Estructura de las claves (una carpeta única por subida, para que las
    partes convivan juntas y los enlaces sean legibles):

        users/tg_<user_id>/telegram/<AAAAMMDD-HHMMSS>-<hash>/<archivo>

    Cada usuario mantiene su "namespace" (tg_<user_id>) y así los archivos
    no se mezclan entre usuarios.
    """
    stamp = time.strftime("%Y%m%d-%H%M%S")
    short = uuid.uuid4().hex[:6]
    folder = f"{stamp}-{short}"
    prefix = CONFIG.s3.key_prefix.strip("/")
    base_key = f"{prefix}/tg_{user_id}/telegram/{folder}"
    return folder, base_key


def plan_parts(user_id: int, original_name: str, file_size: int, chunk_size: int,
               folder: Optional[str] = None) -> Tuple[str, List[PartSpec]]:
    """
    Divide el archivo en partes y calcula las keys.

    Con una sola parte se conserva el nombre original; con varias se usa
    ``nombre.part001.ext``, etc. (así el usuario sabe cómo unirlos).
    """
    chunk_size = max(1, int(chunk_size))
    num_parts = max(1, (file_size + chunk_size - 1) // chunk_size)

    if folder:
        # Reanudación: se reutiliza la carpeta del intento anterior para que las
        # partes ya subidas sigan siendo válidas (mismas claves y mismos enlaces).
        prefix = CONFIG.s3.key_prefix.strip("/")
        base_key = f"{prefix}/tg_{user_id}/telegram/{folder}"
    else:
        folder, base_key = pretty_key(user_id)

    clean = safe_filename(original_name)
    stem, ext = os.path.splitext(clean)

    parts: List[PartSpec] = []
    for i in range(num_parts):
        offset = i * chunk_size
        length = min(chunk_size, file_size - offset)
        part_name = clean if num_parts == 1 else f"{stem}.part{i + 1:03d}{ext}"
        parts.append(PartSpec(
            number=i + 1,
            offset=offset,
            length=length,
            part_name=part_name,
            key=f"{base_key}/{part_name}",
            label=f"parte {i + 1}/{num_parts}",
        ))
    return folder, parts


# ----------------------------------------------------------------------
# Backend: todus.S3Client.
# ----------------------------------------------------------------------
class _LowLevelBackend:
    """Envoltorio fino sobre ``todus.S3Client``."""

    name = "todus.S3Client"

    def __init__(self) -> None:
        from todus import S3Client as TodusS3Client  # type: ignore

        kwargs = {
            "timeout": CONFIG.s3.timeout,
            "max_retries": 1,          # los reintentos los controla el bot
            "chunk_size": 512 * 1024,  # lectura por bloques al subir
        }
        if CONFIG.s3.base_url:
            kwargs["base_url"] = CONFIG.s3.base_url
        self.client = TodusS3Client(**kwargs)

    def put_file(self, local_path: str, key: str, filename: str) -> None:
        result = self.client.put(
            local_path=local_path,
            key=key,
            filename=filename,
            inline=CONFIG.s3.inline,
            show_progress=False,
        )
        if not result or not result.get("success"):
            raise S3Error(
                f"PUT rechazado (HTTP {result.get('status_code') if result else '?'}): "
                f"{str(result.get('error') if result else 'sin respuesta')[:200]}"
            )

    def build_url(self, key: str) -> str:
        # La lib devuelve ".../stream/users/..." (URL-encoded): es la URL pública.
        return self.client.build_url(key)

    def delete(self, key: str) -> bool:
        try:
            return bool(self.client.delete(key))
        except Exception as exc:  # pragma: no cover
            log.debug("delete(%s) falló: %s", key, exc)
            return False

    def head(self, key: str) -> Optional[dict]:
        try:
            return self.client.head(key)
        except Exception:
            return None

    def ping(self) -> Tuple[bool, str]:
        """
        Comprueba que el endpoint contesta.

        ``client.head()`` NO sirve para esto: devuelve ``None`` tanto cuando la
        clave no existe (404) como cuando no hay red, así que siempre parecería
        "accesible". Se hace una petición directa: cualquier respuesta HTTP
        (aunque sea 404/403) significa que el endpoint está vivo.
        """
        try:
            key = f"{CONFIG.s3.key_prefix}/_healthcheck_{uuid.uuid4().hex[:8]}"
            url = self.client.build_url(key)
            resp = self.client.session.head(url, timeout=min(10, CONFIG.s3.timeout))
            if resp.status_code >= 500:
                return False, f"el endpoint respondió HTTP {resp.status_code}"
            return True, f"endpoint toDus accesible (HTTP {resp.status_code})"
        except Exception as exc:  # noqa: BLE001
            return False, f"sin respuesta del endpoint: {exc}"


# ----------------------------------------------------------------------
# Cliente del bot.
# ----------------------------------------------------------------------
class S3Client:
    """Fachada única usada por el resto del bot."""

    _backend: Optional[_LowLevelBackend] = None
    _lock = threading.Lock()

    def __init__(self, cfg=None) -> None:  # cfg se acepta por compatibilidad
        self.backend = self._get_backend()

    @classmethod
    def _get_backend(cls) -> _LowLevelBackend:
        if cls._backend is not None:
            return cls._backend
        with cls._lock:
            if cls._backend is None:
                try:
                    import todus  # noqa: F401
                except Exception as exc:  # pragma: no cover
                    raise S3Error(
                        "La librería 'todus' no está instalada. Instálala con:\n"
                        "  pip install todus-client==3.5.0"
                    ) from exc
                import todus as todus_mod

                if not hasattr(todus_mod, "S3Client"):
                    raise S3Error(
                        "La librería 'todus' instalada no expone S3Client: "
                        "actualízala a la versión 3.x."
                    )
                cls._backend = _LowLevelBackend()
                log.info("Backend de almacenamiento: %s", cls._backend.name)
            return cls._backend

    # ------------------------------------------------------------------
    # Subida de una parte (bloqueante; se llama desde un hilo).
    # ------------------------------------------------------------------
    def upload_part(
        self,
        local_path: str,
        key: str,
        filename: str,
        task: UploadTask,
        label: str,
    ) -> None:
        """Sube un trozo con reintentos. Lanza ChunkUploadError o TaskCancelled."""
        from .runtime_settings import RUNTIME_SETTINGS

        max_retries = max(1, int(RUNTIME_SETTINGS.max_retries))
        base_delay = CONFIG.s3.retry_base_delay
        max_delay = CONFIG.s3.retry_max_delay
        last_error = "error desconocido"
        attempts = 0

        while attempts < max_retries:
            if task.is_cancelled:
                raise TaskCancelled("cancelada por el usuario")
            attempts += 1
            try:
                self.backend.put_file(local_path, key, filename)
                return
            except TaskCancelled:
                raise
            except Exception as exc:  # noqa: BLE001
                last_error = str(exc) or exc.__class__.__name__
                if attempts >= max_retries:
                    break
                delay = min(max_delay, base_delay * (2 ** (attempts - 1)))
                delay += random.uniform(0, delay * 0.25)  # jitter anti-estampida
                log.warning(
                    "[%s] %s falló (intento %d/%d): %s → reintento en %.1fs",
                    task.task_id, label, attempts, max_retries, last_error, delay,
                )
                if not self._sleep_cancellable(task, delay):
                    raise TaskCancelled("cancelada por el usuario")

        raise ChunkUploadError(label, attempts, max_retries, last_error)

    # ------------------------------------------------------------------
    # Utilidades.
    # ------------------------------------------------------------------
    @staticmethod
    def _sleep_cancellable(task: UploadTask, seconds: float) -> bool:
        """Espera fraccionada: reacciona al botón de cancelar al instante."""
        end = time.monotonic() + seconds
        while time.monotonic() < end:
            if task.is_cancelled:
                return False
            time.sleep(min(0.25, max(0.0, end - time.monotonic())))
        return not task.is_cancelled

    @staticmethod
    def slice_to_file(src_path: str, offset: int, length: int, dest_path: str) -> None:
        """Copia un rango del archivo a un temporal (para subirlo como parte)."""
        remaining = length
        with open(src_path, "rb") as src, open(dest_path, "wb") as dst:
            src.seek(offset)
            while remaining > 0:
                block = src.read(min(1024 * 1024, remaining))
                if not block:
                    break
                dst.write(block)
                remaining -= len(block)

    def build_url(self, key: str, task: Optional[UploadTask] = None) -> str:
        return self.backend.build_url(key)

    # ------------------------------------------------------------------
    # Verificación de integridad.
    # ------------------------------------------------------------------
    def verify_part(self, key: str, expected_size: int) -> Tuple[bool, str]:
        """
        Comprueba con HEAD que el objeto subido tiene el tamaño esperado.

        Devuelve (ok, detalle). Si el endpoint no responde a HEAD se considera
        correcto (no todos los S3 compatibles lo soportan) — nunca se rompe una
        subida por un chequeo opcional.
        """
        try:
            info = self.backend.head(key)
        except Exception as exc:  # pragma: no cover
            return True, f"sin verificación ({exc})"
        if not info:
            return True, "sin verificación (HEAD no soportado)"
        size = int(info.get("size") or 0)
        if size <= 0:
            return True, "sin verificación (tamaño no informado)"
        if size != expected_size:
            return False, f"tamaño en el almacenamiento {size} != esperado {expected_size}"
        return True, "ok"

    def delete_keys(self, keys: List[str]) -> Tuple[List[str], List[str]]:
        """Borra una lista de keys. Devuelve (borradas, fallidas) como listas de keys."""
        deleted: List[str] = []
        failed: List[str] = []
        for key in keys:
            (deleted if self.backend.delete(key) else failed).append(key)
        return deleted, failed

    def ping(self) -> Tuple[bool, str]:
        try:
            return self.backend.ping()
        except Exception as exc:
            return False, str(exc)


_CLIENT: Optional[S3Client] = None


def get_client() -> S3Client:
    """Cliente compartido por todo el bot (se crea la primera vez que se pide)."""
    global _CLIENT
    if _CLIENT is None:
        _CLIENT = S3Client(CONFIG.s3)
    return _CLIENT
