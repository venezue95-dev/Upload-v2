"""
s3_client.py
============
Cliente de subida a toDus S3, con soporte para las DOS versiones de la
librería ``todus``:

1. ``todus.S3Client``  (v3.x, la actual): cliente low-level con
   ``put() / head() / delete() / build_url()``. **Es la que espera el bot.**
2. ``todus.NamespaceManager`` (v2.x, antiguas): namespaces por usuario con
   ``ns.upload()`` y ``ns.share_url()``.

El bot detecta automáticamente cuál está instalada, así que si mañana
cambian la librería el bot sigue funcionando. (El código original hacía
``from todus import NamespaceManager`` directamente: con la versión actual
eso rompía el arranque con ImportError.)

Este módulo NO descarga nada: recibe archivos locales (o trozos) y los
sube con:
- reintentos por trozo con backoff exponencial y espera cancelable,
- cancelación cooperativa desde el botón del bot,
- límite global de subidas simultáneas (DynamicSemaphore, ajustable en
  caliente desde /settings).
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


def plan_parts(user_id: int, original_name: str, file_size: int, chunk_size: int) -> Tuple[str, List[PartSpec]]:
    """
    Divide el archivo en partes y calcula las keys.

    Con una sola parte se conserva el nombre original; con varias se usa
    ``nombre.part001.ext``, etc. (así el usuario sabe cómo unirlos).
    """
    chunk_size = max(1, int(chunk_size))
    num_parts = max(1, (file_size + chunk_size - 1) // chunk_size)
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
# Backends (todus 3.x o 2.x).
# ----------------------------------------------------------------------
class _LowLevelBackend:
    """todus.S3Client (v3.x)."""

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
        url = self.client.build_url(key)
        # La lib devuelve ".../stream/users/..." (URL-encoded). Es la pública.
        return url

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
        # Comprobamos que el endpoint responde (404 esperado para una key random).
        try:
            key = f"{CONFIG.s3.key_prefix}/_healthcheck_{uuid.uuid4().hex[:8]}"
            self.client.head(key)
            return True, "endpoint toDus accesible"
        except Exception as exc:
            return False, f"sin respuesta del endpoint: {exc}"


class _NamespaceBackend:
    """todus.NamespaceManager (v2.x, versiones antiguas)."""

    name = "todus.NamespaceManager"

    def __init__(self) -> None:
        from todus import NamespaceManager  # type: ignore

        self.manager = NamespaceManager()
        self._cache: dict = {}
        self._lock = threading.Lock()

    def _namespace(self, user_id: int, username: str = ""):
        name = f"tg_{user_id}"
        with self._lock:
            if name in self._cache:
                return self._cache[name]
        try:
            if not self.manager.exists(name):
                self.manager.create(name, description=f"@{username or 'unknown'}")
            ns = self.manager.get_namespace(name)
        except Exception as exc:
            raise S3Error(f"No se pudo preparar el namespace {name}: {exc}") from exc
        with self._lock:
            self._cache[name] = ns
        return ns

    def put_file(self, local_path: str, key: str, filename: str, user_id: int = 0,
                 username: str = "") -> None:
        ns = self._namespace(user_id, username)
        path = "/".join(key.split("/")[3:-1]) or "telegram"
        ns.upload(local_path=local_path, path=path, original_name=filename,
                  metadata={"from": "telegram"})

    def build_url(self, key: str, user_id: int = 0, username: str = "") -> str:
        ns = self._namespace(user_id, username)
        # La key interna usada por la lib es el nombre del objeto subido.
        obj_name = key.split("/")[-1]
        obj_name = obj_name if not key.endswith(("/part", "")) else obj_name
        share = ns.share_url(obj_name)
        return share.url if hasattr(share, "url") else str(share)

    def delete(self, key: str) -> bool:
        try:
            ns = self._namespace(0)
            ns.delete(key.split("/")[-1])
            return True
        except Exception:
            return False

    def ping(self) -> Tuple[bool, str]:
        try:
            self.manager  # construido correctamente
            return True, "NamespaceManager listo (auth interna de la librería)"
        except Exception as exc:
            return False, f"librería todus no responde: {exc}"


# ----------------------------------------------------------------------
# Cliente del bot.
# ----------------------------------------------------------------------
class S3Client:
    """Fachada única usada por el resto del bot."""

    _backend = None
    _lock = threading.Lock()

    def __init__(self, cfg=None) -> None:  # cfg se acepta por compatibilidad
        self.backend = self._get_backend()

    @classmethod
    def _get_backend(cls):
        if cls._backend is not None:
            return cls._backend
        with cls._lock:
            if cls._backend is not None:
                return cls._backend
            try:
                import todus  # noqa: F401
            except Exception as exc:  # pragma: no cover
                raise S3Error(
                    "La librería 'todus' no está instalada. Instálala con:\n"
                    "  pip install git+https://github.com/nyxthor-dev/todus-client.git"
                ) from exc

            import todus as todus_mod

            if hasattr(todus_mod, "S3Client"):
                backend = _LowLevelBackend()
            elif hasattr(todus_mod, "NamespaceManager"):
                backend = _NamespaceBackend()
            else:
                raise S3Error(
                    "La librería 'todus' instalada no expone S3Client ni "
                    "NamespaceManager: revisa la versión."
                )
            cls._backend = backend
            log.info("Backend de almacenamiento: %s", backend.name)
            return backend

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
        *,
        user_id: Optional[int] = None,
        username: str = "",
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
                if isinstance(self.backend, _NamespaceBackend):
                    self.backend.put_file(local_path, key, filename,
                                          user_id=user_id or task.user_id,
                                          username=username or task.username)
                else:
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
        if isinstance(self.backend, _NamespaceBackend):
            return self.backend.build_url(
                key,
                user_id=task.user_id if task else 0,
                username=task.username if task else "",
            )
        return self.backend.build_url(key)

    def delete_keys(self, keys: List[str]) -> Tuple[int, int]:
        """Borra una lista de keys. Devuelve (borradas, fallidas)."""
        ok = 0
        failed = 0
        for key in keys:
            if self.backend.delete(key):
                ok += 1
            else:
                failed += 1
        return ok, failed

    def ping(self) -> Tuple[bool, str]:
        try:
            return self.backend.ping()
        except Exception as exc:
            return False, str(exc)
