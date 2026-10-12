"""Stub de la librería `todus` para pruebas locales (sin red)."""

from __future__ import annotations

import os
import time


class ToDusError(RuntimeError):
    pass


class S3Client:
    """Imita la API real: put / get / head / delete / list / exists / build_url."""

    base_url = "https://s3.todus.cu/stream"
    fail_times = 0          # cuántas veces fallar antes de aceptar (para probar reintentos)
    put_delay = 0.01        # retardo simulado por subida

    def __init__(self, base_url=None, timeout=60, max_retries=3, chunk_size=65536, session=None):
        self.base = (base_url or self.base_url).rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries
        self.chunk_size = chunk_size
        self.uploads = []

    def put(self, local_path, key, content_type=None, filename=None, inline=False,
            show_progress=True, extra_headers=None):
        if type(self).fail_times > 0:
            type(self).fail_times -= 1
            raise ToDusError("fallo simulado de red")
        size = os.path.getsize(local_path)
        time.sleep(type(self).put_delay)
        self.uploads.append(key)
        return {"success": True, "etag": "abc123", "size": size, "duration": 0.01,
                "status_code": 200, "error": None}

    def build_url(self, key):
        return f"{self.base}/{key}"

    def delete(self, key):
        return True

    def head(self, key):
        return None

    def exists(self, key):
        return False

    def list(self, prefix=None, max_keys=None):
        return []
