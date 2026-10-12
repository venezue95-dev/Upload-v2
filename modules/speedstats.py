"""
speedstats.py
=============
Estimador de velocidad GLOBAL y persistente.

Por qué existe
--------------
La librería `todus` sube cada parte con un único PUT: mientras una parte está
en vuelo, el bot no recibe ningún aviso de bytes. Si solo se midiera la
velocidad con los bytes ya confirmados, pasaría lo que pasaba antes:

  · archivos de 1 parte (los más comunes) → barra congelada en 0 % y 0 B/s
    durante TODA la subida, y salto final a 100 %;
  · archivos de varias partes → saltos enormes (0 % → 30 % → 60 %) y picos de
    velocidad falsos (se contabilizaba una parte entera en 0,5 s).

Solución: además de medir, se **aprende**. Cada parte terminada aporta su
velocidad media real (bytes / duración). Con eso:

  1. Se mantiene una media exponencial (EWMA) de subida y de bajada.
  2. Esa media se usa como *prior* para estimar el avance de las partes que
     están en vuelo → la barra se mueve desde el primer segundo, incluso con
     una sola parte.
  3. Se guarda en disco (`data/speed_stats.json`), así el bot arranca ya con
     una estimación realista de tu servidor y de tus subidas anteriores.

Los valores aprendidos se marcan como estimados ("~") en la interfaz hasta que
exista una medición real de la tarea en curso.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from typing import Optional

log = logging.getLogger("s3_bot.speed")

# Estimaciones iniciales conservadoras (se sobreescriben al primer uso).
DEFAULT_UPLOAD_BPS = 700 * 1024        # 700 KB/s
DEFAULT_DOWNLOAD_BPS = 1500 * 1024     # 1.5 MB/s

# Cuánto pesa cada medición nueva en la media (0.25 = suave, 0.5 = reactiva).
ALPHA = 0.3

# Límites de cordura: evitan que una medición absurda (disco local, caché)
# envenene la estimación durante horas.
MIN_BPS = 32 * 1024                    # 32 KB/s
MAX_BPS = 200 * 1024 * 1024            # 200 MB/s


class SpeedStats:
    """Media exponencial de velocidades, persistida entre reinicios."""

    def __init__(self, path: Optional[str] = None) -> None:
        self._lock = threading.Lock()
        self._path = path
        self.upload_bps: float = 0.0        # 0 = sin medición todavía
        self.download_bps: float = 0.0
        self.samples_upload = 0
        self.samples_download = 0
        self.updated_at = 0.0
        self._last_save = 0.0

    # ------------------------------------------------------------------
    # Configuración / carga.
    # ------------------------------------------------------------------
    def configure(self, path: str) -> None:
        self._path = path
        self._load()

    def _load(self) -> None:
        if not self._path or not os.path.exists(self._path):
            return
        try:
            with open(self._path, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            with self._lock:
                self.upload_bps = float(data.get("upload_bps") or 0.0)
                self.download_bps = float(data.get("download_bps") or 0.0)
                self.samples_upload = int(data.get("samples_upload") or 0)
                self.samples_download = int(data.get("samples_download") or 0)
                self.updated_at = float(data.get("updated_at") or 0.0)
            log.info(
                "Velocidades aprendidas: subida %.2f MB/s (%d muestras) · "
                "bajada %.2f MB/s (%d muestras)",
                self.upload_bps / 1024 / 1024, self.samples_upload,
                self.download_bps / 1024 / 1024, self.samples_download,
            )
        except Exception as exc:  # pragma: no cover
            log.debug("No se pudo leer %s: %s", self._path, exc)

    def save(self, force: bool = False) -> None:
        if not self._path:
            return
        now = time.time()
        if not force and now - self._last_save < 5:
            return
        with self._lock:
            data = {
                "upload_bps": round(self.upload_bps, 1),
                "download_bps": round(self.download_bps, 1),
                "samples_upload": self.samples_upload,
                "samples_download": self.samples_download,
                "updated_at": round(now, 1),
            }
        try:
            os.makedirs(os.path.dirname(self._path) or ".", exist_ok=True)
            tmp = f"{self._path}.tmp"
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(data, fh, indent=2)
            os.replace(tmp, self._path)
            self._last_save = now
        except Exception as exc:  # pragma: no cover
            log.debug("No se pudo guardar %s: %s", self._path, exc)

    # ------------------------------------------------------------------
    # Aprendizaje.
    # ------------------------------------------------------------------
    def note_upload(self, nbytes: int, seconds: float) -> float:
        """Registra la velocidad media real de una parte subida."""
        return self._note("upload_bps", "samples_upload", nbytes, seconds)

    def note_download(self, nbytes: int, seconds: float) -> float:
        """Registra la velocidad media real de una descarga terminada."""
        return self._note("download_bps", "samples_download", nbytes, seconds)

    def _note(self, attr: str, samples_attr: str, nbytes: int, seconds: float) -> float:
        if nbytes <= 0 or seconds <= 0.2:
            return getattr(self, attr)
        rate = nbytes / seconds
        if rate < MIN_BPS or rate > MAX_BPS:
            return getattr(self, attr)
        with self._lock:
            previous = getattr(self, attr)
            new_value = rate if previous <= 0 else (1 - ALPHA) * previous + ALPHA * rate
            setattr(self, attr, new_value)
            setattr(self, samples_attr, getattr(self, samples_attr) + 1)
            self.updated_at = time.time()
        self.save()
        return new_value

    def seed_from_history(self, upload_bps: float, download_bps: float = 0.0) -> None:
        """Siembra la estimación con el histórico (solo si no hay muestras)."""
        with self._lock:
            if self.upload_bps <= 0 and upload_bps > 0:
                self.upload_bps = min(MAX_BPS, max(MIN_BPS, upload_bps))
            if self.download_bps <= 0 and download_bps > 0:
                self.download_bps = min(MAX_BPS, max(MIN_BPS, download_bps))

    # ------------------------------------------------------------------
    # Consulta.
    # ------------------------------------------------------------------
    def upload_estimate(self) -> float:
        """Velocidad a usar para estimar progreso/ETA de subida."""
        with self._lock:
            return self.upload_bps if self.upload_bps > 0 else DEFAULT_UPLOAD_BPS

    def download_estimate(self) -> float:
        with self._lock:
            return self.download_bps if self.download_bps > 0 else DEFAULT_DOWNLOAD_BPS

    def has_upload_measurement(self) -> bool:
        with self._lock:
            return self.upload_bps > 0

    def summary(self) -> str:
        """Texto corto para /status: valores efectivos y si están aprendidos."""
        with self._lock:
            up = self.upload_bps or DEFAULT_UPLOAD_BPS
            down = self.download_bps or DEFAULT_DOWNLOAD_BPS
            up_n, down_n = self.samples_upload, self.samples_download
        up_tag = f"{up_n} muestras" if up_n else "por defecto"
        down_tag = f"{down_n} muestras" if down_n else "por defecto"
        marca = "aprendida" if (up_n or down_n) else "sin datos aún"
        return (
            f"subida {up / 1024 / 1024:.2f} MB/s ({up_tag}) · "
            f"bajada {down / 1024 / 1024:.2f} MB/s ({down_tag}) · {marca}"
        )


# Singleton global.
SPEEDS = SpeedStats()
