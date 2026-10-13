"""
runtime_settings.py
===================
Configuración mutable en caliente desde el propio bot (/settings).

- Se guarda en ``settings.json`` (junto a la sesión) para sobrevivir reinicios.
- Los valores iniciales salen del ``.env`` (CONFIG), así que si un valor del
  .env no está en la lista cíclica, se respeta y se cicla desde ahí.
- Solo el owner puede modificar; el resto únicamente ve los valores.
"""

from __future__ import annotations

import json
import logging
import os
import threading
from typing import Any, Dict, List, Sequence

from config import CONFIG

log = logging.getLogger("s3_bot.settings")


# ----------------------------------------------------------------------
# Opciones cíclicas + etiquetas + unidades + páginas del menú.
# ----------------------------------------------------------------------
CYCLE_OPTIONS: Dict[str, List[Any]] = {
    "max_file_mb": [500, 800, 1000, 2000],
    "chunk_size_mb": [10, 25, 50, 100, 200, 400, 800],
    "max_parallel": [1, 2, 3, 4],
    "global_chunk_workers": [2, 4, 6, 8, 12],
    "max_retries": [3, 5, 10],
    "max_concurrent_tasks": [2, 3, 4, 6, 8],
    "max_active_per_user": [1, 2, 3],
    "max_queue_per_user": [5, 10, 15, 25],
    "max_queue_total": [50, 100, 200, 400],
    "rate_limit_sec": [0, 3, 10, 30],
    "pipeline_mode": ["auto", "stream", "classic"],
    "progress_interval": [2.0, 3.0, 5.0, 10.0],
    # Nuevas
    "verify_upload": [True, False],
    "daily_quota_mb": [0, 1024, 2048, 5120, 10240],
    "maintenance": [False, True],
}

CYCLE_LABELS: Dict[str, str] = {
    "max_file_mb": "📏 Máx. por archivo",
    "chunk_size_mb": "🧩 Trozo (chunk)",
    "max_parallel": "⚡️ Partes en paralelo (por tarea)",
    "global_chunk_workers": "🏭 Subidas paralelas (todo el bot)",
    "max_retries": "🔁 Reintentos por parte",
    "max_concurrent_tasks": "🚦 Tareas simultáneas (global)",
    "max_active_per_user": "👤 Tareas activas por usuario",
    "max_queue_per_user": "📥 En espera por usuario",
    "max_queue_total": "📚 Cola total (espera)",
    "rate_limit_sec": "🐢 Espera entre envíos (seg)",
    "pipeline_mode": "🌊 Modo de trabajo",
    "progress_interval": "⏱️ Refresco del progreso (seg)",
    "verify_upload": "🔍 Verificar subidas (HEAD)",
    "daily_quota_mb": "📊 Cuota diaria por usuario",
    "maintenance": "🛠 Mantenimiento (bloquear envíos)",
}

CYCLE_UNITS: Dict[str, str] = {
    "max_file_mb": "MB",
    "chunk_size_mb": "MB",
    "rate_limit_sec": "s",
    "progress_interval": "s",
    "daily_quota_mb": "MB/día",
}

# Los ajustes agrupados en páginas (así caben en el teclado inline).
SETTINGS_PAGES: List[List[str]] = [
    [  # 1 · Cola y usuarios
        "max_concurrent_tasks",
        "max_active_per_user",
        "max_queue_per_user",
        "max_queue_total",
    ],
    [  # 2 · Subida y troceo
        "chunk_size_mb",
        "max_parallel",
        "global_chunk_workers",
        "max_retries",
        "max_file_mb",
    ],
    [  # 3 · Avanzado, integridad y control
        "rate_limit_sec",
        "pipeline_mode",
        "progress_interval",
        "verify_upload",
        "daily_quota_mb",
        "maintenance",
    ],
]

DEFAULTS: Dict[str, Any] = {
    "max_file_mb": CONFIG.s3.max_file_mb,
    "chunk_size_mb": CONFIG.s3.chunk_size_mb,
    "max_parallel": CONFIG.s3.max_parallel,
    "global_chunk_workers": CONFIG.s3.global_workers,
    "max_retries": CONFIG.s3.max_retries,
    "max_concurrent_tasks": CONFIG.queue.max_concurrent_tasks,
    "max_active_per_user": CONFIG.queue.max_active_per_user,
    "max_queue_per_user": CONFIG.queue.max_queue_per_user,
    "max_queue_total": CONFIG.queue.max_queue_total,
    "rate_limit_sec": CONFIG.queue.rate_limit_sec,
    "pipeline_mode": CONFIG.stream_mode,
    "progress_interval": CONFIG.progress_interval,
    "verify_upload": True,
    "daily_quota_mb": 0,
    "maintenance": False,
}


def _coerce(key: str, value: Any) -> Any:
    """Convierte un valor leído de settings.json al tipo del ajuste (None = inválido)."""
    default = DEFAULTS[key]
    try:
        if isinstance(default, bool):
            return value if isinstance(value, bool) else None
        if isinstance(default, int):
            if isinstance(value, bool):
                return None
            return int(value)
        if isinstance(default, float):
            return float(value)
        if isinstance(default, str):
            if key == "pipeline_mode" and value not in CYCLE_OPTIONS["pipeline_mode"]:
                return None
            return str(value)
    except (TypeError, ValueError):
        return None
    return value


class RuntimeSettings:
    """Wrapper thread-safe sobre ``settings.json``."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._values: Dict[str, Any] = dict(DEFAULTS)
        self._load()

    # ------------------------------------------------------------------
    # Lectura.
    # ------------------------------------------------------------------
    def __getattr__(self, item: str) -> Any:
        if item.startswith("_"):
            raise AttributeError(item)
        values = self.__dict__.get("_values", {})
        if item in values:
            return values[item]
        raise AttributeError(f"Ajuste desconocido: {item}")

    def get(self, key: str, default: Any = None) -> Any:
        """Lectura segura: nunca lanza (útil para valores del .env, no cíclicos)."""
        if key in self._values:
            return self._values[key]
        if key in DEFAULTS:
            return DEFAULTS[key]
        return default

    def as_dict(self) -> Dict[str, Any]:
        with self._lock:
            return dict(self._values)

    @property
    def max_file_bytes(self) -> int:
        return int(self.get("max_file_mb")) * 1024 * 1024

    @property
    def chunk_bytes(self) -> int:
        return int(self.get("chunk_size_mb")) * 1024 * 1024

    def page_for(self, key: str) -> int:
        for idx, keys in enumerate(SETTINGS_PAGES, start=1):
            if key in keys:
                return idx
        return 1

    # ------------------------------------------------------------------
    # Mutación.
    # ------------------------------------------------------------------
    def cycle(self, key: str) -> Any:
        """Avanza al siguiente valor de la opción cíclica y persiste."""
        if key not in CYCLE_OPTIONS:
            raise KeyError(f"Opción no ciclable: {key}")
        options: Sequence[Any] = CYCLE_OPTIONS[key]
        with self._lock:
            current = self._values.get(key)
            try:
                idx = list(options).index(current)
            except ValueError:
                # Valor del .env que no está en la lista: el siguiente mayor.
                idx = -1
                for pos, option in enumerate(options):
                    if isinstance(option, (int, float)) and isinstance(current, (int, float)):
                        if option > current:
                            idx = pos - 1
                            break
            next_val = options[(idx + 1) % len(options)]
            self._values[key] = next_val
        self._save()
        log.info("Ajuste %s = %s", key, next_val)
        return next_val

    def set_value(self, key: str, value: Any) -> None:
        if key not in DEFAULTS:
            raise KeyError(key)
        with self._lock:
            self._values[key] = value
        self._save()

    def reset(self) -> None:
        with self._lock:
            self._values = dict(DEFAULTS)
        self._save()
        log.info("Ajustes restaurados a los valores del .env")

    # ------------------------------------------------------------------
    # Persistencia.
    # ------------------------------------------------------------------
    def _load(self) -> None:
        path = CONFIG.settings_file
        if not os.path.exists(path):
            return
        try:
            with open(path, "r", encoding="utf-8") as fh:
                data = json.load(fh)
        except Exception as exc:
            log.warning("settings.json ilegible (%s); se usan valores del .env", exc)
            return
        if not isinstance(data, dict):
            log.warning("settings.json no es un objeto; se usan valores del .env")
            return
        with self._lock:
            for key, value in data.items():
                if key not in DEFAULTS:
                    continue
                coerced = _coerce(key, value)
                if coerced is None:
                    log.warning("settings.json: valor inválido para %s (%r); se ignora", key, value)
                    continue
                self._values[key] = coerced
        log.info("Ajustes cargados desde %s: %s", path, self._values)

    def _save(self) -> None:
        with self._lock:
            data = dict(self._values)
        try:
            path = CONFIG.settings_file
            os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
            tmp = f"{path}.tmp"
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(data, fh, indent=2, ensure_ascii=False)
            os.replace(tmp, path)
        except Exception as exc:  # pragma: no cover
            log.warning("No se pudo guardar settings.json: %s", exc)


RUNTIME_SETTINGS = RuntimeSettings()
