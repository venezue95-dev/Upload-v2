"""
progress.py
===========
Utilidades de formato para la interfaz del bot (tamaños, velocidades,
tiempos, barras de progreso). Sin dependencias del bot: se puede usar
desde cualquier módulo.
"""

from __future__ import annotations

from typing import Callable, Optional

# Ancho por defecto de las barras.
BAR_WIDTH = 18


# ----------------------------------------------------------------------
# Tamaños / velocidades / tiempos.
# ----------------------------------------------------------------------
def human_size(num: float) -> str:
    """1536 -> '1.50 KB'."""
    if num is None or num < 0:
        return "—"
    num = float(num)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if num < 1024.0:
            if unit == "B":
                return f"{int(num)} B"
            return f"{num:.2f}".rstrip("0").rstrip(".") + f" {unit}"
        num /= 1024.0
    return f"{num:.2f}".rstrip("0").rstrip(".") + " PB"


def human_size_compact(num: float) -> str:
    """1536 -> '1.5 KB' (más corto, para líneas de progreso)."""
    if num is None or num <= 0:
        return "0 B"
    num = float(num)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if num < 1024.0:
            if unit == "B":
                return f"{int(num)} B"
            return f"{num:.1f}".rstrip("0").rstrip(".") + f" {unit}"
        num /= 1024.0
    return f"{num:.1f}".rstrip("0").rstrip(".") + " PB"


def human_speed(bps: float) -> str:
    """1024 -> '1.00 KB/s'."""
    if not bps or bps <= 0:
        return "0 B/s"
    return f"{human_size(bps)}/s"


def human_time(seconds: Optional[float]) -> str:
    """Segundos -> 'MM:SS' / 'H:MM:SS'. None o negativo -> '—'."""
    if seconds is None or seconds < 0:
        return "—"
    seconds = int(seconds)
    if seconds < 60:
        return f"{seconds}s"
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    if h:
        return f"{h}h {m:02d}m"
    return f"{m}m {s:02d}s"


def human_time_clock(seconds: Optional[float]) -> str:
    """Segundos -> 'MM:SS' o 'HH:MM:SS' (estilo cronómetro)."""
    if seconds is None or seconds < 0:
        return "--:--"
    seconds = int(seconds)
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    if h:
        return f"{h:02d}:{m:02d}:{s:02d}"
    return f"{m:02d}:{s:02d}"


def bar(percent: float, width: int = BAR_WIDTH,
        filled: str = "⬢", empty: str = "⬡") -> str:
    """Barra visual: '⬢⬢⬢⬢⬡⬡⬡⬡' con porcentaje acotado a 0–100."""
    percent = max(0.0, min(100.0, percent or 0.0))
    done = int(round(width * percent / 100.0))
    return filled * done + empty * (width - done)


def percent_str(percent: float) -> str:
    value = f"{max(0.0, min(100.0, percent or 0.0)):.1f}".rstrip("0").rstrip(".")
    return f"{value}%"


# ----------------------------------------------------------------------
# Velocímetro (media móvil exponencial suavizada).
# ----------------------------------------------------------------------
class SpeedMeter:
    """
    Calcula la velocidad (bytes/s) de forma estable.

    La primera muestra define el origen (evita picos al arrancar) y luego
    se aplica una media móvil exponencial para suavizar los saltos.
    """

    __slots__ = ("_last_t", "_last_bytes", "_alpha", "speed", "_samples")

    def __init__(self, alpha: float = 0.35) -> None:
        self._last_t: Optional[float] = None
        self._last_bytes: int = 0
        self._alpha = alpha
        self.speed: float = 0.0
        self._samples: int = 0

    def update(self, total_bytes: int, now: float) -> float:
        """Registra el total acumulado en el instante `now` (time.monotonic)."""
        if self._last_t is None:
            self._last_t, self._last_bytes = now, total_bytes
            return self.speed

        dt = now - self._last_t
        if dt < 0.5:
            return self.speed

        delta = max(0, total_bytes - self._last_bytes)
        instant = delta / dt
        self._last_t, self._last_bytes = now, total_bytes
        self._samples += 1

        if self.speed <= 0 or instant <= 0:
            self.speed = instant
        else:
            self.speed = (1 - self._alpha) * self.speed + self._alpha * instant
        return self.speed

    def eta(self, remaining_bytes: float) -> Optional[float]:
        if remaining_bytes <= 0:
            return 0.0
        if self.speed <= 0:
            return None
        return remaining_bytes / self.speed

    def reset(self) -> None:
        self._last_t = None
        self._last_bytes = 0
        self.speed = 0.0
        self._samples = 0


# ----------------------------------------------------------------------
# Velocímetro por eventos (para la SUBIDA).
# ----------------------------------------------------------------------
class RateMeter:
    """
    Media exponencial de velocidad alimentada por *eventos completos*
    (una parte subida = bytes / duración real).

    Se usa para la subida porque el PUT de cada parte no informa de bytes
    intermedios: medir con "bytes confirmados / 0,5 s" daba picos falsos y
    barras congeladas.

    `prior_fn` devuelve la estimación global aprendida (speedstats); se usa
    mientras la tarea no tenga ninguna medición propia, de modo que la barra
    y la ETA funcionan desde el primer segundo.
    """

    __slots__ = ("_value", "_prior_fn", "_alpha", "_samples")

    def __init__(self, prior_fn: Optional[Callable[[], float]] = None,
                 alpha: float = 0.3) -> None:
        self._value: float = 0.0
        self._prior_fn = prior_fn
        self._alpha = alpha
        self._samples = 0

    def note(self, nbytes: int, seconds: float) -> float:
        """Registra una medición real (bytes subidos en `seconds`)."""
        if nbytes <= 0 or seconds <= 0.2:
            return self.value()
        rate = nbytes / seconds
        if self._value <= 0:
            self._value = rate
        else:
            self._value = (1 - self._alpha) * self._value + self._alpha * rate
        self._samples += 1
        return self._value

    def value(self) -> float:
        """Velocidad medida; si aún no hay muestras, la estimación global."""
        if self._value > 0:
            return self._value
        if self._prior_fn is not None:
            try:
                return float(self._prior_fn())
            except Exception:  # pragma: no cover
                return 0.0
        return 0.0

    @property
    def measured(self) -> bool:
        """True si hay al menos una medición real de esta tarea."""
        return self._value > 0

    @property
    def samples(self) -> int:
        return self._samples

    def reset(self) -> None:
        self._value = 0.0
        self._samples = 0
