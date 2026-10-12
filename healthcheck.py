"""
healthcheck.py
==============
Comprobación de salud para Docker (HEALTHCHECK).

No basta con que el proceso exista: el bot puede estar "vivo" pero con la cola
bloqueada. Por eso el planificador escribe un latido (`data/heartbeat`) cada
pocos segundos y aquí se verifica que siga fresco.

Salida: 0 = sano, 1 = no responde.
"""

from __future__ import annotations

import os
import sys
import time

MAX_AGE = 180      # segundos sin latido antes de considerarlo caído


def main() -> int:
    try:
        from config import CONFIG
    except Exception as exc:  # noqa: BLE001
        print(f"[healthcheck] configuración inválida: {exc}")
        return 1

    path = os.path.join(os.path.dirname(CONFIG.state_file) or ".", "heartbeat")
    if not os.path.exists(path):
        print("[healthcheck] aún sin latido (arrancando)")
        return 1

    try:
        age = time.time() - os.path.getmtime(path)
    except OSError as exc:
        print(f"[healthcheck] no se pudo leer el latido: {exc}")
        return 1

    if age > MAX_AGE:
        print(f"[healthcheck] la cola no late desde hace {age:.0f}s")
        return 1

    print(f"[healthcheck] ok ({age:.0f}s desde el último latido)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
