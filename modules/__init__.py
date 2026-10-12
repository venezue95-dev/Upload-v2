"""
Paquete de módulos del bot toDus S3.

Arquitectura rápida
-------------------
    handlers/   → interfaz con Telegram (solo validan y encolan)
    queue.py    → planificador: cupos, prioridades, justicia, ETA, persistencia
    pipeline.py → trabajo real: descarga → trocea → sube → finaliza
    s3_client.py→ adaptador de la librería todus + reintentos y cancelación
    downloader.py → descargas de Telegram y de URLs (por rangos o completas)
    notifier.py → actualización de los mensajes de progreso (un solo bucle)
    storage.py  → historial y estadísticas en SQLite
    runtime_settings.py → ajustes en caliente (/settings)
    texts.py / progress.py → toda la presentación
"""

__all__ = [
    "downloader",
    "handlers",
    "limiter",
    "notifier",
    "pipeline",
    "progress",
    "queue",
    "runtime_settings",
    "s3_client",
    "storage",
    "texts",
]
