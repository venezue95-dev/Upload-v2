"""
bot.py
======
Punto de entrada del bot toDus S3 Uploader.

Orden de arranque:
1. Validar configuración (.env) e inicializar el historial SQLite.
2. Crear el cliente de Pyrogram con ``workers`` altos: los handlers ahora solo
   validan y encolan, así que el bot se mantiene responsivo aunque haya
   40 usuarios subiendo archivos.
3. Arrancar la cola (planificador + trabajadores) y el notificador de progreso.
4. Recuperar las tareas que quedaron pendientes del último apagado.
5. Registrar los handlers y quedarse escuchando.

Apagado (Ctrl+C / SIGTERM de Docker): se cancela la cola, se guarda el estado
y se avisa a los usuarios cuyas tareas quedan en pausa.
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys

# Aseguramos un event loop antes de importar pyrogram (comportamiento clásico).
try:
    asyncio.get_event_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())

from pyrogram import Client, idle
from pyrogram.enums import ParseMode

from config import CONFIG
from modules import keyboards as K
from modules import notifier, storage
from modules.handlers import admin, basic, callbacks, cola, settings, upload, url
from modules import texts as T
from modules.pipeline import cleanup_downloads_root, run_task
from modules.queue import QUEUE
from modules.speedstats import SPEEDS
from modules.runtime_settings import RUNTIME_SETTINGS
from modules.s3_client import shutdown_pool

log = logging.getLogger("s3_bot")


def setup_logging() -> None:
    logging.basicConfig(
        level=getattr(logging, CONFIG.log_level, logging.INFO),
        format="%(asctime)s | %(levelname)-7s | %(name)-16s | %(message)s",
        datefmt="%H:%M:%S",
    )
    logging.getLogger("pyrogram").setLevel(logging.WARNING)


def build_app() -> Client:
    return Client(
        CONFIG.telegram.session_name,
        api_id=CONFIG.telegram.api_id,
        api_hash=CONFIG.telegram.api_hash,
        bot_token=CONFIG.telegram.bot_token,
        workdir=CONFIG.work_dir,
        workers=CONFIG.telegram.workers,
        max_concurrent_transmissions=CONFIG.telegram.max_concurrent_transmissions,
        parse_mode=ParseMode.HTML,
    )


def register_handlers(app: Client) -> None:
    basic.register(app)
    admin.register(app)
    cola.register(app)
    settings.register(app)
    upload.register(app)
    url.register(app)
    callbacks.register(app)
    log.info("Handlers registrados (7 módulos)")


async def resume_pending_tasks(client: Client) -> None:
    """Reencola lo que quedó a medias en el apagado anterior."""
    tasks = QUEUE.load_state()
    if not tasks:
        return
    resumed = 0
    for task in tasks:
        ok, reason = QUEUE.submit(task, force=True)   # ya estaba admitida antes del apagado
        if not ok:
            log.warning("No se pudo reanudar %s: %s", task.task_id, reason)
            continue
        resumed += 1
        # Avisamos al usuario de que su tarea continúa.
        try:
            msg = await client.send_message(
                task.chat_id,
                f"♻️ <b>Tarea reanudada</b> tras el reinicio del bot\n"
                f"📄 {T.code(T.shorten(task.filename, 50))}\n"
                f"🆔 {T.code(task.task_id)}\n"
                f"{T.i('Sigue el progreso con /cola.')}",
                reply_markup=K.task_message_keyboard(task),
            )
            # Desde ahora el progreso se muestra en este mensaje (el antiguo puede
            # haberse borrado o quedado con datos viejos).
            task.status_message_id = msg.id
        except Exception:  # noqa: BLE001
            pass
    if resumed:
        log.info("↻ %d tarea(s) reencoladas", resumed)


async def main_async(app: Client) -> None:
    await app.start()
    # Los handlers se registran con el loop YA en marcha (Pyrogram los añade
    # mediante tareas asíncronas).
    register_handlers(app)
    me = await app.get_me()
    log.info("Bot en línea: @%s (id=%s)", me.username, me.id)

    # Cola: le damos el cliente y el ejecutor de tareas.
    QUEUE.attach(app, run_task)
    await QUEUE.start()
    await notifier.start(app)
    await resume_pending_tasks(app)

    log.info(
        "Listo | tareas simultáneas=%d | por usuario=%d | en espera máx=%d | "
        "partes paralelas=%d/%d",
        RUNTIME_SETTINGS.max_concurrent_tasks,
        RUNTIME_SETTINGS.max_active_per_user,
        RUNTIME_SETTINGS.max_queue_total,
        RUNTIME_SETTINGS.max_parallel,
        RUNTIME_SETTINGS.global_chunk_workers,
    )
    if CONFIG.allowed_users:
        log.info("Modo privado | %d usuario(s) autorizados", len(CONFIG.allowed_users))
    else:
        log.info("Modo público: cualquiera puede usar el bot")

    # idle() de Pyrogram captura SIGINT/SIGTERM y vuelve aquí: es el punto
    # perfecto para apagar la cola ordenadamente.
    try:
        await idle()
    finally:
        await shutdown(app)


async def shutdown(app: Client) -> None:
    """Cancela tareas, guarda el estado y libera recursos."""
    log.info("Deteniendo la cola…")
    pending = QUEUE.shutdown_running()
    await QUEUE.stop(cancel_running=True)
    await notifier.stop()
    try:
        await app.stop()
    except Exception:  # noqa: BLE001
        pass
    try:
        SPEEDS.save(force=True)
    except Exception:  # noqa: BLE001
        pass
    shutdown_pool()
    log.info(
        "Apagado completo | %d tarea(s) pendientes guardadas para reanudar",
        pending,
    )


def main() -> int:
    setup_logging()

    log.info("=== toDus S3 Uploader Bot ===")
    log.info(
        "Almacenamiento: toDus S3 (%s) | chunk=%d MB | reintentos=%d",
        CONFIG.s3.base_url or "endpoint por defecto de la librería",
        CONFIG.s3.chunk_size_mb,
        CONFIG.s3.max_retries,
    )

    # Historial y limpieza de restos de la ejecución anterior.
    try:
        storage.init(CONFIG)
    except Exception as exc:  # noqa: BLE001
        log.warning("Historial no disponible (%s): sigo sin estadísticas", exc)

    # Velocidades aprendidas (barra de progreso y ETA realistas desde el
    # primer segundo, incluso antes de medir nada en esta sesión).
    SPEEDS.configure(os.path.join(CONFIG.data_dir, "speed_stats.json"))
    try:
        avg_upload, avg_download = storage.avg_speeds()
        SPEEDS.seed_from_history(avg_upload, avg_download)
    except Exception as exc:  # noqa: BLE001
        log.debug("No se pudo sembrar velocidades: %s", exc)
    cleanup_downloads_root()

    # Comprobamos la librería todus al arrancar (falla rápido y claro).
    try:
        from modules.pipeline import s3

        ok, detail = s3().ping()
        log.info("Almacenamiento: %s %s", "✅" if ok else "⚠️", detail)
    except Exception as exc:  # noqa: BLE001
        log.error("No se pudo inicializar el cliente de almacenamiento: %s", exc)
        log.error(
            "Instala la librería con: pip install "
            "'todus-client @ git+https://github.com/nyxthor-dev/todus-client.git'"
        )
        return 2

    app = build_app()

    try:
        app.run(main_async(app))
    except KeyboardInterrupt:  # pragma: no cover
        log.info("Interrumpido por el usuario")
    except Exception as exc:  # noqa: BLE001
        log.exception("Error fatal: %s", exc)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
