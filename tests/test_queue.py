"""
test_queue.py
=============
Pruebas de la cola SIN Telegram ni toDus reales:

  python tests/test_queue.py          (no necesita pytest)

Comprueba:
1. Admisión: cupo por usuario, cola global y cola pausada.
2. Planificador: nunca supera max_concurrent_tasks ni max_active_per_user.
3. Justicia: un usuario con 10 archivos no bloquea a otro con 1.
4. Cancelación (de una tarea y de todas).
5. Posiciones y ETA de la cola.
6. Persistencia: guardar y recuperar tareas tras un "reinicio".
7. Historial SQLite: se registran las tareas completadas.
8. Troceo: plan_parts genera nombres/keys correctos.

Los tests usan un ejecutor falso (no descarga ni sube nada de verdad).
"""

from __future__ import annotations

import asyncio
import os
import shutil
import sys
import tempfile
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "tests", "stub_todus"))

# Entorno de prueba (se fija ANTES de importar config).
_TMP = tempfile.mkdtemp(prefix="todus-test-")
os.environ.update({
    "API_ID": "12345",
    "API_HASH": "test-hash",
    "BOT_TOKEN": "123:test",
    "BOT_OWNER_ID": "999",
    "DOWNLOAD_DIR": os.path.join(_TMP, "downloads"),
    "WORK_DIR": _TMP,
    "DATA_DIR": os.path.join(_TMP, "data"),
    "S3_MAX_RETRIES": "3",
    "QUEUE_MAX_CONCURRENT": "3",
    "QUEUE_MAX_PER_USER": "1",
    "QUEUE_MAX_TOTAL": "20",
    "QUEUE_MAX_WAITING_PER_USER": "5",
    "RATE_LIMIT_SEC": "0",
    "PROGRESS_INTERVAL_SEC": "2",
})

from config import CONFIG                                  # noqa: E402
from modules import storage                                # noqa: E402
from modules.queue import QUEUE, TaskStatus, UploadTask     # noqa: E402
from modules.runtime_settings import RUNTIME_SETTINGS       # noqa: E402
from modules.s3_client import plan_parts, safe_filename     # noqa: E402

PASSED = []
FAILED = []


def check(name: str, condition: bool, detail: str = "") -> None:
    if condition:
        PASSED.append(name)
        print(f"  ✅ {name}")
    else:
        FAILED.append(f"{name} {detail}")
        print(f"  ❌ {name} {detail}")


def make_task(user_id: int, name: str = "video.mp4", size: int = 5_000_000,
              source: str = "telegram", url=None) -> UploadTask:
    return UploadTask(
        user_id=user_id, chat_id=user_id, message_id=1,
        filename=name, file_size=size, source=source, url=url,
        username=f"user{user_id}", max_attempts=1,
    )


async def fake_runner(client, task: UploadTask) -> None:
    """Ejecutor simulado: respeta la cancelación y 'trabaja' un ratito."""
    task.phase = "Simulando"
    for _ in range(20):
        if task.is_cancelled:
            task.mark_finished(TaskStatus.CANCELLED)
            return
        await asyncio.sleep(0.02)
    task.keys = [f"users/tg_{task.user_id}/telegram/{task.task_id}"]
    task.urls = [f"https://s3.todus.cu/stream/{task.keys[0]}"]
    task.bytes_transferred = task.file_size
    task.mark_finished(TaskStatus.COMPLETED)


async def wait_until(cond, timeout: float = 6.0) -> bool:
    end = time.time() + timeout
    while time.time() < end:
        if cond():
            return True
        await asyncio.sleep(0.02)
    return cond()


async def test_admission() -> None:
    print("\n▶ Admisión y cupos")
    QUEUE.paused = False
    ok, _ = QUEUE.submit(make_task(101))
    check("acepta una tarea normal", ok)

    for i in range(5):
        QUEUE.submit(make_task(101, name=f"f{i}.mp4"))
    ok, reason = QUEUE.submit(make_task(101, name="extra.mp4"))
    check("rechaza al superar el cupo del usuario", not ok, reason[:60])

    QUEUE.paused = True
    ok, reason = QUEUE.submit(make_task(102))
    check("rechaza si la cola está pausada", not ok and "pausada" in reason)
    QUEUE.paused = False

    original = RUNTIME_SETTINGS.max_queue_total
    RUNTIME_SETTINGS.set_value("max_queue_total", 3)
    for i in range(10):
        QUEUE.submit(make_task(200 + i))
    ok, reason = QUEUE.submit(make_task(999))
    check("rechaza si el sistema está saturado", not ok and "saturado" in reason)
    RUNTIME_SETTINGS.set_value("max_queue_total", original)

    QUEUE.cancel_all()
    await wait_until(lambda: not QUEUE.counts()["active"], timeout=3)
    QUEUE.cleanup(max_age_seconds=0)


async def test_scheduler_limits() -> None:
    print("\n▶ Límites del planificador")
    RUNTIME_SETTINGS.set_value("max_concurrent_tasks", 3)
    RUNTIME_SETTINGS.set_value("max_active_per_user", 1)

    for i in range(12):
        QUEUE.submit(make_task(301 + (i % 4), name=f"p{i}.mp4"))

    peak_active = 0
    peak_per_user = 0
    for _ in range(60):
        await asyncio.sleep(0.05)
        counts = QUEUE.counts()
        peak_active = max(peak_active, counts["active"])
        per_user = max(QUEUE._user_active.values(), default=0)
        peak_per_user = max(peak_per_user, per_user)

    check("no supera max_concurrent_tasks", peak_active <= 3, f"(pico={peak_active})")
    check("no supera 1 activa por usuario", peak_per_user <= 1, f"(pico={peak_per_user})")
    check("encoló las 12 tareas", len(QUEUE.all_tasks(limit=50)) >= 12)

    QUEUE.cancel_all()
    await wait_until(lambda: not QUEUE.counts()["active"], timeout=3)
    QUEUE.cleanup(max_age_seconds=0)


async def test_fairness() -> None:
    print("\n▶ Reparto justo entre usuarios")
    QUEUE.cleanup(max_age_seconds=0)
    RUNTIME_SETTINGS.set_value("max_concurrent_tasks", 2)
    RUNTIME_SETTINGS.set_value("max_active_per_user", 1)

    for i in range(10):
        QUEUE.submit(make_task(401, name=f"heavy{i}.mp4"))   # usuario pesado
    single = None
    await asyncio.sleep(0.05)
    single = make_task(402, name="uno.mp4")
    QUEUE.submit(single)

    await asyncio.sleep(0.4)
    started = [
        t.filename for t in QUEUE.all_tasks(limit=50)
        if t.status in (TaskStatus.DOWNLOADING, TaskStatus.UPLOADING)
    ]
    check("el usuario con una tarea no queda detrás de 10", "uno.mp4" in started,
          f"(activas={started})")

    QUEUE.cancel_all()
    await wait_until(lambda: not QUEUE.counts()["active"], timeout=3)
    QUEUE.cleanup(max_age_seconds=0)


async def test_cancel() -> None:
    print("\n▶ Cancelación")
    task = make_task(501)
    QUEUE.submit(task)
    await wait_until(lambda: task.status.is_active, timeout=3)
    check("la tarea arrancó", task.status.is_active, task.status.value)

    QUEUE.cancel(task.task_id)
    check("cancel marca la bandera", task.is_cancelled)
    finished = await wait_until(lambda: task.status == TaskStatus.CANCELLED, timeout=3)
    check("termina como cancelada", finished, task.status.value)

    for i in range(4):
        QUEUE.submit(make_task(502, name=f"c{i}.mp4"))
    await asyncio.sleep(0.2)
    n = QUEUE.cancel_user(502)
    check("cancel_user cancela varias", n >= 1, f"(n={n})")
    await wait_until(lambda: not QUEUE.counts()["active"], timeout=3)
    QUEUE.cleanup(max_age_seconds=0)


async def test_positions_and_eta() -> None:
    print("\n▶ Posiciones y ETA")
    RUNTIME_SETTINGS.set_value("max_concurrent_tasks", 1)
    RUNTIME_SETTINGS.set_value("max_active_per_user", 1)
    QUEUE.cleanup(max_age_seconds=0)

    tasks = [make_task(601 + i, name=f"q{i}.mp4", size=10_000_000) for i in range(4)]
    for task in tasks:
        QUEUE.submit(task)
    await asyncio.sleep(0.3)

    waiting = [t for t in tasks if t.status == TaskStatus.QUEUED]
    check("hay tareas en espera", len(waiting) >= 2)
    positions = sorted(t.queue_position for t in waiting)
    check("las posiciones son 1..n", positions == list(range(1, len(waiting) + 1)), str(positions))
    check("hay ETA calculada", all(t.queue_eta is not None for t in waiting))
    check("ETA crece con la posición",
          waiting[0].queue_eta <= waiting[-1].queue_eta + 1e-6)

    QUEUE.cancel_all()
    await wait_until(lambda: not QUEUE.counts()["active"], timeout=3)
    QUEUE.cleanup(max_age_seconds=0)


async def test_persistence_and_history() -> None:
    print("\n▶ Persistencia e historial")
    QUEUE.cleanup(max_age_seconds=0)
    task = make_task(701, name="guardado.bin")
    QUEUE.submit(task)
    await wait_until(lambda: task.status.is_active, timeout=3)
    QUEUE._save_state(force=True)

    saved_path = CONFIG.state_file
    check("se escribió el archivo de estado", os.path.exists(saved_path))

    recovered = QUEUE.load_state()
    check("se recupera la tarea pendiente",
          any(t.filename == "guardado.bin" for t in recovered),
          f"({[t.filename for t in recovered]})")

    QUEUE.cancel_all()
    await wait_until(lambda: not QUEUE.counts()["active"], timeout=3)

    # Historial: completamos una tarea y comprobamos SQLite.
    done = make_task(702, name="historial.bin")
    QUEUE.submit(done)
    ok = await wait_until(lambda: done.status == TaskStatus.COMPLETED, timeout=5)
    check("la tarea se completa", ok, done.status.value)
    await asyncio.sleep(0.2)

    rows = storage.user_history(702, limit=5)
    check("aparece en el historial", any(r["filename"] == "historial.bin" for r in rows))
    stats = storage.global_stats()
    check("las estadísticas cuentan la subida", stats["completed"] >= 1, str(stats))
    QUEUE.cleanup(max_age_seconds=0)


def test_plan_parts() -> None:
    print("\n▶ Troceo de archivos")
    base, parts = plan_parts(55, "pelicula.mkv", 800 * 1024 * 1024, 100 * 1024 * 1024)
    check("genera 8 partes de 100 MB", len(parts) == 8, f"({len(parts)})")
    check("nombres con part", parts[0].part_name.startswith("pelicula.part001"),
          parts[0].part_name)
    check("keys ordenadas y únicas", len({p.key for p in parts}) == 8)
    check("la última parte ajusta el tamaño",
          parts[-1].length == 800 * 1024 * 1024 - 7 * 100 * 1024 * 1024,
          str(parts[-1].length))

    _, single = plan_parts(55, "foto.png", 3 * 1024 * 1024, 100 * 1024 * 1024)
    check("un solo trozo conserva el nombre", single[0].part_name == "foto.png",
          single[0].part_name)

    cleaned = safe_filename("../../etc/passwd\r\n")
    check("sanea nombres peligrosos (sin rutas ni saltos)",
          "/" not in cleaned and "\n" not in cleaned and cleaned == "passwd_",
          f"({cleaned!r})")


def test_runtime_settings() -> None:
    print("\n▶ Ajustes en caliente")
    original = RUNTIME_SETTINGS.max_concurrent_tasks
    nxt = RUNTIME_SETTINGS.cycle("max_concurrent_tasks")
    check("cycle cambia el valor", nxt != original, f"{original}->{nxt}")
    check("el cambio es visible al instante", RUNTIME_SETTINGS.max_concurrent_tasks == nxt)
    check("persiste en disco", os.path.exists(CONFIG.settings_file))
    RUNTIME_SETTINGS.set_value("max_concurrent_tasks", original)
    RUNTIME_SETTINGS.set_value("max_queue_total", 200)


async def test_shutdown_resume() -> None:
    """Simula el SIGTERM de Railway: lo que estaba en curso debe reanudarse."""
    print("\n▶ Apagado del bot (SIGTERM) y reanudación")
    QUEUE.cleanup(max_age_seconds=0)

    async def slow_runner(client, task: UploadTask) -> None:
        for _ in range(400):
            if task.is_cancelled:
                return
            await asyncio.sleep(0.05)

    QUEUE.attach(client=None, runner=slow_runner)
    task = make_task(801, name="interrumpida.bin", size=100_000_000)
    QUEUE.submit(task)
    await wait_until(lambda: task.status.is_active, timeout=3)
    check("la tarea está en curso antes del apagado", task.status.is_active, task.status.value)

    await QUEUE.stop(cancel_running=True)

    import json

    with open(CONFIG.state_file, encoding="utf-8") as fh:
        data = json.load(fh)
    names = [t["filename"] for t in data.get("tasks", [])]
    check("la tarea a medias se guarda para reanudar", "interrumpida.bin" in names, str(names))

    recovered = QUEUE.load_state()
    check("se recupera con su origen", any(t.filename == "interrumpida.bin" for t in recovered))
    check("se devuelve el cupo del usuario", QUEUE.user_load(801) == (0, 0))


async def main() -> int:
    print("=" * 62)
    print(" PRUEBAS DE LA COLA — toDus S3 Uploader Bot")
    print("=" * 62)

    storage.init(CONFIG)
    QUEUE.attach(client=None, runner=fake_runner)
    await QUEUE.start()

    test_plan_parts()
    test_runtime_settings()
    await test_admission()
    await test_scheduler_limits()
    await test_fairness()
    await test_cancel()
    await test_positions_and_eta()
    await test_persistence_and_history()
    await test_shutdown_resume()

    await QUEUE.stop(cancel_running=True)

    print("\n" + "=" * 62)
    print(f" ✅ Correctas: {len(PASSED)}   ❌ Fallidas: {len(FAILED)}")
    for item in FAILED:
        print(f"    · {item}")
    print("=" * 62)

    shutil.rmtree(_TMP, ignore_errors=True)
    return 1 if FAILED else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
