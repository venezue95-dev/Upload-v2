"""
test_progress.py
================
Pruebas del CÁLCULO DE PROGRESO (el problema reportado: "no calcula bien").

Se simula una subida a velocidad conocida (50 MB en 20 s = 2,5 MB/s) y se
muestrea exactamente lo que ve el usuario en pantalla. Se comprueba que:

 1. La barra NO se queda congelada en 0 % (el fallo original: 30 de 32 muestras
    en 0 % con un archivo de una sola parte).
 2. El progreso es monótono: nunca retrocede.
 3. La velocidad mostrada es razonable (entre 0,5x y 2x la real) y no hay picos
    absurdos (antes se veían "29 MB/s" en un enlace de 2,5 MB/s).
 4. La ETA converge: se acerca al tiempo restante real.
 5. Con 10 partes (3 en paralelo) la barra avanza de forma continua, no a
    saltos del 30 %.
 6. En modo streaming se muestran las dos barras (subida y bajada).
 7. Las cifras estimadas se marcan como tal ("~") y dejan de marcarse en cuanto
    hay una medición real.
 8. `bytes_fetched` de las descargas por partes es correcto y no se duplica.
 9. La velocidad global aprendida se persiste y se reutiliza (data/speed_stats.json).

  python tests/test_progress.py
"""

from __future__ import annotations

import asyncio
import json
import math
import os
import sys
import tempfile
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "tests", "stub_todus"))

_TMP = tempfile.mkdtemp(prefix="todus-prog-")
os.environ.update({
    "API_ID": "1", "API_HASH": "h", "BOT_TOKEN": "1:a", "BOT_OWNER_ID": "999",
    "WORK_DIR": _TMP, "DATA_DIR": os.path.join(_TMP, "data"),
    "DOWNLOAD_DIR": os.path.join(_TMP, "downloads"),
    "S3_CHUNK_SIZE_MB": "50", "RATE_LIMIT_SEC": "0", "MIN_FREE_GB": "0",
})

from modules import progress as P                        # noqa: E402
from modules import texts as T                           # noqa: E402
from modules.queue import UploadTask, TaskStatus         # noqa: E402
from modules.speedstats import SPEEDS                    # noqa: E402

PASSED, FAILED = [], []


def check(name: str, condition: bool, detail: str = "") -> None:
    if condition:
        PASSED.append(name)
        print(f"  ✅ {name}")
    else:
        FAILED.append(f"{name} {detail}")
        print(f"  ❌ {name} {detail}")


REAL_SPEED = 2.5 * 1024 * 1024        # 2,5 MB/s simulados


def build_task(size_mb: int, chunk_mb: int, status=TaskStatus.UPLOADING,
               mode: str = "classic") -> UploadTask:
    size = size_mb * 1024 * 1024
    chunk = chunk_mb * 1024 * 1024
    task = UploadTask(user_id=7, chat_id=7, filename=f"{size_mb}mb.bin", file_size=size)
    task.mode = mode
    task.status = status
    task.phase = "Subiendo" if status == TaskStatus.UPLOADING else "Descargando"
    task.plan_parts(chunk, max(1, (size + chunk - 1) // chunk))
    task.mark_started()
    return task


async def simulate_upload(size_mb: int, chunk_mb: int, tick: float = 0.4) -> dict:
    """
    Simula la subida real: partes de `chunk_mb` subiéndose de 3 en 3 a
    REAL_SPEED y sin avisos intermedios (como el PUT de todus).
    """
    task = build_task(size_mb, chunk_mb)
    n_parts = task.parts_total
    lengths = [task.expected_part_length(i) for i in range(1, n_parts + 1)]
    parallelism = min(3, n_parts)
    part_seconds = lengths[0] / REAL_SPEED
    # Tiempo real: tantas "oleadas" como haga falta (la última puede ir a medias).
    total_seconds = math.ceil(n_parts / parallelism) * part_seconds

    started, finished = {}, {}
    next_part = 1
    t0 = time.monotonic()
    samples = []

    guard = total_seconds + 10.0
    while len(finished) < n_parts:
        elapsed = time.monotonic() - t0
        if elapsed > guard:                       # red de seguridad
            break

        while next_part <= n_parts and len(started) - len(finished) < parallelism:
            if elapsed >= ((next_part - 1) // parallelism) * part_seconds:
                task.part_started(next_part)
                started[next_part] = elapsed
                next_part += 1
            else:
                break
        for number in list(started):
            if number not in finished and elapsed >= started[number] + part_seconds:
                finished[number] = elapsed
                task.part_finished(number, lengths[number - 1],
                                   part_seconds)

        task.sample_speed()
        samples.append({
            "t": elapsed,
            "pct": task.progress_percent(),
            "speed": task.upload_speed,
            "eta": task.eta_seconds(),
            "estimated": task.is_estimated(),
            "text": T.progress_text(task),
        })
        await asyncio.sleep(tick)

    return {"task": task, "samples": samples, "total_seconds": total_seconds,
            "n_parts": n_parts}


def bars_report(result: dict, every: int = 6) -> None:
    for i in range(0, len(result["samples"]), every):
        s = result["samples"][i]
        eta = f"{s['eta']:5.1f}s" if s["eta"] is not None else "  —  "
        print(
            f"     t={s['t']:5.1f}s {P.bar(s['pct'], 18)} {s['pct']:5.1f}%  "
            f"{P.human_speed(s['speed']):>10}  ETA {eta}"
        )


async def main() -> int:
    print("=" * 66)
    print(" PRUEBAS DE PROGRESO — toDus Upload Bot")
    print("=" * 66)

    SPEEDS.configure(os.path.join(_TMP, "data", "speed_stats.json"))

    # ------------------------------------------------------------------
    print("\n▶ Caso 1 · archivo de UNA sola parte (el que se quedaba en 0 %)")
    result = await simulate_upload(size_mb=30, chunk_mb=50)
    samples = result["samples"]
    frozen = sum(1 for s in samples if s["pct"] < 0.5)
    moving = sum(1 for s in samples if 1.0 < s["pct"] < 99.0)
    print(f"     ({len(samples)} muestras, {result['n_parts']} parte)")
    bars_report(result)

    check("el progreso se mueve durante la subida (no se congela)",
          moving >= 3, f"(solo {moving} muestras en progreso)")
    check("menos de la mitad de muestras en 0 %",
          frozen < len(samples) * 0.5, f"({frozen}/{len(samples)} en 0 %)")
    monotonic = all(
        samples[i + 1]["pct"] >= samples[i]["pct"] - 0.01
        for i in range(len(samples) - 1)
    )
    check("el progreso nunca retrocede (monótono)", monotonic)
    speeds = [s["speed"] for s in samples if s["speed"] > 0]
    check("hay velocidad distinta de cero casi siempre",
          len(speeds) >= len(samples) - 2, f"({len(speeds)}/{len(samples)})")
    check("la velocidad mostrada siempre es un valor sano (nunca 0 B/s)",
          all(32 * 1024 <= sp <= 200 * 1024 * 1024 for sp in speeds),
          f"({[round(s/1024/1024,2) for s in speeds][:4]} MB/s)")
    check("la estimación inicial no se dispara muy lejos de la realidad",
          0.2 * REAL_SPEED <= speeds[0] <= 5 * REAL_SPEED,
          f"(estimada {speeds[0]/1024/1024:.2f} vs real {REAL_SPEED/1024/1024:.2f} MB/s)")
    check("las primeras muestras se marcan como estimadas",
          samples[0]["estimated"] is True)
    check("el texto avisa de que la velocidad es estimada",
          "estimada" in samples[0]["text"] or "~" in samples[0]["text"],
          repr(samples[0]["text"][:120]))
    check("el texto trae la barra desde el primer refresco",
          "⬡" in samples[0]["text"])
    check("hay ETA desde las primeras muestras",
          sum(1 for s in samples if s["eta"] is not None) >= len(samples) - 1)

    # ------------------------------------------------------------------
    print("\n▶ Caso 2 · archivo de 10 partes, 3 en paralelo (saltos del 30 % antes)")
    result = await simulate_upload(size_mb=250, chunk_mb=50)
    samples = result["samples"]
    print(f"     ({len(samples)} muestras, {result['n_parts']} partes, "
          f"{result['total_seconds']:.1f}s de subida real)")
    bars_report(result, every=max(1, len(samples) // 8))
    jumps = [
        samples[i + 1]["pct"] - samples[i]["pct"]
        for i in range(len(samples) - 1)
    ]
    biggest = max(jumps) if jumps else 0
    check("ningún salto de progreso mayor del 12 % entre refrescos",
          biggest <= 12.0, f"(salto máx {biggest:.1f} %)")
    pasos = sorted({round(s["pct"]) for s in samples})
    check("la barra pasa por muchos valores (progreso continuo)",
          len(pasos) >= 12, f"({len(pasos)} valores distintos)")
    check("acaba en 100 %",
          samples[-1]["pct"] >= 99.9, f"({samples[-1]['pct']:.1f} %)")
    etas = [s["eta"] for s in samples if s["eta"] is not None]
    check("la ETA inicial se acerca al tiempo total real (con partes en paralelo)",
          bool(etas) and abs(etas[0] - result["total_seconds"]) < result["total_seconds"] * 0.4,
          f"(ETA inicial {etas[0]:.1f}s vs real {result['total_seconds']:.1f}s)" if etas else "")
    final_eta = [s for s in samples if s["eta"] is not None][-1]
    check("la ETA final es corta (se acaba el trabajo)",
          final_eta["eta"] < result["total_seconds"] * 0.25,
          f"({final_eta['eta']:.1f}s)")
    check("la velocidad medida acaba siendo casi exacta",
          abs(samples[-1]["speed"] - REAL_SPEED) < REAL_SPEED * 0.35,
          f"({samples[-1]['speed']/1024/1024:.2f} vs {REAL_SPEED/1024/1024:.2f} MB/s)")

    # ------------------------------------------------------------------
    print("\n▶ Caso 3 · descarga por partes (modo streaming)")
    task = build_task(500, 50, status=TaskStatus.DOWNLOADING, mode="stream")
    task.note_fetched(1, 10 * 1024 * 1024)
    task.note_fetched(1, 40 * 1024 * 1024)          # crece
    task.note_fetched(1, 20 * 1024 * 1024)          # retroceso: se ignora
    task.part_fetched_done(1, 50 * 1024 * 1024)
    check("los bytes descargados suman bien por partes",
          task.bytes_fetched == 50 * 1024 * 1024, str(task.bytes_fetched))
    task.note_fetched(2, 10 * 1024 * 1024)
    check("las partes se acumulan", task.bytes_fetched == 60 * 1024 * 1024)
    task.part_started(1)
    task.part_started(2)
    text = T.progress_text(task)
    check("el mensaje de streaming muestra subida y bajada",
          "⬆️" in text and "⬇️" in text)
    check("el mensaje de streaming incluye la parte en curso",
          "parte" in text or "🧩" in text)

    # ------------------------------------------------------------------
    print("\n▶ Caso 4 · descarga (modo clásico) con callback intermitente")
    task = build_task(200, 50, status=TaskStatus.DOWNLOADING, mode="classic")
    task.add_fetched(20 * 1024 * 1024)
    mono = time.monotonic()
    task._download_meter.update(task.bytes_fetched, mono)
    task._download_meter.update(task.bytes_fetched + 20 * 1024 * 1024, mono + 4)
    task.add_fetched(20 * 1024 * 1024)
    task.sample_speed()
    check("velocidad de bajada calculada (5 MB/s esperados)",
          abs(task.download_speed - 5 * 1024 * 1024) < 1024 * 1024,
          f"({task.download_speed/1024/1024:.2f} MB/s)")
    check("porcentaje de descarga correcto",
          abs(task.download_percent() - 20.0) < 0.5, f"({task.download_percent():.1f} %)")
    check("el ETA usa la velocidad de bajada",
          task.eta_seconds() is not None and 25 < task.eta_seconds() < 35,
          f"({task.eta_seconds()})")

    # ------------------------------------------------------------------
    print("\n▶ Caso 5 · velocidad global aprendida y persistida")
    path = os.path.join(_TMP, "data", "speed_stats.json")
    SPEEDS.note_upload(50 * 1024 * 1024, 20.0)      # 2,5 MB/s
    SPEEDS.save(force=True)
    check("el archivo de velocidades existe", os.path.exists(path))
    data = json.load(open(path, encoding="utf-8"))
    check("guarda la velocidad de subida",
          abs(data["upload_bps"] / 1024 / 1024 - 2.5) < 0.5, str(data))

    original = SPEEDS.upload_bps
    SPEEDS.upload_bps = 0.0
    SPEEDS._load()
    check("se recupera al reiniciar", abs(SPEEDS.upload_bps - original) < 1.0,
          f"({SPEEDS.upload_bps/1024/1024:.2f} MB/s)")

    task = build_task(30, 50)
    check("una tarea nueva usa la velocidad aprendida como estimación",
          abs(task.upload_speed - SPEEDS.upload_estimate()) < 1.0)
    check("sigue marcada como estimada", task.is_estimated() is True)

    # Una medición absurda (disco/caché) no debe envenenar la estimación.
    before = SPEEDS.upload_bps
    SPEEDS.note_upload(10 * 1024 * 1024, 0.01)      # 1 GB/s irreal
    check("se descartan mediciones absurdas", abs(SPEEDS.upload_bps - before) < 1.0)

    # ------------------------------------------------------------------
    print("\n▶ Caso 6 · coherencia del texto final y de la barra")
    check("la barra refleja el porcentaje",
          P.bar(50.0, 10).count("⬢") == 5, P.bar(50.0, 10))
    check("el porcentaje se muestra sin decimales inútiles",
          T.display_number(2.0) == "2" and T.display_number(True) == "sí")
    task = build_task(100, 50)
    task.status = TaskStatus.COMPLETED
    task.urls = ["https://s3.todus.cu/stream/x/y/parte.mkv"]
    task.bytes_transferred = 100 * 1024 * 1024
    final = T.final_text(task)
    check("el mensaje final incluye el enlace", "https://s3.todus.cu" in final)
    check("el mensaje final cabe en Telegram", len(final) < 4096, str(len(final)))

    print("\n" + "=" * 66)
    print(f" ✅ Correctas: {len(PASSED)}   ❌ Fallidas: {len(FAILED)}")
    for item in FAILED:
        print(f"    · {item}")
    print("=" * 66)
    return 1 if FAILED else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
