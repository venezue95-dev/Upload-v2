# 🛠 Cambios de la v2 — qué había, qué cambié y por qué

Resumen honesto del trabajo sobre tu proyecto **Uplo** (bot toDus S3). Al final
está la parte que más te interesa: **cómo se aguanta con 20 / 30 / 40 usuarios**.

---

## 0. ⚠️ Dos cosas urgentes antes de nada

1. **El `.env` con tus credenciales reales está publicado en GitHub**
   (`API_ID`, `API_HASH`, `BOT_TOKEN`). Cualquiera puede controlar tu bot.
   → [@BotFather](https://t.me/BotFather) → `/revoke` y actualiza el token.
   Ya incluí un `.gitignore` para que no vuelva a pasar.
2. **El bot no arrancaba con la librería actual.** Tu `modules/s3_client.py`
   hacía `from todus import NamespaceManager`, pero la versión actual de
   `todus-client` (v3) **ya no exporta `NamespaceManager`**: expone
   `S3Client` con `put() / head() / delete() / build_url()`. Eso significaba
   `ImportError` al iniciar. **Arreglado** con un adaptador que detecta la
   versión instalada y funciona con las dos APIs.

---

## 1. 🚦 La cola: lo que estaba mal y cómo queda ahora

### Cómo estaba

```python
# handlers/upload.py (antes)
user_lock = QUEUE.get_user_lock(user_id)
async with user_lock:                      # ¡el handler se queda aquí!
    await _process_upload(...)             # descarga + sube TODO
```

- Existía un "QueueManager", pero **no planificaba nada**: solo guardaba tareas
  y un `asyncio.Lock` por usuario. El trabajo real ocurría **dentro del handler**.
- Pyrogram procesa los updates con un número limitado de *workers*. Con 4-5
  archivos subiendo en paralelo, **los workers se agotaban**: `/cola`, los
  botones de cancelar y cualquier comando dejaban de responder hasta que
  terminaba una subida. Ese es el motivo por el que "se sentía" congelado.
- Sin límite global: N usuarios × 3 partes = decenas de PUT a toDus a la vez.
- Sin control de admisión: cualquiera podía encolar 200 tareas.
- Sin posiciones ni ETA: el usuario solo veía "⏳ encolada detrás".
- Un bucle de progreso **por tarea** editando Telegram cada 2,5 s: con 40 tareas
  eso son 16 ediciones/s → `FLOOD_WAIT` garantizado.
- Si el bot se reiniciaba, **todo lo encolado desaparecía**.
- El progreso de subida iba por "partes completadas" sin crédito parcial, y la
  velocidad se medía desde el inicio (contaba también el tiempo en cola → ETA falsa).

### Cómo queda

| Antes | Ahora |
|---|---|
| El handler hace el trabajo | El handler **solo encola** (responde al instante) |
| Sin planificador | **Planificador único**: `QUEUE_MAX_CONCURRENT` tareas activas |
| Un archivo por usuario, sin cupos | **Cupos**: activas por usuario, en espera por usuario y globales + anti-spam |
| Cola FIFO simple | **Reparto justo + prioridad + anti-inanición** (nadie se queda abajo) |
| Sin límite global de subidas | **Semáforo dinámico** `S3_GLOBAL_WORKERS` (ajustable en caliente) |
| Sin posiciones | `/cola` con **#posición, ETA de arranque, %, velocidad y ETA de fin** |
| Un bucle de progreso por tarea | **Un único bucle** que refresca todas (con límite de ediciones) |
| Estado solo en memoria | **Persistencia** (`data/queue_state.json`) + **reanudación** al arrancar |
| Cancelar = esperar | Cancelación **cooperativa e inmediata** (botón, comando, apagado) |
| Reintentos por tarea inexistentes | **Reintentos automáticos** al empezar + botón/`/reintentar` manual |
| Velocidad/ETA imprecisas | **SpeedMeter** con media exponencial + crédito de partes en vuelo |

### Además: trabajar menos y más rápido (pipeline)

- **Modo streaming**: descarga por rangos y **sube mientras descarga**. Antes:
  descargar 800 MB (esperar) y luego subir 800 MB. Ahora los dos procesos se
  solapan, el disco usado pasa de ~1,6 GB a ~3 trozos, y el tiempo total baja
  mucho. Se activa solo cuando el origen lo permite (`STREAM_MODE=auto`) y cae
  a modo clásico si no.
- **Trocear sin cargar en RAM**: los trozos se copian por bloques de 1 MB
  (antes se hacía `read()` de todo el trozo).
- **Espera cancelable en los reintentos**: antes un `time.sleep(30)` bloqueaba
  el hilo e ignoraba la cancelación.
- **`slice_to_file` + claves por número de parte**: las subidas terminan
  desordenadas y las URLs se generan en orden correcto (`part001`, `part002`…).
- **Los objetos van a una carpeta única por subida**:
  `users/tg_<id>/telegram/20261012-124207-8b309d/pelicula.part002.mkv` →
  enlaces legibles y sin mezclar usuarios.
- **Guardia de disco**: no se descarga si el espacio libre cae por debajo de
  `MIN_FREE_GB` (evita llenar el servidor y romper todo).
- **SSRF**: se rechazan enlaces a IPs privadas por defecto.
- **`max_concurrent_transmissions=4`** en Pyrogram: por defecto es **1**, es
  decir, *una sola descarga de Telegram a la vez* en todo el bot. Era otro
  cuello de botella silencioso para 40 usuarios.

---

## 2. 🎨 Interfaz y funciones nuevas

### Mensajes rediseñados

Barra de progreso, velocidad, ETA y detalle en un formato consistente:

```
📤 Descargando y subiendo
pelicula.mkv
▰▰▰▰▰▰▱▱▱▱▱▱▱▱ 43.2%
📦 172.5 MB / 400.0 MB
⚡️ 3.42 MB/s   ⏳ 01:12
🧩 parte 4/8 · ⬇️ 215.0 MB
```

Y mensajes finales con los enlaces en bloque copiable, aviso de partes y
botones de acción.

### Comandos y botones nuevos

- `/start` con **menú de botones** (cola, historial, estado, ayuda, admin).
- `/help` en 3 páginas.
- `/cola` paginada + **vista global** para el owner + acción por tarea.
- `/historial` (SQLite) con enlaces.
- `/cancelar` con confirmación y `/reintentar [id]`, `/enlaces [id]`.
- `/status`, `/ping` con información real de la cola.
- **Owner**: `/stats`, `/usuarios`, `/anuncio`, `/pausar`, `/reanudar`,
  `/sistema`, `/limpiar`.
- **Botones por tarea**: cancelar, detalle, reintentar, **enlaces .txt**,
  **borrar de toDus**, quitar de la lista.
- **Álbumes**: los grupos de archivos se detectan y se encolan juntos con un
  solo resumen.
- **Varios enlaces en un mensaje**: se encolan todos (hasta 10) con resumen.
- **Antiduplicados**: si repites la misma URL estando en cola, te lo dice.
- **Aviso al owner** cuando una tarea falla (máximo 1 por minuto).
- **Estadísticas persistentes** (SQLite): subidas, datos, partes, tiempos, top
  usuarios; base también para la ETA de la cola.

### Operación (nuevo)

- **Healthcheck real**: el planificador escribe un latido (`data/heartbeat`)
  cada 15 s y `healthcheck.py` verifica que esté fresco. Docker marca
  `unhealthy` si la cola se queda bloqueada (y no solo si el proceso muere).
  El healthcheck anterior usaba `pgrep`, que **no existe** en `python:3.12-slim`.
- **Apagado limpio**: `SIGTERM` (Docker) → se cancelan las tareas activas, se
  guarda el estado y al arrancar se **reanudan** avisando a cada usuario.

### `/settings` (12 ajustes en caliente, 3 páginas)

Cola (concurrencia global, por usuario, tamaños de cola), subida (chunk,
paralelismo por tarea, paralelismo global, reintentos, máximo por archivo),
avanzado (anti-spam, modo de trabajo, refresco del progreso). Se guardan en
`data/settings.json` y se aplican **al instante**; el botón ♻️ vuelve a los
valores del `.env`.

---

## 3. 🧱 Arquitectura nueva (por si tienes que tocar algo)

```
handlers/  → interfaz: validan, comprueban cupos, encolan y responden
queue.py   → planificador: admisión, justicia, prioridad, ETA, persistencia
pipeline.py→ trabajo: resolver origen → descargar → trocear → subir → enlaces
s3_client.py → adaptador todus (v3/v2) + reintentos + cancelación
downloader.py → Telegram por rangos / URLs por rangos / guardias de disco
notifier.py → un bucle que refresca el progreso de TODAS las tareas
storage.py → SQLite (historial, usuarios, estadísticas)
runtime_settings.py → ajustes en caliente
limiter.py → semáforo dinámico
texts.py / progress.py → toda la presentación
tests/ → 101 pruebas automáticas sin Telegram ni toDus
```

**Regla de oro**: si algo es lento (descargar, subir, trocear), va en el
pipeline/cola, nunca en el handler.

---

## 4. 🧮 Recomendación concreta para 20 / 30 / 40 usuarios

Ya lo dejé configurado en los valores por defecto (`.env.example` y `.env`
comentado), pero por si quieres ajustar:

```dotenv
MAX_FILE_MB=800                 # límite por archivo
S3_CHUNK_SIZE_MB=50             # 800 MB -> 16 partes de 50 MB
QUEUE_MAX_CONCURRENT=4          # 4 archivos "de verdad" a la vez en todo el bot
QUEUE_MAX_PER_USER=1            # cada persona, de una en una (equidad total)
QUEUE_MAX_TOTAL=200             # hasta 200 tareas en espera
QUEUE_MAX_WAITING_PER_USER=15   # corta el spam de uno solo
RATE_LIMIT_SEC=3                # anti-spam
S3_MAX_PARALLEL=3               # 3 trozos por tarea
S3_GLOBAL_WORKERS=6             # 6 subidas simultáneas en TODO el bot
TG_CONCURRENT_DOWNLOADS=4       # 4 descargas de Telegram a la vez
STREAM_MODE=auto                # descargar+subir solapados
MIN_FREE_GB=2                   # no llenar el disco
PROGRESS_INTERVAL_SEC=3         # refresco del progreso
```

**Cuentas rápidas para saber qué esperar**

- Subida efectiva ≈ nº de subidas simultáneas × velocidad por conexión. Si toDus
  te da ~2 MB/s por conexión: `6 × 2 = 12 MB/s` agregados ≈ 43 GB/hora.
- Con 40 usuarios enviando ~30 archivos de 100 MB (3 GB en total) a 12 MB/s:
  ≈ 4-5 minutos de cola total; cada usuario ve su `#posición` y su ETA.
- Disco en streaming: `S3_MAX_PARALLEL × chunk × 2 + margen` ≈ 300-400 MB por
  tarea activa (4 tareas ≈ 1,5 GB). En modo clásico: archivo completo por tarea.

**Si el servidor va justo**: baja `QUEUE_MAX_CONCURRENT` a 3 y
`S3_GLOBAL_WORKERS` a 4 (de 20-40 usuarios, 4-6 siguen atendidos). Todo se
cambia en caliente con `/settings`, sin reiniciar.

**Si quieres que vaya más rápido**: sube `S3_GLOBAL_WORKERS` a 8-12 y
`QUEUE_MAX_CONCURRENT` a 6, y pon `TgCrypto` (ya está en requirements) para
acelerar Telegram. Vigila el disco y el `FLOOD_WAIT` de Telegram.
