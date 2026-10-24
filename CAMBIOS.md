# 🛠 Corrección de progresos (descarga y subida)

**Subida**
- El progreso ahora usa bytes **realmente enviados** (PUT por streaming con `ProgressReader`). Con bytes reales ya no se
  inventa «crédito» por tiempo: antes la barra saltaba y se quedaba congelada cuando llegaban los primeros bytes reales.
- Velocidad y ETA de subida se miden sobre el total en vivo (todas las partes en paralelo); ya no dependen de que
  termine una parte de 50 MB.
- **Content-Type**: se deducía del temporal `.tmp` (siempre `application/octet-stream`); ahora sale del nombre visible.
- Códigos 2xx (200/201/204) se aceptan como éxito; antes solo el 200.
- Cancelar corta la parte **a mitad** del envío (antes esperaba a que terminara el PUT).
- Cada reintento de una parte vuelve a contar desde 0 sin que la pantalla retroceda, y los avisos tardíos no suman dos veces.
- Si la librería `todus` no admite el PUT con progreso, se vuelve solo al `put()` normal (aviso en el log).
  Se puede desactivar con `S3_LIVE_PROGRESS=0`.

**Descarga**
- `SpeedMeter` ya no marca 0 B/s entre bloques (Telegram entrega 1 MiB a saltos): la velocidad y la ETA dejaban de
  parpadear a «calculando…». La velocidad solo baja a 0 tras una parada real.
- URLs sin `Content-Length` en el HEAD: el tamaño se aprende de la respuesta, así la barra/ETA funcionan.
- En streaming, las partes reanudadas cuentan como descargadas (la barra de bajada no se quedaba por debajo del 100 %).
- yt-dlp: se muestra «Procesando con FFmpeg» al unir/convertir (antes la barra quedaba en 100 % sin explicación).

**Pruebas nuevas**: `tests/test_live_upload.py` (servidor HTTP real: contenido íntegro, cabeceras, progreso repartido,
cancelación a mitad, errores HTTP, reintentos, velocidad).

---

# 🛠 Revisión completa (última versión)

Segunda pasada sobre **todo** el código (no solo los botones). Detalle:

**Botones** — reescritos con un único esquema (`keyboards.py` + `views.py` + `callbacks.py`).
Cada pulsación se responde una sola vez, hay control de acceso/baneo, y los mensajes
de tarea ya no se sobrescriben con paneles (peleaban con el refresco del progreso).
«Cancelar todas» del owner ya no vacía la cola de todo el bot; los botones de tareas
antiguas funcionan desde el historial.

**Bugs reales corregidos**
- Subir una foto fallaba (`message.photo.sizes` no existe); fotos y notas de vídeo no se podían descargar.
- Descargas por URL: una respuesta gzip truncaba el archivo; un servidor sin Range corrompía partes en streaming.
- SSRF: las redirecciones saltaban la validación; la resolución DNS bloqueaba el bucle.
- Handlers lentos (sondeo de URL, /anuncio, /status) bloqueaban los workers: ahora van en segundo plano.
- Reintentos: perdían claves, numeraban mal las partes con huecos, ignoraban un cambio de tamaño de parte; `/reintentar` podía resubir una tarea completada.
- Textos: listas largas dejaban `<pre>` sin cerrar y el mensaje final no llegaba.
- Borrado de toDus: si fallaba alguna parte se perdían todos los enlaces.
- `STREAM_MODE` on/off mal interpretado; `ping()` siempre decía OK; cancelado contaba como fallo; los baneos consultaban SQLite en cada mensaje.
- Faltaba `.env.example` (tu `.env` no se ha tocado).

El cálculo del progreso (`RateMeter`) no ha cambiado.

---

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
- **Los objetos van a una carpeta propia por subida**:
  `toDusDownload/<token7>/pelicula.part002.mkv`; la clave no incluye el ID de Telegram.
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


---

## 5. 🔎 Auditoría v1.3 de tu repo `Upload-v2`

Revisé **todo** el proyecto sobre tu copia (commits hasta `264c461`): cola,
pipeline, progreso, notificador, S3, storage, handlers, ajustes, Docker,
`healthcheck.py`, `.env`, `.gitignore`, documentación y seguridad. Esto es lo
que encontré y lo que hice.

### 5.1 📈 El progreso no calculaba bien: qué pasaba exactamente

Lo reproduje con una prueba de velocidad conocida antes de tocar nada. Los
números del fallo:

| Escenario | Qué veía el usuario |
|---|---|
| Archivo de 30 MB con `chunk=50 MB` (**1 sola parte**) | **0,0 % y 0 B/s durante los 15 s de subida** (30 de 32 refrescos en 0 %), y de golpe 100 % |
| Archivo de 500 MB (**3 partes en paralelo**) | Escalones **0 % → 30 % → 60 % → 100 %**, con **0 B/s en 165 de 168 refrescos** |

**Por qué pasaba.** La velocidad se calculaba como *"bytes confirmados ÷ tiempo
transcurrido"* (`sample_speed` + `_meter`). Pero el `PUT` de toDus **no informa
de bytes intermedios**: el bot solo sabe que una parte avanzó **cuando la parte
termina**. Entonces:

- Con 1 parte, entre el inicio y el final no hay ni un byte "confirmado" →
  velocidad 0 y progreso 0 durante toda la subida.
- El "crédito" de las partes en vuelo (`_inflight_credit`) se calculaba con esa
  velocidad, que era 0 → crédito 0 → la barra no se movía.
- El ETA se calculaba con `restante ÷ velocidad` sobre esa velocidad falsa (0)
  y **sin dividir por las partes en paralelo** → tiempos irreales.
- Además, el texto del progreso tenía una rama `priming` que dejaba el mensaje
  en "⏱️ Tiempo transcurrido" **mientras `bytes_transferred` fuera 0**, es decir,
  durante toda la subida de un archivo de una sola parte: sin barra, sin
  velocidad y sin ETA.

**Cómo queda ahora** (todo medido en `tests/test_progress.py`, 33 pruebas):

1. `RateMeter`: cada parte terminada aporta **bytes y segundos reales** → la
   velocidad sale de datos de verdad, no de "cuánto ha confirmado el endpoint".
2. `SpeedStats` (`data/speed_stats.json`): velocidad **aprendida y persistida**
   con suavizado EWMA (α 0,3), semilla del historial al arrancar, límites de
   cordura (32 KB/s – 200 MB/s) para que una medición absurda no envenene nada.
3. **Crédito en vuelo** con la velocidad real (propia o aprendida), con tope del
   90 % por parte: el 10 % final solo al confirmar. Eso es lo que hace que la
   barra se mueva desde el primer refresco.
4. **Progreso monótono** (`_processed_peak`): nunca retrocede, ni con reintentos.
5. **Descarga por parte** (`note_fetched` / `_part_fetched`): en streaming la
   bajada también se ve avanzar, no solo la subida.
6. **ETA correcta**: cuenta las **oleadas** de partes en paralelo
   (`ceil(partes/workers)`) y el tramo final de las partes ya en vuelo.
7. **Velocidad honesta**: mientras no haya una medición propia se muestra `~` y
   la etiqueta *📈 velocidad estimada*; deja de marcarse en cuanto hay dato real.
8. La barra y el ETA se muestran **desde el primer refresco** (ya no hay rama
   que esconda el progreso).

Resultado en la misma prueba: 0,68 % a los 0,3 s, 2,28 % a los 1,0 s, sin saltos
(ningún salto > 12 % entre refrescos), ETA inicial correcta con 3 partes en
paralelo y velocidad medida final a menos del 2 % de la real.

### 5.2 🐞 Fallos reales encontrados y arreglados

| # | Fallo | Consecuencia | Arreglo |
|---|---|---|---|
| 1 | `sample_speed` medía la subida por bytes confirmados | Barra congelada en 0 % (1 parte) y escalones (varias) | `RateMeter` + crédito en vuelo (§5.1) |
| 2 | `eta_seconds` ignoraba las partes en paralelo | ETA 3× más larga de lo real | Cuenta por oleadas + partes en vuelo |
| 3 | Texto en modo `priming` durante toda la subida de 1 parte | Sin barra, velocidad ni ETA | Se eliminó la rama; la barra siempre se pinta |
| 4 | `bytes_fetched` era un atributo asignado a mano | La bajada en streaming no se veía avanzar | Es una propiedad que suma por partes |
| 5 | `notifier._sync_disk_progress` importaba mal el nombre seguro | El tapón de seguridad de progreso podía fallar | `from .downloader import safe_local_name` |
| 6 | `pipeline._upload_sync` medía con `time.time()`/`monotonic()` mezclados | Velocidad por parte imprecisa | Cronómetro `time.monotonic()` por parte |
| 7 | No se verificaba lo subido | Una parte truncada pasaba por buena | `verify_part` con HEAD (tolerante a endpoints sin HEAD) |
| 8 | Reintentar una tarea volvía a subir todo | Datos y tiempo perdidos | `resume_folder` + `resumed_parts` (reanudación real) |
| 9 | `storage.avg_speeds()` con un JOIN innecesario | Consulta frágil | Simplificada |
| 10 | `pipeline.py` variable sin usar, `texts.metric_speed` duplicada | Ruido de lint | Limpiado (`pyflakes` sin avisos) |

### 5.3 ✨ Funciones nuevas que añadí

| Función | Dónde | Qué hace |
|---|---|---|
| Velocidad aprendida persistente | `modules/speedstats.py` (nuevo) | la estimación mejora sola con cada subida y sobrevive a los reinicios |
| Verificación de integridad | ajuste 🔍 + `s3_client.verify_part` | comprueba con HEAD el tamaño de cada parte subida |
| Reanudación de partes | `_run_parts(skip=…)` + `_resume_state` | al reintentar sube **solo lo que falta** ("♻️ Reanudando: 3 de 5 partes") |
| Registro en canal | `LOG_CHANNEL_ID` + `notifier._log_to_channel` | copia cada subida completada a un canal |
| Baneos | `/ban`, `/unban`, `/baneados` | el baneado recibe aviso una vez y no puede usar nada |
| Cuota diaria | ajuste 📊 `daily_quota_mb` + `storage.bytes_today` | corta a quien abuse sin tocar el resto |
| Modo mantenimiento | `/mantenimiento` (ajuste 🛠) | bloquea envíos nuevos sin cortar los que están en curso |
| Búsqueda en el historial | `/buscar <texto>` | encuentra una subida antigua por nombre |
| Soporte de **fotos** | `modules/handlers/upload.py` | acepta fotos (toma la resolución mayor) además de archivos/vídeos/URLs |
| Aviso de fallos al owner | `notifier._alert_owner` | máximo un aviso por minuto, con el motivo |
| `/status` con velocidades | `modules/handlers/basic.py` | muestra la velocidad aprendida de subida y bajada |
| `.env.example` | raíz del repo | plantilla completa comentada (ya no hay que adivinar variables) |

`/settings` pasa de 12 a **15 ajustes** en las mismas 3 páginas.

### 5.4 ⚠️ Aviso sobre tu `.env` (lo dejo como está, como pediste)

1. **`DOWNLOAD_DIR=/app/downloads` está fuera del volumen.** En Railway el disco
   fuera del volumen es diminuto y efímero: con `MIN_FREE_GB=2` empezarás a ver
   *"❌ Sin espacio en disco"* en archivos grandes. **Cámbialo a
   `/app/data/downloads`** (el `.env.example` ya lo trae así).
2. **`API_ID` / `API_HASH` / `BOT_TOKEN` siguen escritos en el `.env`.** No los
   toqué, pero recuerda: si ese `.env` estuvo alguna vez en GitHub, revoca el
   token con @BotFather (`/revoke`) y pon el nuevo.
3. `railtway.toml` / `railway.json`: **no lo commitees** en el repo público
   (fuerza *builds* en cada push). Usa las variables de entorno del panel.

---

## 6. 🗺 Lo que todavía le falta (por orden de utilidad real)

Esto es lo que **aún no está** y yo añadiría después, con prioridad honesta.

### 🔴 Prioridad alta (se nota todos los días)

1. **Deduplicación por hash SHA-256 + caché de enlaces.** Si alguien reenvía un
   archivo que ya subió antes, devolver sus enlaces al instante sin volver a
   subirlo. Ahorra el 100 % del tiempo en los reenvíos típicos de grupos. Es la
   mejora más rentable que le queda al bot.
2. **Renovación / comprobación de enlaces.** Comprobar si los enlaces siguen
   vivos (HEAD) y avisar cuando caduquen, con opción de re-subir desde el
   histórico (`keys` ya está guardado, así que es viable).
3. **Exportación del historial** a `.csv` / `.json` desde `/historial` (el owner
   ya pide el `.txt` de enlaces; el CSV sirve para controlar consumo).
4. **Prueba de carga real con 40 usuarios** (`tests/test_load.py`): el bot está
   diseñado para eso, pero conviene un test que simule 40 usuarios × 3 tareas
   con `todus` falso y compruebe que no hay *flood* ni bloqueos.

### 🟡 Prioridad media

5. **Miniaturas / previsualización**: primer fotograma o imagen embebida al
   mandar los enlaces (mejora muchísimo cómo se ve en el chat).
6. **Rate limit por segundo y por usuario** (ahora es "espera entre envíos", no
   un caudal): útil si alguien encola cientos de archivos pequeños.
7. **Limpieza automática de temporales huérfanos** al arrancar y cada X horas
   (hoy se limpia en `/limpiar` y al terminar tareas).
8. **Alertas al owner más allá de fallos**: cola saturada, disco bajo, endpoint
   de toDus caído, `FLOOD_WAIT` de Telegram.
9. **Página de estado web** (`healthcheck.py` ya existe): exponerla con un
   `healthcheckPath` real para monitorizar desde el navegador.
10. **Internacionalización (ES/EN)** del texto de los mensajes.

### 🟢 Opcional / cuando haya tiempo

11. **Webhook/HTTP opcional** para integrarlo con otro sistema propio.
12. **Firma de los mensajes del canal de registro** (avisar también de fallos y
    cancelaciones, no solo completadas).
13. **Cifrado del `history.db`** si te preocupa que los nombres de archivo queden
    en disco en claro.

Dime cuál quieres que haga y lo monto; la deduplicación por hash y la
comprobación de enlaces son las dos que más valor dan por el trabajo que cuestan.
