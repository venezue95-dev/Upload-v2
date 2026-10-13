# ☁️ toDus Upload Bot — v2

Bot de Telegram que convierte **archivos de Telegram** o **enlaces directos** en
**enlaces de descarga directa** alojados en el S3 público de toDus
(`s3.todus.cu`), con una **cola justa y escalable para 20-40+ usuarios**.

> 📌 Si vienes de la versión anterior, lee **[CAMBIOS.md](CAMBIOS.md)**: explica
> el bug crítico que impedía arrancar el bot, el rediseño de la cola y todo lo nuevo.

---

> 🚂 **¿Lo vas a subir a Railway?** Tienes una guía específica, paso a paso, en
> **[DESPLIEGUE-RAILWAY.md](DESPLIEGUE-RAILWAY.md)** (volumen en `/app/data`,
> 1 sola réplica, variables y los 5 fallos típicos de la plataforma).

## ⚡️ Lo esencial en 30 segundos

```bash
cp .env.example .env      # rellena API_ID, API_HASH, BOT_TOKEN, BOT_OWNER_ID
docker compose up -d --build
docker compose logs -f
```

En Telegram: `/start` → reenvía un archivo o pega un enlace → recibes el enlace
directo. Con `/cola` ves posición, progreso y ETA de cada tarea.

---

## 🚦 Cómo funciona la cola (lo importante para 40 usuarios)

```
Usuario A ─┐
Usuario B ─┤   ┌──────────────┐   ┌───────────────┐   ┌──────────────┐
Usuario C ─┼──▶│  ADMISIÓN    │──▶│ PLANIFICADOR  │──▶│  TRABAJADOR  │──▶ toDus S3
   ...     │   │ cupos por    │   │ reparto justo │   │ descarga +   │
Usuario Z ─┘   │ usuario      │   │ + prioridad   │   │ sube trozos  │
               └──────────────┘   └───────────────┘   └──────────────┘
```

1. **Los handlers no trabajan, solo encolan.** Antes, cada archivo ocupaba un
   worker del bot durante toda la subida: con 4 workers, el quinto usuario
   dejaba el bot ciego (ni `/cola` ni botones respondían). Ahora el handler
   responde en milisegundos.
2. **Admisión con cupos**: máximo de tareas activas por usuario, máximo en
   espera por usuario, máximo global en espera y anti-spam (`RATE_LIMIT_SEC`).
   Si el sistema está saturado, el usuario recibe un aviso **claro** en vez de
   silencio.
3. **Planificador único con reparto justo**: nunca hay más de
   `QUEUE_MAX_CONCURRENT` tareas activas, y quien **no tiene nada activo pasa
   primero**. Si un usuario sube 20 archivos, no bloquea a los demás.
4. **Prioridad con anti-inanición**: el owner tiene prioridad y, además, cada
   minuto de espera sube la prioridad de una tarea (nadie se queda abajo para
   siempre).
5. **Trabajadores reales**: cada tarea activa corre en su propio `asyncio.Task`;
   el trabajo pesado (descarga/subida) va a hilos. El bot sigue respondiendo.
6. **Un solo bucle de progreso** para todas las tareas (no uno por tarea): con
   40 tareas activas no se inunda la API de Telegram ni el event loop.
7. **Límite global de subidas** (`S3_GLOBAL_WORKERS`): 40 usuarios no lanzan
   300 PUT simultáneos a toDus. Ajustable en caliente con `/settings`.
8. **Modo de trabajo**: `auto` muestra primero la descarga y luego la subida para
   que el progreso sea claro. `stream` descarga y sube **a la vez**, trozo a trozo,
   para ahorrar disco; se activa explícitamente desde `/settings`.
9. **Posición + ETA por tarea**: `/cola` muestra `#3 en cola · comienza en
   ~4m 12s`, calculado con la velocidad real del bot y el trabajo pendiente.
10. **Sobrevive a los reinicios**: el estado de la cola se guarda en disco y
    las tareas pendientes se **reanudan** al arrancar (avisando al usuario).

### Números recomendados

| Usuarios simultáneos | QUEUE_MAX_CONCURRENT | QUEUE_MAX_PER_USER | S3_GLOBAL_WORKERS | S3_MAX_PARALLEL | RAM/CPU |
|---|---|---|---|---|---|
| 1-10 | 2 | 1 | 4 | 2 | 1 GB / 1 CPU |
| 10-25 | 4 | 1 | 6 | 3 | 1 GB / 2 CPU |
| **25-45** | **4-6** | **1** | **6-8** | **3** | **2 GB / 2 CPU** |
| 45+ | 6-8 | 1 | 8-12 | 3-4 | 4 GB / 4 CPU |

> Con `QUEUE_MAX_PER_USER=1` cada persona avanza de una en una: la sensación de
> "el bot me ignora" desaparece. Si tus usuarios envían archivos pequeños y
> prefieres más velocidad individual, súbelo a 2.

---

## 📈 Cómo se calcula el progreso (v1.3)

Antes el progreso se medía con "bytes confirmados ÷ tiempo". Eso **no funciona
con toDus**, porque el `PUT` de una parte no informa de bytes intermedios: el
bot solo se enteraba del avance **cuando terminaba la parte completa**. Resultado
real, medido en las pruebas: con un archivo de una sola parte la barra se quedaba
en **0 % durante toda la subida** (salto final a 100 %) y con varias partes daba
**escalones del tamaño de cada parte** con velocidad de 0 B/s casi todo el rato.

Ahora se calcula así:

1. **Velocidad aprendida y persistida** (`data/speed_stats.json`): cada parte
   terminada enseña al bot cuánto tarda toDus de verdad. Se guarda entre
   reinicios y se siembra con el historial (`/status` la muestra).
2. **Crédito en vuelo**: las partes que están subiéndose cuentan su avance
   estimado con esa velocidad (hasta el 90 %; el 10 % final solo al confirmar).
3. **Progreso monótono**: nunca retrocede, aunque una parte falle o se reintente.
4. **Velocidad honesta**: mientras no haya medición propia se muestra con `~`
   y la etiqueta *📈 velocidad estimada*.
5. **ETA correcta**: cuenta las partes en paralelo (`ceil(partes/workers)`)
   y el tramo final de las que ya están en vuelo, en vez de `restante ÷ velocidad`.

Efecto medido en `tests/test_progress.py`: la barra arranca al primer refresco
(0,7 % a los 0,3 s) y avanza de forma continua hasta el 100 %, sin congelarse
ni pegar saltos.

---

## 🧭 Comandos

| Comando | Quién | Qué hace |
|---|---|---|
| `/start` · `/menu` | todos | Menú con botones (cola, historial, estado, ayuda) |
| `/help` | todos | Ayuda en 3 páginas (lo básico, la cola, partes y consejos) |
| `/cola` | todos | Tus tareas: estado, posición, %, velocidad, ETA y botones |
| `/historial` | todos | Tus últimas subidas completadas con enlaces |
| `/cancelar` | todos | Cancela todas tus tareas (con confirmación) |
| `/reintentar [id]` | todos | Reintenta una tarea fallida/cancelada |
| `/enlaces [id]` | todos | Reenvía la lista de enlaces como `.txt` |
| `/buscar <texto>` | todos | Busca en tu historial por nombre de archivo |
| `/status` | todos | Estado del bot, del almacenamiento y de la cola |
| `/settings` | **owner** | 15 ajustes en caliente, en 3 páginas |
| `/stats` | **owner** | Estadísticas globales + top usuarios |
| `/usuarios` | **owner** | Lista de usuarios y su consumo |
| `/anuncio <texto>` | **owner** | Aviso a todos los usuarios conocidos |
| `/pausar` · `/reanudar` | **owner** | Pausa/reactiva la cola (deja terminar lo activo) |
| `/sistema` | **owner** | Disco, memoria, CPU, temporales, uptime |
| `/limpiar` | **owner** | Cancela la cola, borra temporales y purga historial >90 días |
| `/ban <id│@user> [motivo]` | **owner** | Bloquea a un usuario (no puede enviar nada) |
| `/unban <id│@user>` | **owner** | Levanta el bloqueo |
| `/baneados` | **owner** | Lista de usuarios bloqueados con motivo y fecha |
| `/mantenimiento` | **owner** | Activa/desactiva el modo mantenimiento (mira abajo) |

**Botones** (un único modelo, igual en comandos y menús):
- **Mensaje de una tarea**: solo acciones sobre ella (cancelar · reintentar · enlaces `.txt` · borrar de toDus). Nunca se convierte en un panel.
- **Paneles** (`/start`, cola, historial, ajustes…): navegación y siempre un botón «🏠 Inicio». El owner tiene un único botón «🛠 Administración».
- Los botones de mensajes antiguos siguen funcionando: la tarea se recupera del historial.

---

## 🗂 Estructura del proyecto

```
bot.py                  arranque, apagado ordenado y reanudación
healthcheck.py          salud para Docker (verifica que la COLA late, no solo el proceso)
config.py               lectura/validación del .env
modules/
  queue.py              ⭐️ planificador: cupos, justicia, prioridad, ETA, persistencia
  pipeline.py           ⭐️ trabajo real: descarga → trocea → sube → enlaces
  s3_client.py          adaptador de la librería todus (v3 y v2) + reintentos
  downloader.py         descargas de Telegram (por rangos) y de URLs + control de disco
  notifier.py           un único bucle que refresca el progreso de todas las tareas
  storage.py            historial y estadísticas en SQLite (data/history.db)
  runtime_settings.py   ajustes en caliente (/settings) con persistencia
  limiter.py            semáforo dinámico (límite global de subidas)
  texts.py              todos los textos (HTML seguro, recorta sin romper etiquetas)
  keyboards.py          todos los teclados y el esquema de callback_data
  views.py              texto + teclado de cada pantalla (comandos y botones comparten)
  actions.py            buscar / reintentar / borrar tareas (compartido)
  progress.py           formato de tamaños/velocidades/barras
  handlers/             interfaz con Telegram (solo validan y encolan)
tests/                  pruebas sin Telegram ni toDus (ver abajo)
```

---

## 🔧 Instalación y configuración

### 1. Credenciales de Telegram

#### API_ID y API_HASH

1. Entra en **https://my.telegram.org** con tu número (ej. `+53512345678`).
2. Te pedirá un código de confirmación que llega por Telegram.
3. Ve a **API development tools** → **Create new application**.
4. Rellena el formulario:
   - **App title**: cualquiera (ej. `toDus S3 Bot`).
   - **Short name**: corto en minúsculas (ej. `s3todusbot`).
   - **URL**: tu web o cualquier URL.
   - **Platform**: `Other`.
   - **Description**: opcional.
5. Pulsa **Create app** y copia `App api_id` → `API_ID` y `App api_hash` → `API_HASH`.

> ⚠️ **No compartas el `api_hash` con nadie.** Funciona como contraseña de la aplicación.

#### BOT_TOKEN

1. Abre [@BotFather](https://t.me/BotFather) → `/newbot`.
2. Nombre visible (ej. `toDus S3 Uploader`) y username terminado en `bot`.
3. Copia el token (`1234567890:ABCdef_GHIjkl-MNO...`) → `BOT_TOKEN`.

> ¿Lo perdiste? [@BotFather](https://t.me/BotFather) → `/mybots` → tu bot → **API Token**.

#### BOT_OWNER_ID

1. Abre [@userinfobot](https://t.me/userinfobot) → `/start`.
2. Copia el número de `Id:` → `BOT_OWNER_ID`. (También vale [@getmyid_bot](https://t.me/getmyid_bot).)

#### BOT_ALLOWED_USERS (opcional)

Para un bot **privado**, pega los IDs separados por comas:

```dotenv
BOT_ALLOWED_USERS=12345678,87654321,11223344
```

Vacío = **bot público** (cualquiera que hable con el bot puede subir).

### 2. toDus (sin credenciales)

La librería [`todus`](https://github.com/nyxthor-dev/todus-client) habla con
`s3.todus.cu` directamente; no hay token, Access Key ni Secret Key.

Cada usuario de Telegram sube a su propia carpeta:

```
users/tg_<user_id>/telegram/<fecha-hora>-<hash>/pelicula.mkv
users/tg_<user_id>/telegram/<fecha-hora>-<hash>/pelicula.part002.mkv
```

> ✅ **v2 corrige un fallo grave**: la versión anterior hacía
> `from todus import NamespaceManager`, que **ya no existe** en la librería
> actual → el bot moría en el arranque con `ImportError`. Ahora el bot detecta
> la versión instalada (`S3Client` en v3, `NamespaceManager` en v2) y funciona
> con ambas.

### 3. Arrancar

```bash
docker compose up -d --build
docker compose logs -f
```

En Railway no se usa `docker-compose.yml`: se sube el `Dockerfile` y se
configura el volumen + las variables en el panel. Ver
[DESPLIEGUE-RAILWAY.md](DESPLIEGUE-RAILWAY.md).

Variables imprescindibles del `.env`:

```dotenv
API_ID=1234567
API_HASH=abcdef1234567890abcdef1234567890
BOT_TOKEN=1234567890:ABC-DEF_ghijklmnopqrstuvwxyz
BOT_OWNER_ID=123456789
BOT_ALLOWED_USERS=
MAX_FILE_MB=800
S3_CHUNK_SIZE_MB=50
S3_MAX_PARALLEL=3
S3_MAX_RETRIES=10
```

…y las nuevas (todas con valor por defecto razonable, ver `.env.example`):

```dotenv
QUEUE_MAX_CONCURRENT=4
QUEUE_MAX_PER_USER=1
QUEUE_MAX_TOTAL=200
QUEUE_MAX_WAITING_PER_USER=15
RATE_LIMIT_SEC=3
TASK_TIMEOUT_MIN=240
S3_GLOBAL_WORKERS=6
STREAM_MODE=auto
MIN_FREE_GB=2
PROGRESS_INTERVAL_SEC=3
```

### Desarrollo local (sin Docker)

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
export $(grep -v '^#' .env | xargs)      # o pon las variables a mano
python -u bot.py
```

---

## 🧪 Pruebas

Cuatro suites que **no necesitan Telegram ni toDus** (usan un stub de la librería
y una base de datos temporal) y que sirven para verificar cualquier cambio:

```bash
python tests/test_queue.py       # 34 pruebas: cupos, justicia, cancelación, ETA, persistencia
python tests/test_pipeline.py    # 71 pruebas: pipeline real, reintentos, modos, textos, vistas
python tests/test_progress.py    # 33 pruebas: cálculo de progreso, velocidad y ETA (v1.3)
python tests/test_extras.py      # 87 pruebas: funciones nuevas de la v1.3 (v1.3)
```

Cubren, entre otras cosas: que nunca se superen los límites de concurrencia, que
un usuario con 10 archivos no bloquee a otro con 1, que la cancelación funcione a
mitad de subida, que los temporales se limpien siempre, que los mensajes no pasen
del límite de Telegram, que el HTML se escape correctamente, y (lo nuevo) que el
**progreso avance de forma continua y monótona** con velocidades conocidas.

---

### Suites disponibles

| Suite | Qué comprueba |
|---|---|
| `tests/test_queue.py` | cola, cupos, prioridad, posiciones y ETA (34 pruebas) |
| `tests/test_pipeline.py` | descarga, partes, subida, reintentos y reanudación (71 pruebas) |
| `tests/test_progress.py` | **cálculo de progreso y velocidad** con velocidades conocidas (33) |
| `tests/test_extras.py` | velocidades aprendidas, baneos, cuota, ajustes, avisos (87) |

---

## 📊 Ajustes en caliente (`/settings`)

Página 1 · **Cola y usuarios**

| Ajuste | Valores | Nota |
|---|---|---|
| 🚦 Tareas simultáneas (global) | 2 / 3 / 4 / 6 / 8 | el que más afecta a la carga |
| 👤 Tareas activas por usuario | 1 / 2 / 3 | 1 = máxima equidad |
| 📥 En espera por usuario | 5 / 10 / 15 / 25 | corta el spam |
| 📚 Cola total (espera) | 50 / 100 / 200 / 400 | aviso de "sistema saturado" |

Página 2 · **Subida y troceo**

| Ajuste | Valores | Nota |
|---|---|---|
| 🧩 Trozo (chunk) | 10 / 25 / 50 / 100 / 200 / 400 / 800 MB | más pequeño = más partes y más enlaces |
| ⚡️ Partes en paralelo (por tarea) | 1 / 2 / 3 / 4 | por tarea |
| 🏭 Subidas paralelas (todo el bot) | 2 / 4 / 6 / 8 / 12 | el freno global de toDus |
| 🔁 Reintentos por parte | 3 / 5 / 10 | con backoff exponencial |
| 📏 Máx. por archivo | 500 / 800 / 1000 / 2000 MB | límite por archivo |

Página 3 · **Avanzado**

| Ajuste | Valores | Nota |
|---|---|---|
| 🐢 Espera entre envíos | 0 / 3 / 10 / 30 s | anti-spam por usuario |
| 🌊 Modo de trabajo | auto / stream / classic | `auto` = streaming si se puede |
| ⏱️ Refresco del progreso | 2 / 3 / 5 / 10 s | más bajo = mensajes más vivos |
| 🔍 Verificar subidas (HEAD) | sí / no | comprueba el tamaño en toDus al terminar cada parte |
| 📊 Cuota diaria por usuario | 0 / 1 / 2 / 5 / 10 GB | `0` = sin límite; se cuenta lo enviado hoy |
| 🛠 Mantenimiento | no / sí | bloquea envíos nuevos sin cortar los que están en curso |

Todo se guarda en `data/settings.json` y se aplica **al instante** a las tareas
nuevas (el botón ♻️ vuelve a los valores del `.env`).

---

## 📣 Registro de subidas en un canal

Si defines `LOG_CHANNEL_ID` (por ejemplo `-1001234567890`), cada subida
completada se copia a ese canal con usuario, archivo, tamaño, partes, tiempo y
velocidad media. Es opcional: sin la variable el bot no envía nada. El bot debe
ser **administrador** del canal.

---

## 🧩 Partes, enlaces y `.txt`

Un archivo mayor que el trozo configurado se divide en
`pelicula.part001.mkv`, `pelicula.part002.mkv`… Cada parte es un archivo
independiente en toDus con su enlace (el almacenamiento no admite subidas
multiparte).

- El mensaje final lista los enlaces en un bloque `<pre>` (se copian con un toque).
- El botón **📄 Enlaces .txt** manda un archivo con los enlaces en orden y las
  instrucciones para unir las partes.
- El botón **🗑 Borrar de S3** elimina las partes del almacenamiento (los enlaces
  dejan de funcionar) — con confirmación.

---

## 🩺 Problemas frecuentes

| Síntoma | Causa y solución |
|---|---|
| `ImportError: cannot import name 'NamespaceManager'` | Versión vieja del bot. Esta v2 lo soluciona. |
| "Sin espacio en disco" | Baja `MIN_FREE_GB` no: **libera disco**. Con streaming cada tarea usa ~2-3 trozos (`S3_MAX_PARALLEL`); en modo classic necesita el archivo completo. |
| Los enlaces dan error 404 | El bucket de toDus puede purgar objetos. Guarda los enlaces y descarga pronto. |
| Telegram responde `FLOOD_WAIT` | Sube `PROGRESS_INTERVAL_SEC` a 5-10 y/o baja `QUEUE_MAX_CONCURRENT`. |
| Los archivos se suben lentos | Sube `S3_GLOBAL_WORKERS` (6→8/12) y `QUEUE_MAX_CONCURRENT`, y revisa el ancho de banda del servidor. |
| El bot no responde a comandos | Será un `AttributeError` o excepción: mira `docker compose logs`. El planificador nunca debería bloquearse (los handlers solo encolan). |
| Tras reiniciar, una tarea "no vuelve" | Se reanudan las tareas que no terminaron; si el mensaje original fue borrado, se marca fallida con aviso. |
| Docker marca el contenedor como `unhealthy` | Es el latido de la cola (`data/heartbeat`): si no se actualiza en 3 min, el planificador está bloqueado. Mira los logs. (En Railway no aplica: usa Restart Policy = On Failure.) |

---

## 🔐 Seguridad (importante)

- **El `.env` de este proyecto estuvo publicado en GitHub con `API_ID`,
  `API_HASH` y `BOT_TOKEN` reales.** Cualquiera que los copie puede controlar tu
  bot y usar tu api_hash. **Acciones recomendadas, en este orden:**
  1. [@BotFather](https://t.me/BotFather) → `/revoke` → **revoca el token** y
     pon el nuevo en `.env`.
  2. Si compartiste el `api_hash`, considera crear una nueva app en
     my.telegram.org y mover el bot a esas credenciales.
  3. Añade `.env`, `data/`, `downloads/` a `.gitignore` (ya incluido en la v2)
     y bórralos del historial de git si hace falta
     (`git rm --cached .env && git commit -m "quitar credenciales"`).
- El bot bloquea por defecto enlaces a IPs privadas (SSRF): `ALLOW_PRIVATE_URLS=false`.
- `BOT_ALLOWED_USERS` te deja el bot en privado si lo prefieres.

---

## 📄 Licencia

Uso libre para la comunidad de toDus. Respeta los términos de toDus y de
Telegram al usar el bot.
