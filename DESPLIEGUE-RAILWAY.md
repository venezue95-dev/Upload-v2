# 🚂 Desplegar el bot en Railway (guía paso a paso)

Esta guía está escrita para **este** proyecto (bot de Telegram con Pyrogram +
librería `todus`, sin servidor web). Cubre las dos formas de subirlo y los
5 puntos donde la gente se atasca en Railway.

---

## ⚠️ Antes de empezar: 5 reglas de oro en Railway

| Regla | Por qué |
|---|---|
| **1 sola réplica** (`numReplicas = 1`) | Con 2 réplicas, dos procesos hacen `getUpdates` con el mismo token → Telegram devuelve *Conflict: terminated by other getUpdates request* y las tareas se descuadran. Además **Railway no permite volúmenes con réplicas**. |
| **Volumen montado en `/app/data`** | Ahí viven la sesión de Pyrogram, `settings.json`, `queue_state.json` (cola guardada) y `history.db`. Sin volumen, cada deploy borra todo y el bot "olvida" las tareas. |
| **Nada de `.env` en el repo** | Railway usa *Variables* del panel. El `.dockerignore` y el `.railwayignore` ya excluyen el `.env`. |
| **`DOWNLOAD_DIR` dentro del volumen** (`/app/data/downloads`) | El disco del contenedor en Railway es pequeño y **efímero**. Si lo dejas en `/app/downloads`, la comprobación `MIN_FREE_GB=2` puede rechazar todas las descargas por "falta de espacio". |
| **No hace falta dominio público** | El bot usa *long polling*: no abre puerto. Deja la sección **Networking** sin tocar. |

---

## Opción A · Desde GitHub (la más cómoda)

### 1. Sube el código (¡sin el `.env`!)

```bash
cd Uplo                     # la carpeta del proyecto ya arreglado
git init                    # si aún no es un repo
git add .
git status                  # comprueba que .env NO aparece en la lista
git commit -m "Bot v2: cola escalable, streaming y /settings"
git remote add origin https://github.com/TU_USUARIO/Uplo.git
git push -u origin main
```

> 🔒 Antes de subir nada: si vas a reutilizar el repo público
> `venezue95-dev/Uplo`, **revoca el token en @BotFather primero** (el `.env`
> antiguo con tus credenciales sigue en el historial de ese repositorio).

### 2. Crea el proyecto en Railway

1. [railway.com](https://railway.com) → **New Project** → **Deploy from GitHub repo**.
2. Autoriza GitHub y elige el repositorio.
3. Railway detecta el **`Dockerfile` en la raíz** y construye con él
   (no hace falta `railway.toml`: además *Config as Code* está en desuso y los
   servicios nuevos ya no lo usan).

### 3. Configura el servicio

**Settings → Build**
- *Builder*: **Dockerfile** (normalmente ya autodetectado).
- *Root Directory*: `/` (el Dockerfile está en la raíz).

**Settings → Deploy**
- *Replicas*: **1** ← crítico.
- *Restart Policy*: **On Failure**.
- *Healthcheck*: déjalo vacío (no hay servidor HTTP; el bot trae su propio
  `healthcheck.py` para Docker/compose).

**Volumes → Add Volume**
- *Mount Path*: `/app/data` · tamaño 5 GB (Hobby) es suficiente de sobra.

**Variables → Raw Editor**: pega el bloque de
[Variables recomendadas](#-variables-para-pegar-en-railway) de abajo.

### 4. Deploy

Pulsa **Deploy**. En *Deployments → View Logs* deberías ver:

```
=== toDus S3 Uploader Bot ===
Almacenamiento: toDus S3 | chunk=50 MB | reintentos=5
Historial SQLite listo en /app/data/history.db
Backend de almacenamiento: todus.S3Client
Handlers registrados (8 módulos)
Cola iniciada | tareas simultáneas=4 | por usuario=1 | espera máx=200
Bot en línea: @tu_bot (id=123456789)
Listo | tareas simultáneas=4 | ...
```

Abre Telegram y manda `/status`.

---

## Opción B · Desde tu PC con la CLI (sin GitHub)

```bash
# 1. Instalar la CLI
npm i -g @railway/cli          # o: brew install railway
#   (alternativa: curl -fsSL https://railway.app/install.sh | sh)

# 2. Entrar
railway login

# 3. Dentro de la carpeta del proyecto:
cd Uplo
railway init                   # crea el proyecto (o `railway link` si ya existe)

# 4. Subir y desplegar el código actual
railway up                     # adjunta los logs del build
#   --detach para no quedarte viendo los logs, --ci para CI

# 5. Volumen persistente (mismo mount path que usan las variables)
railway volume add --mount-path /app/data

# 6. Variables (opcional; también se hace en el panel)
railway variables --set "API_ID=1234567" --set "API_HASH=..." --set "BOT_TOKEN=..." \
                  --set "BOT_OWNER_ID=123456789"
#   (si tu versión de la CLI usa otro flag: `railway variables --help`)

# 7. Reiniciar para que quede montado el volumen y aplicadas las variables
railway redeploy

# 8. Ver logs en vivo
railway logs
```

> `railway up` respeta `.gitignore` y `.railwayignore`, así que el `.env`,
> `data/`, `downloads/` y las sesiones **no suben**.

---

## 🔑 Variables para pegar en Railway

En **Variables → Raw Editor** pega esto y cambia solo los valores:

```
API_ID=1234567
API_HASH=tu_api_hash_de_my.telegram.org
BOT_TOKEN=1234567890:ABCdef_GHIjkl-MNOpqr
BOT_SESSION_NAME=s3_bot_session
BOT_OWNER_ID=tu_id_de_userinfobot
BOT_ALLOWED_USERS=

# --- almacenamiento ---
MAX_FILE_MB=800
S3_CHUNK_SIZE_MB=50
S3_MAX_PARALLEL=3
S3_MAX_RETRIES=5
S3_GLOBAL_WORKERS=6

# --- cola (esto es lo que aguanta 20-40 usuarios) ---
QUEUE_MAX_CONCURRENT=4
QUEUE_MAX_PER_USER=1
QUEUE_MAX_TOTAL=200
QUEUE_MAX_WAITING_PER_USER=15
RATE_LIMIT_SEC=3
TASK_TIMEOUT_MIN=240
TASK_AUTO_RETRIES=1
FAIR_SCHEDULING=true

# --- descargas y disco (¡rutas del volumen!) ---
WORK_DIR=/app/data
DATA_DIR=/app/data
DOWNLOAD_DIR=/app/data/downloads
MIN_FREE_GB=1
STREAM_MODE=auto
TG_CONCURRENT_DOWNLOADS=4
TELEGRAM_WORKERS=12

# --- interfaz ---
PROGRESS_INTERVAL_SEC=3
LOG_LEVEL=INFO
```

Notas:
- **No definas `PORT`**: no hay servidor web.
- `BOT_ALLOWED_USERS` vacío = bot público. Para hacerlo privado:
  `BOT_ALLOWED_USERS=11111111,22222222`.
- Los ajustes marcados como cíclicos en `/settings` se pueden cambiar desde el
  propio bot sin redeploy (se guardan en `/app/data/settings.json`).
- El resto de las variables del panel **tienen prioridad** sobre cualquier
  fichero; el bot nunca lee un `.env` en producción porque no se despliega.

---

## 💰 Qué plan y qué recursos

| | Free | Hobby ($5/mes) | Pro ($20/mes) |
|---|---|---|---|
| Volumen | 0.5 GB | **5 GB** | 50 GB |
| RAM/vCPU por servicio | 0.5 GB / 1 | hasta 48 GB / 48 | hasta 1 TB / 1000 |
| Réplicas | 1 | 6 | 42 |

- Con el bot en marcha (~300-450 MB de RAM, CPU baja salvo cuando sube):
  en **Hobby** el consumo típico se queda dentro o muy cerca de los $5 de
  crédito incluidos; el volumen de 5 GB cuesta ≈ $0.75/mes.
- **Free** puede arrancar el bot, pero 0.5 GB de volumen es poco y 0.5 GB de
  RAM se queda corto con varias descargas a la vez → no lo recomiendo para
  20-40 usuarios.
- Región: elige la más cercana a tus usuarios (US East si estás en Miami).
  El volumen se crea en la región del servicio y no se puede mover después.

---

## 🧪 Comprobar que quedó bien

En Telegram:

1. `/status` → debe decir **✅ en línea**, el endpoint de toDus accesible y la
   cola con 4 tareas simultáneas.
2. Manda un archivo pequeño → debe dar el enlace directo.
3. `/cola` → posición, progreso, ETA y botones.

En Railway:

- **Deployments → Logs**: sin `Traceback` al arrancar.
- **Metrics**: RAM estable y picos de CPU solo al subir.
- **Volumes → Browse**: después de un rato deben aparecer `history.db`,
  `queue_state.json` y `settings.json`.

Prueba de persistencia (opcional, recomendable): con una tarea pesada en curso,
haz **Redeploy**. El bot debe volver y avisarte por Telegram
*"♻️ Tarea reanudada tras el reinicio del bot"*.

---

## 🩹 Problemas típicos en Railway

| Síntoma | Causa / solución |
|---|---|
| `Conflict: terminated by other getUpdates request` | Hay **2+ réplicas** (o dos servicios con el mismo token). Deja **1 réplica** y borra el otro servicio. |
| Al redeployar se pierden cola, ajustes e historial | No montaste el volumen o no está en `/app/data`. Revisa *Volumes* y que `WORK_DIR`/`DATA_DIR` apunten ahí. |
| "❌ Sin espacio en disco" en cada tarea | `DOWNLOAD_DIR` está en el disco efímero. Ponlo en `/app/data/downloads` y `MIN_FREE_GB=1`. |
| El build falla en `pip install ... todcrypto/tgcrypto` | Es la compilación de TgCrypto: el Dockerfile ya instala `gcc` + `libc6-dev`. Si aun así falla, quita `TgCrypto` de `requirements.txt` (el bot funciona igual, solo más lento). |
| El servicio se reinicia solo cada pocos minutos | Suele ser falta de RAM en Free (0.5 GB). Sube a Hobby o baja `QUEUE_MAX_CONCURRENT` a 2 y `S3_GLOBAL_WORKERS` a 3. |
| Railway avisa *"No port detected"* / no hay dominio | Es normal y correcto en un bot de long polling. No genera dominio. |
| `ImportError: cannot import name 'NamespaceManager'` | Estás desplegando el código viejo. Con la v2 no ocurre (se adapta a `S3Client`). |
| El bot funciona pero no responde a `/start` | Revisa `BOT_ALLOWED_USERS` (si está puesto, solo esos IDs) y que el token sea el nuevo (si revocaste el anterior). |

---

## 🔄 Actualizar el bot más adelante

- **Con GitHub**: `git push` → Railway redeploya solo.
- **Con CLI**: `railway up` otra vez.
- **Sin cambiar código**: usa `/settings` en el propio Telegram para ajustar la
  cola (se aplica al instante y se guarda en el volumen).
