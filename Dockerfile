# Dockerfile — toDus S3 Uploader Bot (v2)
FROM python:3.12-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# gcc + libc6-dev: compilar TgCrypto | git: instalar todus-client desde GitHub
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc \
        libc6-dev \
        git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Dependencias primero (mejor caché de capas).
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Código del bot.
COPY config.py bot.py healthcheck.py ./
COPY modules/ ./modules/

# Directorios persistentes.
RUN mkdir -p /app/data /app/downloads

# data/  → sesión de Pyrogram, settings.json, queue_state.json, history.db
# downloads/ → temporales (se limpian solos)

# Healthcheck: comprueba que la COLA sigue latiendo (no solo que el proceso viva).
HEALTHCHECK --interval=60s --timeout=10s --start-period=45s --retries=3 \
    CMD ["python", "healthcheck.py"]

CMD ["python", "-u", "bot.py"]
