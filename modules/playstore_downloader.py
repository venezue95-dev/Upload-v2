"""Descarga de aplicaciones gratuitas desde enlaces públicos de Google Play.

El trabajo de extracción se delega al componente GPLv3 de
third_party/gplay-apk-downloader. Este módulo solo valida el enlace, prepara
un entorno persistente de autenticación y devuelve el APK generado.
"""
from __future__ import annotations

import asyncio
import os
import re
import subprocess
from pathlib import Path
from typing import Optional
from urllib.parse import parse_qs, urlparse

from config import CONFIG

PACKAGE_RE = re.compile(r"^[a-zA-Z][a-zA-Z0-9_]*(?:\.[a-zA-Z0-9_]+)+$")


def package_from_url(value: str) -> Optional[str]:
    """Extrae un applicationId de una URL de Play Store o de un package ID."""
    raw = (value or "").strip()
    if PACKAGE_RE.fullmatch(raw):
        return raw
    parsed = urlparse(raw)
    host = (parsed.hostname or "").lower()
    if host not in {"play.google.com", "www.play.google.com"}:
        return None
    package = parse_qs(parsed.query).get("id", [""])[0]
    return package if PACKAGE_RE.fullmatch(package) else None


def is_play_store_url(value: str) -> bool:
    return package_from_url(value) is not None and value.strip().startswith(("http://", "https://"))


def _root() -> Path:
    return Path(__file__).resolve().parent.parent / "third_party" / "gplay-apk-downloader"


def _home() -> Path:
    path = Path(CONFIG.data_dir) / "gplay-home"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _run(package: str, output: Path) -> Path:
    root = _root()
    script = root / "gplay-downloader.py"
    if not script.exists():
        raise RuntimeError("el componente de Google Play no está instalado en el despliegue")
    env = os.environ.copy()
    env["HOME"] = str(_home())
    env["PYTHONUNBUFFERED"] = "1"
    dispenser = os.getenv("PLAYSTORE_DISPENSER_URL", "").strip()
    account_email = os.getenv("GPLAY_ACCOUNT_EMAIL", "").strip()
    aas_token = os.getenv("GPLAY_AAS_TOKEN", "").strip()
    auth = _home() / ".gplay-auth.json"
    if not auth.exists():
        if account_email and aas_token:
            auth_cmd = [
                "python", str(script), "auth-account",
                "--email", account_email,
                "--aas-token", aas_token,
            ]
        elif dispenser:
            auth_cmd = ["python", str(script), "auth", "--dispenser", dispenser]
        else:
            raise RuntimeError(
                "Play Store necesita autenticación. Configura GPLAY_ACCOUNT_EMAIL y "
                "GPLAY_AAS_TOKEN en Railway (recomendado), o PLAYSTORE_DISPENSER_URL"
            )
        result = subprocess.run(auth_cmd, cwd=root, env=env, capture_output=True, text=True, timeout=120)
        if result.returncode != 0:
            detail = (result.stderr or result.stdout or "autenticación fallida").strip()
            raise RuntimeError(f"no se pudo autenticar con Google Play: {detail[-500:]}")
    output.mkdir(parents=True, exist_ok=True)
    cmd = ["python", str(script), "download", package, "--output", str(output), "--merge", "--arch", "arm64"]
    result = subprocess.run(cmd, cwd=root, env=env, capture_output=True, text=True, timeout=900)
    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "descarga fallida").strip()
        raise RuntimeError(f"Google Play rechazó la descarga: {detail[-700:]}")
    merged = sorted(output.glob("*-merged.apk"), key=lambda p: p.stat().st_mtime, reverse=True)
    if merged:
        return merged[0]
    apks = sorted(output.glob("*.apk"), key=lambda p: p.stat().st_mtime, reverse=True)
    if len(apks) == 1:
        return apks[0]
    if apks:
        raise RuntimeError("la aplicación usa APK divididos y no se pudo generar un APK único")
    raise RuntimeError("Google Play no devolvió ningún APK")


async def download_play_store(package_or_url: str, destination_dir: str, task=None) -> str:
    package = package_from_url(package_or_url)
    if not package:
        raise RuntimeError("enlace de Google Play inválido o sin parámetro id")
    if task is not None and task.is_cancelled:
        raise RuntimeError("descarga cancelada por el usuario")
    output = Path(destination_dir)
    output.mkdir(parents=True, exist_ok=True)
    apk = await asyncio.to_thread(_run, package, output)
    target = output / f"{package}.apk"
    if apk != target:
        apk.replace(target)
    return str(target)
