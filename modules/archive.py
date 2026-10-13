"""Creación de archivos 7z divididos en volúmenes."""

from __future__ import annotations

import os
import shutil
import subprocess
from typing import List, Tuple

from .downloader import ensure_dir, safe_local_name


def create_7z_volumes(source_path: str, output_dir: str, original_name: str,
                       volume_bytes: int) -> List[Tuple[str, str, int]]:
    """Crea volúmenes 7z sin compresión y devuelve (ruta, nombre, tamaño).

    Los volúmenes resultantes tienen el formato ``archivo.7z.001`` y pueden
    abrirse con ZArchiver usando siempre el primer volumen (.001).
    """
    if not os.path.isfile(source_path):
        raise FileNotFoundError(source_path)
    volume_bytes = max(1, int(volume_bytes))
    ensure_dir(output_dir)
    safe = safe_local_name(original_name)
    stem, _ = os.path.splitext(safe)
    archive_base = os.path.join(output_dir, f"{stem}.7z")
    staged_source = os.path.join(output_dir, safe)
    if os.path.abspath(staged_source) != os.path.abspath(source_path):
        shutil.copyfile(source_path, staged_source)

    # -mx=0 conserva el contenido sin compresión; -v divide el contenedor.
    command = [
        "7z", "a", "-t7z", "-mx=0", "-y", "-bd",
        f"-v{volume_bytes}b", archive_base, staged_source,
    ]
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "error desconocido").strip()
        raise RuntimeError(f"no se pudo crear el archivo 7z: {detail[-500:]}")

    prefix = f"{stem}.7z."
    names = sorted(
        name for name in os.listdir(output_dir)
        if name.startswith(prefix) and name[len(prefix):].isdigit()
    )
    if not names:
        raise RuntimeError("7z no generó ningún volumen")
    volumes = []
    for name in names:
        path = os.path.join(output_dir, name)
        size = os.path.getsize(path)
        if size <= 0:
            raise RuntimeError(f"volumen 7z vacío: {name}")
        volumes.append((path, name, size))
    return volumes
