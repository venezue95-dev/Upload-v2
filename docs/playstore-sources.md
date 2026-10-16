# Fuentes de la integración de Google Play

- https://github.com/alltechdev/gplay-apk-downloader — componente elegido; licencia GPLv3; requiere Python, Java 17 o superior, `apksigner` y un dispenser o autenticación propia. El contenedor usa OpenJDK 21 por compatibilidad con Debian trixie. Descarga APK base, splits, OBB y asset packs.
- https://developer.android.com/guide/app-bundle — documentación oficial de Android sobre App Bundles, APK divididos y el hecho de que una instalación parcial puede fallar.

La integración conserva el componente de terceros en `third_party/gplay-apk-downloader/` junto con su licencia. El bot entrega preferentemente el APK fusionado que genera el componente para la arquitectura ARM64. Si no hay `PLAYSTORE_DISPENSER_URL` configurado en Railway, el bot rechaza la tarea con un mensaje explícito y no usa credenciales personales automáticamente.
