# ============================================================
# ☁️  SUBIR A DATABRICKS — Sube los archivos JSONL a Unity Catalog Volume
#
# Cómo ejecutar:
#   python Subir_Databricks.py
#
# Requiere:
#   databricks configure --token  (ya configurado)
# ============================================================

import os
import subprocess
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger(__name__)

CARPETA_LOCAL  = "./data/landing"
DBFS_DESTINO   = "dbfs:/Volumes/amazon_bi/bronze/landing"
DATABRICKS_EXE = os.path.expandvars(
    r"%LOCALAPPDATA%\Microsoft\WinGet\Links\databricks.exe"
)


def main():
    # Verificar que el ejecutable existe
    if not os.path.exists(DATABRICKS_EXE):
        log.error("No se encontró databricks.exe en: %s", DATABRICKS_EXE)
        log.error("Verifica la instalación con: winget install Databricks.DatabricksCLI")
        return

    archivos = [f for f in os.listdir(CARPETA_LOCAL) if f.endswith(".jsonl")]

    if not archivos:
        log.warning("No hay archivos JSONL en %s", CARPETA_LOCAL)
        log.warning("Ejecuta primero: python consumer.py")
        return

    log.info("Ejecutable : %s", DATABRICKS_EXE)
    log.info("Destino    : %s", DBFS_DESTINO)
    log.info("Subiendo %d archivos...", len(archivos))
    log.info("=" * 55)

    subidos  = 0
    fallidos = 0
    omitidos = 0

    for nombre in sorted(archivos):
        ruta_local = os.path.join(CARPETA_LOCAL, nombre)
        ruta_vol   = f"{DBFS_DESTINO}/{nombre}"

        try:
            result = subprocess.run(
                [DATABRICKS_EXE, "fs", "cp", ruta_local, ruta_vol],
                check=True, capture_output=True, text=True
            )
            # La CLI muestra "skipped; already exists" si ya estaba subido
            if "skipped" in result.stdout.lower():
                omitidos += 1
                log.info("⏭  %s (ya existe)", nombre)
            else:
                subidos += 1
                log.info("✓  %s", nombre)
        except subprocess.CalledProcessError as e:
            fallidos += 1
            log.error("✗  %s — %s", nombre, e.stderr)

    log.info("=" * 55)
    log.info("Subidos  : %d", subidos)
    log.info("Omitidos : %d (ya existían)", omitidos)
    log.info("Fallidos : %d", fallidos)
    log.info("Destino  : %s", DBFS_DESTINO)
    log.info("Ahora abre Databricks y ejecuta el notebook 01_bronze.py")


if __name__ == "__main__":
    main()