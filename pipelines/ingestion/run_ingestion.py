"""
run_ingestion.py — Orquestador local de los 3 scripts de ingesta
=================================================================
Ejecuta los 3 scripts en secuencia y reporta resultado final.

Uso:
  python run_ingestion.py              # corre los 3
  python run_ingestion.py --dry-run    # sin subir a S3
  python run_ingestion.py --source co2
  python run_ingestion.py --source worldbank
  python run_ingestion.py --source transport

En producción esto será reemplazado por el DAG de Airflow.
"""

import argparse
import importlib
import logging
import os
import sys
import time

# ── CRÍTICO: agregar la carpeta de este script a sys.path ────────────────────
# Sin esto, en Windows (y en cualquier entorno donde se corra el script
# desde otra carpeta), `from config import ...` falla con ModuleNotFoundError
# porque Python no sabe dónde buscar config.py, utils.py, etc.
# __file__ resuelve el path absoluto del script sin importar desde dónde se ejecute.
_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

logger = logging.getLogger("ingestion.runner")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def _import_module(module_name: str):
    """
    Importa un módulo por nombre usando importlib.
    Más robusto que __import__ bare: respeta sys.path correctamente
    en Windows, macOS y entornos de CI.
    Si el módulo ya fue importado en esta sesión, lo recarga
    para evitar estado cacheado entre ejecuciones.
    """
    if module_name in sys.modules:
        return importlib.reload(sys.modules[module_name])
    return importlib.import_module(module_name)


def run_all(dry_run: bool, sources: list[str]) -> None:
    results = {}
    start_total = time.time()

    SOURCES = {
        "co2":       ("ingest_owid_co2",         "OWID CO₂ & Emissions"),
        "worldbank": ("ingest_worldbank_tourism", "World Bank Tourism"),
        "transport": ("ingest_owid_transport",    "OWID Transport Mode"),
    }

    for key, (module_name, label) in SOURCES.items():
        if sources and key not in sources:
            continue

        logger.info("\n%s", "═" * 60)
        logger.info("▶  %s", label)
        logger.info("═" * 60)
        t0 = time.time()

        try:
            module = _import_module(module_name)
            module.run(dry_run=dry_run)
            elapsed = time.time() - t0
            results[label] = ("✅ OK", f"{elapsed:.1f}s")
        except SystemExit as e:
            elapsed = time.time() - t0
            results[label] = (f"❌ FAILED (exit {e.code})", f"{elapsed:.1f}s")
        except Exception as e:
            elapsed = time.time() - t0
            results[label] = (f"❌ ERROR: {e}", f"{elapsed:.1f}s")
            logger.exception("Error en %s", label)

    # ── Resumen final ─────────────────────────────────────────────────────────
    total_elapsed = time.time() - start_total
    logger.info("\n%s", "═" * 60)
    logger.info("📋 RESUMEN DE INGESTA%s", " [DRY-RUN]" if dry_run else "")
    logger.info("═" * 60)
    all_ok = True
    for label, (status, elapsed) in results.items():
        logger.info("  %-35s %s  (%s)", label, status, elapsed)
        if "❌" in status:
            all_ok = False
    logger.info("─" * 60)
    logger.info("  Tiempo total: %.1fs", total_elapsed)
    logger.info("═" * 60)

    if not all_ok:
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Orquestador de ingesta LATAM Datalake")
    parser.add_argument("--dry-run", action="store_true", help="Sin subir a S3")
    parser.add_argument(
        "--source",
        choices=["co2", "worldbank", "transport"],
        help="Ejecutar solo una fuente específica",
    )
    args = parser.parse_args()

    sources = [args.source] if args.source else []
    run_all(dry_run=args.dry_run, sources=sources)