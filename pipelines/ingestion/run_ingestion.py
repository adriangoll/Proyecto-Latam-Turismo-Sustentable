"""
run_ingestion.py — Orquestador local de los 3 scripts de ingesta
=================================================================
Ejecuta los 3 scripts en secuencia y reporta resultado final.

Uso:
  python run_ingestion.py              # corre los 3
  python run_ingestion.py --dry-run    # sin subir a S3
  python run_ingestion.py --source co2                # solo uno
  python run_ingestion.py --source worldbank          # solo uno
  python run_ingestion.py --source transport          # solo uno

En producción esto será reemplazado por el DAG de Airflow.
Este script es útil para pruebas locales y onboarding del equipo.
"""

import argparse
import logging
import sys
import time

logger = logging.getLogger("ingestion.runner")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def run_all(dry_run: bool, sources: list[str]) -> None:
    results = {}
    start_total = time.time()

    SOURCES = {
        "co2": ("ingest_owid_co2", "OWID CO₂ & Emissions"),
        "worldbank": ("ingest_worldbank_tourism", "World Bank Tourism"),
        "transport": ("ingest_owid_transport", "OWID Transport Mode"),
    }

    for key, (module_name, label) in SOURCES.items():
        if sources and key not in sources:
            continue

        logger.info("\n%s", "═" * 60)
        logger.info("▶  %s", label)
        logger.info("═" * 60)
        t0 = time.time()

        try:
            module = __import__(module_name)
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

    # ── Resumen final ──────────────────────────────────────────────────────────
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