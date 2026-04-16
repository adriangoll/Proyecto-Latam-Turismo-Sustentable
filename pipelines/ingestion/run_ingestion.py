"""
run_ingestion.py — Orquestador local de los 3 scripts de ingesta

Uso:
  python run_ingestion.py --dry-run
  python run_ingestion.py --source co2
  python run_ingestion.py --source worldbank
  python run_ingestion.py --source transport
"""

import argparse
import logging
import os
import sys
import time

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

# ✔ FIX IMPORTS
import ingest_owid_co2
import ingest_worldbank_tourism
import ingest_unwto_transport

logger = logging.getLogger("ingestion.runner")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# ✔ FIX SOURCES
SOURCES = {
    "co2": (ingest_owid_co2, "OWID CO2 & Emissions"),
    "worldbank": (ingest_worldbank_tourism, "World Bank Tourism"),
    "transport": (ingest_unwto_transport, "UNWTO Transport Mode"),
}


def run_all(dry_run: bool, sources: list) -> None:
    results = {}
    start_total = time.time()

    for key, (module, label) in SOURCES.items():
        if sources and key not in sources:
            continue

        logger.info("\n%s", "=" * 60)
        logger.info(">>  %s", label)
        logger.info("=" * 60)
        t0 = time.time()

        try:
            module.run(dry_run=dry_run)
            elapsed = time.time() - t0
            results[label] = ("OK", f"{elapsed:.1f}s")
        except SystemExit as e:
            elapsed = time.time() - t0
            results[label] = (f"FAILED (exit {e.code})", f"{elapsed:.1f}s")
        except Exception as e:
            elapsed = time.time() - t0
            results[label] = (f"ERROR: {e}", f"{elapsed:.1f}s")
            logger.exception("Error en %s", label)

    total_elapsed = time.time() - start_total
    logger.info("\n%s", "=" * 60)
    logger.info("RESUMEN%s", " [DRY-RUN]" if dry_run else "")
    logger.info("=" * 60)

    all_ok = True
    for label, (status, elapsed) in results.items():
        icon = "OK  " if status == "OK" else "FAIL"
        logger.info("  [%s] %-35s (%s)", icon, label, elapsed)
        if status != "OK":
            logger.info("       %s", status)
            all_ok = False

    logger.info("-" * 60)
    logger.info("  Tiempo total: %.1fs", total_elapsed)
    logger.info("=" * 60)

    if not all_ok:
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--source", choices=["co2", "worldbank", "transport"])
    args = parser.parse_args()

    sources = [args.source] if args.source else []
    run_all(dry_run=args.dry_run, sources=sources)
