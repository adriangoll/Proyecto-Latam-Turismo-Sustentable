"""
run_transformation.py — Orquestador Silver
==========================================
Corre los 3 scripts de transformación Bronze →Silver en secuencia.

Uso:
  python run_transformation.py --dry-run
  python run_transformation.py --source co2
  python run_transformation.py --source tourism
  python run_transformation.py --source transport

  # Dry-run con Bronze local (para pruebas en Windows sin S3)
  python run_transformation.py --dry-run \\
    --local-bronze-co2       data/bronze/co2_emissions \\
    --local-bronze-tourism   data/bronze/tourism_arrivals \\
    --local-bronze-transport data/bronze/transport_mode
"""

import argparse
import logging
import os
import sys
import time

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

import transform_co2
import transform_tourism
import transform_transport

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logger = logging.getLogger("silver.runner")


def run_all(
    dry_run: bool,
    sources: list,
    local_bronze: dict,
) -> None:
    SOURCES = {
        "co2": (transform_co2, "CO2 Emissions", local_bronze.get("co2")),
        "tourism": (transform_tourism, "Tourism Arrivals", local_bronze.get("tourism")),
        "transport": (
            transform_transport,
            "Transport Mode",
            local_bronze.get("transport"),
        ),
    }

    results = {}
    start_total = time.time()

    for key, (module, label, lb_path) in SOURCES.items():
        if sources and key not in sources:
            continue

        logger.info("\n%s", "=" * 60)
        logger.info(">>  Silver: %s", label)
        logger.info("=" * 60)
        t0 = time.time()

        try:
            module.run(dry_run=dry_run, local_bronze=lb_path)
            elapsed = time.time() - t0
            results[label] = ("OK", f"{elapsed:.1f}s")
        except SystemExit as e:
            elapsed = time.time() - t0
            results[label] = (f"FAILED (exit {e.code})", f"{elapsed:.1f}s")
        except Exception as e:
            elapsed = time.time() - t0
            results[label] = (f"ERROR: {e}", f"{elapsed:.1f}s")
            logger.exception("Error en transform %s", label)

    total_elapsed = time.time() - start_total

    logger.info("\n%s", "=" * 60)
    logger.info("RESUMEN Silver%s", " [DRY-RUN]" if dry_run else "")
    logger.info("=" * 60)

    all_ok = True
    for label, (status, elapsed) in results.items():
        icon = "OK  " if status == "OK" else "FAIL"
        logger.info("  [%s] %-35s (%s)", icon, label, elapsed)
        if status != "OK":
            logger.info("       └─ %s", status)
            all_ok = False

    logger.info("-" * 60)
    logger.info("  Tiempo total: %.1fs", total_elapsed)
    logger.info("=" * 60)

    if not all_ok:
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Runner Silver — Bronze → Silver")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--source",
        choices=["co2", "tourism", "transport"],
        help="Correr solo una fuente específica",
    )
    parser.add_argument("--local-bronze-co2", default=None)
    parser.add_argument("--local-bronze-tourism", default=None)
    parser.add_argument("--local-bronze-transport", default=None)

    args = parser.parse_args()

    local_bronze = {
        "co2": args.local_bronze_co2,
        "tourism": args.local_bronze_tourism,
        "transport": args.local_bronze_transport,
    }

    sources = [args.source] if args.source else []
    run_all(dry_run=args.dry_run, sources=sources, local_bronze=local_bronze)
