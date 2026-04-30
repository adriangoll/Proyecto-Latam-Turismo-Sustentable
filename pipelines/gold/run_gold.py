"""
run_gold.py — Runner Gold
=========================
Uso:
    python run_gold.py
    python run_gold.py --dry-run
"""

import argparse
import logging
import os
import sys
import time

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

import build_gold

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("gold.runner")


def run_all(dry_run: bool) -> None:
    logger.info("\n%s", "=" * 60)
    logger.info(">> Gold Pipeline")
    logger.info("=" * 60)

    t0 = time.time()
    try:
        build_gold.run(dry_run=dry_run)
        elapsed = time.time() - t0
        logger.info("\n%s", "=" * 60)
        logger.info("  [OK  ] Build Gold (%.1fs)", elapsed)
        logger.info("=" * 60)
    except Exception as e:
        elapsed = time.time() - t0
        logger.error("\n%s", "=" * 60)
        logger.error("  [FAIL] Build Gold (%.1fs): %s", elapsed, e)
        logger.error("=" * 60)
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    run_all(dry_run=args.dry_run)
