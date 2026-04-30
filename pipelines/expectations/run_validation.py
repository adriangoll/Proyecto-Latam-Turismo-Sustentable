import argparse
import logging

from bronze_expectations import validate_bronze
from gold_expectations import validate_gold
from silver_expectations import validate_silver

logger = logging.getLogger("validation.runner")


def run(layer: str, source: str = None, dry_run: bool = False):
    """
    Corre validaciones para una capa.

    python run_validation.py --layer bronze --source co2_emissions
    python run_validation.py --layer silver --source all
    python run_validation.py --layer gold
    """

    if layer == "bronze":
        sources = ["co2_emissions", "tourism_arrivals", "transport_mode"] if not source else [source]
        for src in sources:
            validate_bronze(src, f"s3://latam-sustainability-datalake/bronze/{src}/", dry_run)

    elif layer == "silver":
        sources = ["co2_emissions", "tourism_arrivals", "transport_mode"] if source == "all" else [source]
        for src in sources:
            validate_silver(src, "s3://latam-sustainability-datalake/silver/", dry_run)

    elif layer == "gold":
        validate_gold(dry_run)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--layer", choices=["bronze", "silver", "gold"], required=True)
    parser.add_argument("--source", default=None)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    run(args.layer, args.source, args.dry_run)
