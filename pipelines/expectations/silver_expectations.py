"""
silver_expectations.py — Validaciones para la capa Silver
==========================================================
Lee Silver transformado, valida métricas derivadas, genera reportes.

Métricas validadas:
  • co2_emissions: co2_per_capita_calc, co2_intensity_gdp, gdp_growth_pct
  • tourism_arrivals: arrivals_growth_pct, receipts_per_tourist
  • transport_mode: pct_* (modales), dominant_transport

Ejecutar:
  python run_validation.py --layer silver --source co2
  python run_validation.py --layer silver --source all
"""

import logging
import os
import sys

import pandas as pd

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

from config_expectations import EXPECTATIONS
from utils_expectations import (
    create_report,
    get_utc_date,
    read_parquet_s3,
    upload_report_s3,
    validate_column_between,
    validate_column_in_set,
    validate_column_not_null,
    validate_no_duplicates,
    validate_table_row_count,
)

logger = logging.getLogger("validation.silver")

# ─── Constantes ──────────────────────────────────────────────────────────────

BUCKET = "latam-sustainability-datalake"
LAYER = "silver"


def validate_silver(
    dataset_name: str,
    s3_prefix: str = None,
    dry_run: bool = False,
) -> dict:
    """
    Lee Silver desde S3, valida, retorna reporte.

    Args:
        dataset_name: 'co2_emissions', 'tourism_arrivals', 'transport_mode'
        s3_prefix: s3://bucket/silver/ (default construido)
        dry_run: Si True, lee local; si False, lee desde S3

    Returns:
        {
            "dataset": "co2_emissions",
            "layer": "silver",
            "timestamp": "2026-04-26T15:30:00Z",
            "total_checks": 5,
            "passed": 5,
            "failed": 0,
            "failures": [],
            "table_stats": {"rows": 209, "cols": 8},
            "summary": "5/5 checks OK"
        }
    """
    expectations = EXPECTATIONS.get("silver", {}).get(dataset_name)

    if not expectations:
        logger.warning(f"No expectations for silver.{dataset_name}")
        return None

    # Default S3 prefix
    if not s3_prefix:
        s3_prefix = f"s3://{BUCKET}/silver/{dataset_name}/"

    # Leer desde S3 o local
    try:
        df = read_parquet_s3(
            bucket=BUCKET,
            key_prefix=f"{LAYER}/{dataset_name}/data.parquet",
            local_fallback=f"../silver/{dataset_name}/sample.parquet",
            dry_run=dry_run,
        )
    except Exception as e:
        logger.error(f"Error leyendo Silver {dataset_name}: {e}")
        raise

    # Validar
    report = create_report(
        dataset=dataset_name,
        layer=LAYER,
        total_checks=len(expectations.get("checks", [])),
        passed=0,
        failed=0,
        failures=[],
        table_stats={"rows": len(df), "cols": len(df.columns)},
    )

    for check in expectations.get("checks", []):
        result = _run_check(df, check)
        if result["ok"]:
            report["passed"] += 1
        else:
            report["failed"] += 1
            report["failures"].append(result)

    # Actualizar summary
    if report["failed"] == 0:
        report["summary"] = f"{report['passed']}/{report['total_checks']} checks OK"
    else:
        report["summary"] = f"⚠️ {report['failed']}/{report['total_checks']} checks FAILED"

    # Guardar reporte
    timestamp_file = get_utc_date()
    report_key = f"quality_reports/{LAYER}_{dataset_name}_{timestamp_file}.json"

    if not dry_run:
        upload_report_s3(report, BUCKET, report_key, dry_run=False)
    else:
        logger.info(f"[DRY-RUN] Reporte que se subiría a {report_key}")

    # Log
    status_icon = "✓" if report["failed"] == 0 else "✗"
    logger.info(f"{status_icon} Silver {dataset_name}: {report['passed']}/{report['total_checks']} checks OK")

    return report


def _run_check(df: pd.DataFrame, check: dict) -> dict:
    """
    Ejecuta una validación individual usando funciones de utils.
    """
    check_type = check.get("type")

    if check_type == "table_row_count":
        return validate_table_row_count(
            df,
            expected=check.get("expected_value"),
            min_value=check.get("min_value"),
        )

    elif check_type == "column_values_to_not_be_null":
        return validate_column_not_null(
            df,
            column=check["column"],
            threshold_pct=check.get("threshold_pct", 0),
        )

    elif check_type == "no_duplicates":
        return validate_no_duplicates(
            df,
            subset=check.get("subset", ["country_code", "year"]),
        )

    elif check_type == "column_values_in_set":
        return validate_column_in_set(
            df,
            column=check["column"],
            value_set=set(check.get("value_set", [])),
        )

    elif check_type == "column_values_to_be_between":
        return validate_column_between(
            df,
            column=check["column"],
            min_value=check.get("min_value"),
            max_value=check.get("max_value"),
        )

    else:
        return {
            "ok": False,
            "check": check_type,
            "reason": f"Unknown check type: {check_type}",
        }
