"""
gold_expectations.py — Validaciones para la capa Gold
====================================================
Valida integridad relacional de tablas Gold:
  • fact_tourism_emissions: JOIN de 3 silver, no orphans, no duplicados
  • dim_country: 19 países LATAM, ISO2/ISO3 validos

Ejecutar:
  python run_validation.py --layer gold
"""

import logging
import os
import sys

import pandas as pd

_airflow_home = os.getenv("AIRFLOW_HOME", "/opt/airflow")
_expectations_path = os.path.join(_airflow_home, "pipelines", "expectations")
if _expectations_path not in sys.path:
    sys.path.insert(0, _expectations_path)

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

logger = logging.getLogger("validation.gold")

# ─── Constantes ──────────────────────────────────────────────────────────────

BUCKET = "latam-sustainability-datalake"
LAYER = "gold"

LATAM_COUNTRIES_ISO3 = [
    "ARG",
    "BOL",
    "BRA",
    "CHL",
    "COL",
    "CRI",
    "CUB",
    "DOM",
    "ECU",
    "SLV",
    "GTM",
    "HND",
    "MEX",
    "NIC",
    "PAN",
    "PRY",
    "PER",
    "URY",
    "VEN",
]


def validate_gold(dry_run: bool = False) -> dict:
    """
    Valida las tablas Gold:
      1. fact_tourism_emissions
      2. dim_country

    Retorna dict con reportes agregados.
    """
    logger.info("=" * 60)
    logger.info("🚀 Validando capa Gold")
    logger.info("=" * 60)

    reports = {}

    # Validar fact_tourism_emissions
    logger.info("\n→ Validando fact_tourism_emissions...")
    reports["fact"] = validate_fact_tourism_emissions(dry_run)

    # Validar dim_country
    logger.info("\n→ Validando dim_country...")
    reports["dim"] = validate_dim_country(dry_run)

    return reports


def validate_fact_tourism_emissions(dry_run: bool = False) -> dict:
    """
    Valida fact_tourism_emissions:
      • No nulos en country_code, year
      • No duplicados (country_code, year)
      • co2_per_tourist en rango razonable [0, 10000]
      • sustainability_label in {high, medium, low}
      • Número mínimo de filas (outer join de 3 silver)
    """
    expectations = EXPECTATIONS.get("gold", {}).get("fact_tourism_emissions", {})

    if not expectations:
        logger.warning("No expectations para gold.fact_tourism_emissions")
        return None

    # Leer desde S3
    try:
        df = read_parquet_s3(
            bucket=BUCKET,
            key_prefix=f"{LAYER}/fact_tourism_emissions/data.parquet",
            local_fallback="../gold/fact_tourism_emissions/sample.parquet",
            dry_run=dry_run,
        )
    except Exception as e:
        logger.error(f"Error leyendo fact_tourism_emissions: {e}")
        raise

    # Crear reporte
    report = create_report(
        dataset="fact_tourism_emissions",
        layer=LAYER,
        total_checks=len(expectations.get("checks", [])),
        passed=0,
        failed=0,
        failures=[],
        table_stats={"rows": len(df), "cols": len(df.columns)},
    )

    # Ejecutar checks
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
    report_key = f"quality_reports/{LAYER}_fact_tourism_emissions_{timestamp_file}.json"

    if not dry_run:
        upload_report_s3(report, BUCKET, report_key, dry_run=False)
    else:
        logger.info(f"[DRY-RUN] Reporte que se subiría a {report_key}")

    status_icon = "✓" if report["failed"] == 0 else "✗"
    logger.info(f"{status_icon} fact_tourism_emissions: {report['passed']}/{report['total_checks']} checks OK")

    return report


def validate_dim_country(dry_run: bool = False) -> dict:
    """
    Valida dim_country:
      • 19 filas (19 países LATAM)
      • country_code en {ARG, BRA, CHL, ...}
      • country_code no nulos
      • region = "Latin America" para todos
    """
    expectations = EXPECTATIONS.get("gold", {}).get("dim_country", {})

    if not expectations:
        logger.warning("No expectations para gold.dim_country")
        return None

    # Leer desde S3
    try:
        df = read_parquet_s3(
            bucket=BUCKET,
            key_prefix=f"{LAYER}/dim_country/data.parquet",
            local_fallback="../gold/dim_country/sample.parquet",
            dry_run=dry_run,
        )
    except Exception as e:
        logger.error(f"Error leyendo dim_country: {e}")
        raise

    # Crear reporte
    report = create_report(
        dataset="dim_country",
        layer=LAYER,
        total_checks=len(expectations.get("checks", [])),
        passed=0,
        failed=0,
        failures=[],
        table_stats={"rows": len(df), "cols": len(df.columns)},
    )

    # Ejecutar checks
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
    report_key = f"quality_reports/{LAYER}_dim_country_{timestamp_file}.json"

    if not dry_run:
        upload_report_s3(report, BUCKET, report_key, dry_run=False)
    else:
        logger.info(f"[DRY-RUN] Reporte que se subiría a {report_key}")

    status_icon = "✓" if report["failed"] == 0 else "✗"
    logger.info(f"{status_icon} dim_country: {report['passed']}/{report['total_checks']} checks OK")

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
