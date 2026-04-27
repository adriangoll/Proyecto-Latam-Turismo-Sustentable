import json
import logging
from datetime import datetime, timezone
import pandas as pd
import pyarrow.dataset as ds
import pyarrow as pa
import boto3
import io

from config_expectations import EXPECTATIONS

logger = logging.getLogger("validation.bronze")


def validate_bronze(dataset_name: str, s3_path: str, dry_run: bool = False) -> dict:
    """
    Lee Parquet desde S3, valida con GE, retorna reporte.

    Args:
        dataset_name: 'co2_emissions', 'tourism_arrivals', 'transport_mode'
        s3_path: s3://bucket/bronze/co2_emissions/
        dry_run: si True, lee local; si False, lee desde S3

    Returns:
        {
            "dataset": "co2_emissions",
            "timestamp": "2026-04-26T15:30:00Z",
            "total_checks": 7,
            "passed": 6,
            "failed": 1,
            "failures": [{"check": "column_values_in_set", "reason": "..."}],
            "table_stats": {"rows": 209, "cols": 8}
        }
    """
    expectations = EXPECTATIONS["bronze"].get(dataset_name, {})
    if not expectations:
        logger.warning(f"No expectations para {dataset_name}")
        return None

    # Leer parquet
    if dry_run:
        df = pd.read_parquet(f"../{dataset_name}/sample.parquet")
    else:
        # Bronze está particionado por año (year=2013/, year=2014/, ...)
        s3_uri = f"s3://latam-sustainability-datalake/bronze/{dataset_name}/"
        # Forzar year a int64 desde el inicio para evitar conflicto int32 vs int64
        schema_override = pa.schema([
            ("country", pa.string()),
            ("year", pa.int64()),
            ("co2", pa.float64()),
            ("co2_per_capita", pa.float64()),
            ("co2_per_gdp", pa.float64()),
            ("cumulative_co2", pa.float64()),
            ("methane", pa.float64()),
            ("nitrous_oxide", pa.float64()),
            ("gdp", pa.float64()),
            ("population", pa.float64()),
            ("energy_per_capita", pa.float64()),
            ("share_global_co2", pa.float64()),
            ("country_code", pa.string()),
        ])
        dataset = ds.dataset(s3_uri, format="parquet", partitioning="hive", schema=schema_override)
        df = dataset.to_table().to_pandas()
        

    # Validar
    timestamp_iso = datetime.now(timezone.utc).isoformat(timespec='seconds').replace('+00:00', 'Z')

    report = {
        "dataset": dataset_name,
        "timestamp": timestamp_iso,
        "total_checks": len(expectations["checks"]),
        "passed": 0,
        "failed": 0,
        "failures": [],
        "table_stats": {"rows": len(df), "cols": len(df.columns)}
    }

    for check in expectations["checks"]:
        result = _run_check(df, check)
        if result["ok"]:
            report["passed"] += 1
        else:
            report["failed"] += 1
            report["failures"].append(result)

    # Guardar reporte en S3
    timestamp_file = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    if not dry_run:
        # ✅ FIX: s3 definido acá, justo antes de usarlo
        s3 = boto3.client("s3")
        s3.put_object(
            Bucket="latam-sustainability-datalake",
            Key=f"quality_reports/bronze_{dataset_name}_{timestamp_file}.json",
            Body=json.dumps(report, indent=2)
        )

    logger.info(f"✓ Bronze {dataset_name}: {report['passed']}/{report['total_checks']} checks OK")
    return report


def _run_check(df: pd.DataFrame, check: dict) -> dict:
    """Ejecuta una validación individual."""
    check_type = check["type"]

    if check_type == "table_row_count":
        expected = check.get("expected_value")
        min_val = check.get("min_value")
        actual = len(df)

        if expected and actual != expected:
            return {"ok": False, "check": check_type, "reason": f"Expected {expected} rows, got {actual}"}
        if min_val and actual < min_val:
            return {"ok": False, "check": check_type, "reason": f"Expected min {min_val} rows, got {actual}"}
        return {"ok": True, "check": check_type}

    elif check_type == "column_count":
        expected = check["expected_value"]
        actual = len(df.columns)
        if actual != expected:
            return {"ok": False, "check": check_type, "reason": f"Expected {expected} cols, got {actual}"}
        return {"ok": True, "check": check_type}

    elif check_type == "column_values_to_not_be_null":
        col = check["column"]
        nulls = df[col].isnull().sum()
        if nulls > 0:
            return {"ok": False, "check": check_type, "column": col, "reason": f"{nulls} nulls found"}
        return {"ok": True, "check": check_type, "column": col}

    elif check_type == "column_values_in_set":
        col = check["column"]
        value_set = set(check["value_set"])
        invalid = df[~df[col].isin(value_set)][col].unique().tolist()
        if invalid:
            return {"ok": False, "check": check_type, "column": col,
                    "reason": f"Invalid values: {invalid[:5]}"}
        return {"ok": True, "check": check_type, "column": col}

    elif check_type == "column_values_type":
        col = check["column"]
        expected_type = check["expected_type"]
        try:
            if expected_type == "int":
                pd.to_numeric(df[col], errors="raise").astype(int)
            elif expected_type == "float":
                pd.to_numeric(df[col], errors="raise").astype(float)
            return {"ok": True, "check": check_type, "column": col}
        except Exception as e:
            return {"ok": False, "check": check_type, "column": col, "reason": str(e)[:100]}

    elif check_type == "column_values_to_be_between":
        col = check["column"]
        min_value = check.get("min_value")
        max_value = check.get("max_value")

        out_of_range = df[
            ((df[col] < min_value) | (df[col] > max_value)) & (df[col].notnull())
        ]

        if len(out_of_range) > 0:
            return {
                "ok": False,
                "check": check_type,
                "column": col,
                "reason": f"{len(out_of_range)} values outside [{min_value}, {max_value}]"
            }
        return {"ok": True, "check": check_type, "column": col}

    return {"ok": False, "check": check_type, "reason": "Unknown check type"}