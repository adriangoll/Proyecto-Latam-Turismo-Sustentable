"""
utils_expectations.py — Utilidades reutilizables para Great Expectations
=========================================================================
Funciones helper para:
  • Leer Parquet desde S3
  • Escribir reportes JSON a S3
  • Timestamps UTC
  • Logging centralizado
  
Usado por: bronze_expectations.py, silver_expectations.py, gold_expectations.py
"""

import json
import logging
from datetime import datetime, timezone
from typing import Optional
import io
import boto3
import pandas as pd

logger = logging.getLogger("expectations.utils")


# ─── Timestamps ──────────────────────────────────────────────────────────────

def get_utc_now() -> str:
    """
    Retorna timestamp ISO 8601 con zona UTC.
    Reemplazo para datetime.utcnow() (deprecado en Python 3.12+).
    
    Returns:
        "2026-04-26T15:30:00Z"
    """
    return datetime.now(timezone.utc).isoformat(timespec='seconds').replace('+00:00', 'Z')


def get_utc_date() -> str:
    """
    Retorna fecha UTC para nombres de archivos.
    
    Returns:
        "2026-04-26"
    """
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


# ─── Lectura desde S3 ────────────────────────────────────────────────────────

def read_parquet_s3(
    bucket: str,
    key_prefix: str,
    local_fallback: Optional[str] = None,
    dry_run: bool = False,
) -> pd.DataFrame:
    """
    Lee un Parquet (particionado o no) desde S3 o fallback local.
    
    Args:
        bucket: Nombre del bucket S3 (ej: "latam-sustainability-datalake")
        key_prefix: Prefijo S3 (ej: "bronze/co2_emissions/" o "silver/co2_emissions/data.parquet")
        local_fallback: Ruta local si dry_run=True (ej: "../sample.parquet")
        dry_run: Si True, lee local; si False, lee desde S3
    
    Returns:
        pd.DataFrame
    
    Raises:
        FileNotFoundError: Si dry_run=True pero local_fallback no existe
        botocore.exceptions.ClientError: Si error de S3
    """
    if dry_run:
        if not local_fallback:
            raise ValueError("dry_run=True requiere local_fallback")
        logger.info("[DRY-RUN] Leyendo local: %s", local_fallback)
        try:
            return pd.read_parquet(local_fallback)
        except FileNotFoundError as e:
            logger.error("Archivo local no encontrado: %s", local_fallback)
            raise
    
    # Leer desde S3
    s3 = boto3.client("s3")
    
    try:
        logger.info("Leyendo desde S3: s3://%s/%s", bucket, key_prefix)
        
        # Si termina en .parquet, es un archivo único
        if key_prefix.endswith(".parquet"):
            obj = s3.get_object(Bucket=bucket, Key=key_prefix)
            return pd.read_parquet(io.BytesIO(obj["Body"].read()))
        
        # Si no termina en .parquet, es un prefijo (dataset particionado)
        # Usar s3fs + pyarrow.dataset para leer particiones
        import pyarrow.dataset as ds
        
        s3_uri = f"s3://{bucket}/{key_prefix}"
        dataset = ds.dataset(s3_uri, format="parquet")
        
        # ✅ FIX: types_mapper para resolver dictionary<int32> vs int64
        types_mapper = {
            "year": "int64",
            "country_code": "string",
        }
        
        df = dataset.to_pandas()
        
        # Forzar tipos si fuera necesario (después de leer)
        for col, target_type in types_mapper.items():
            if col in df.columns:
                if target_type == "int64" and df[col].dtype == "object":
                    df[col] = df[col].astype("int64", errors="coerce")
                elif target_type == "string" and df[col].dtype != "object":
                    df[col] = df[col].astype("object")
        
        logger.info("✓ Leído %d filas, %d columnas", len(df), len(df.columns))
        return df
    
    except Exception as e:
        logger.error("Error leyendo S3: %s", e)
        raise


# ─── Escritura a S3 ──────────────────────────────────────────────────────────

def upload_report_s3(
    report: dict,
    bucket: str,
    key: str,
    dry_run: bool = False,
) -> None:
    """
    Sube un reporte JSON a S3.
    
    Args:
        report: Diccionario con estructura de reporte
        bucket: Nombre del bucket S3
        key: Ruta completa en S3 (ej: "quality_reports/bronze_co2_2026-04-26.json")
        dry_run: Si True, solo logs; si False, sube realmente
    
    Returns:
        None
    
    Raises:
        botocore.exceptions.ClientError: Si error de S3
    """
    json_body = json.dumps(report, indent=2)
    
    if dry_run:
        logger.info("[DRY-RUN] Reporte que se subiría a s3://%s/%s", bucket, key)
        logger.info(json_body)
        return
    
    try:
        s3 = boto3.client("s3")
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json_body,
            ContentType="application/json",
        )
        logger.info("✓ Reporte subido: s3://%s/%s", bucket, key)
    except Exception as e:
        logger.error("Error subiendo reporte a S3: %s", e)
        raise


# ─── Construcción de reportes ────────────────────────────────────────────────

def create_report(
    dataset: str,
    layer: str,
    total_checks: int,
    passed: int,
    failed: int,
    failures: list,
    table_stats: dict,
) -> dict:
    """
    Construye estructura de reporte estándar.
    
    Args:
        dataset: "co2_emissions", "tourism_arrivals", "transport_mode"
        layer: "bronze", "silver", "gold"
        total_checks: Cantidad total de validaciones
        passed: Cantidad que pasaron
        failed: Cantidad que fallaron
        failures: Lista de dicts con detalles de fallas
        table_stats: {"rows": N, "cols": M}
    
    Returns:
        dict con estructura de reporte
    """
    if failed == 0:
        summary = f"{passed}/{total_checks} checks OK"
    else:
        # ✅ FIX: incluir total_checks en el summary (antes era solo f"⚠️ {failed} checks FAILED")
        summary = f"⚠️ {failed}/{total_checks} checks FAILED"

    return {
        "dataset": dataset,
        "layer": layer,
        "timestamp": get_utc_now(),
        "total_checks": total_checks,
        "passed": passed,
        "failed": failed,
        "failures": failures,
        "table_stats": table_stats,
        "summary": summary,
    }


# ─── Validaciones genéricas ─────────────────────────────────────────────────

def validate_table_row_count(
    df: pd.DataFrame,
    expected: Optional[int] = None,
    min_value: Optional[int] = None,
) -> dict:
    """
    Valida cantidad de filas.
    
    Args:
        df: DataFrame
        expected: Cantidad exacta esperada
        min_value: Mínimo aceptado
    
    Returns:
        {"ok": bool, "check": "table_row_count", "reason": str if failed}
    """
    actual = len(df)
    
    if expected is not None and actual != expected:
        return {
            "ok": False,
            "check": "table_row_count",
            "reason": f"Expected {expected} rows, got {actual}",
        }
    
    if min_value is not None and actual < min_value:
        return {
            "ok": False,
            "check": "table_row_count",
            "reason": f"Expected min {min_value} rows, got {actual}",
        }
    
    return {"ok": True, "check": "table_row_count"}


def validate_column_not_null(
    df: pd.DataFrame,
    column: str,
    threshold_pct: float = 0.0,
) -> dict:
    """
    Valida ausencia de nulos en una columna.
    
    Args:
        df: DataFrame
        column: Nombre de columna
        threshold_pct: Máximo % de nulos permitido (default 0 = 0 nulos permitidos)
    
    Returns:
        {"ok": bool, "check": "column_values_to_not_be_null", ...}
    """
    if column not in df.columns:
        return {
            "ok": False,
            "check": "column_values_to_not_be_null",
            "column": column,
            "reason": f"Column '{column}' not found in DataFrame",
        }
    
    null_count = df[column].isnull().sum()
    null_pct = (null_count / len(df)) * 100 if len(df) > 0 else 0
    
    if null_pct > threshold_pct:
        return {
            "ok": False,
            "check": "column_values_to_not_be_null",
            "column": column,
            "reason": f"{null_count} nulls ({null_pct:.1f}%) exceeds threshold {threshold_pct}%",
        }
    
    return {"ok": True, "check": "column_values_to_not_be_null", "column": column}


def validate_no_duplicates(
    df: pd.DataFrame,
    subset: list,
) -> dict:
    """
    Valida ausencia de duplicados en un subconjunto de columnas.
    
    Args:
        df: DataFrame
        subset: Lista de columnas ["country_code", "year"]
    
    Returns:
        {"ok": bool, "check": "no_duplicates", ...}
    """
    missing_cols = [c for c in subset if c not in df.columns]
    if missing_cols:
        return {
            "ok": False,
            "check": "no_duplicates",
            "reason": f"Columns not found: {missing_cols}",
        }
    
    dupes = df.duplicated(subset=subset, keep=False)
    dup_count = dupes.sum()
    
    if dup_count > 0:
        return {
            "ok": False,
            "check": "no_duplicates",
            "columns": subset,
            "reason": f"{dup_count} duplicate rows found",
        }
    
    return {"ok": True, "check": "no_duplicates", "columns": subset}


def validate_column_in_set(
    df: pd.DataFrame,
    column: str,
    value_set: set,
) -> dict:
    """
    Valida que valores de columna estén en un conjunto permitido.
    
    Args:
        df: DataFrame
        column: Nombre de columna
        value_set: Set de valores permitidos
    
    Returns:
        {"ok": bool, "check": "column_values_in_set", ...}
    """
    if column not in df.columns:
        return {
            "ok": False,
            "check": "column_values_in_set",
            "column": column,
            "reason": f"Column '{column}' not found",
        }
    
    invalid = df[~df[column].isin(value_set)][column].unique().tolist()
    
    if invalid:
        return {
            "ok": False,
            "check": "column_values_in_set",
            "column": column,
            "reason": f"Invalid values: {invalid[:5]}{'...' if len(invalid) > 5 else ''}",
        }
    
    return {"ok": True, "check": "column_values_in_set", "column": column}


def validate_column_between(
    df: pd.DataFrame,
    column: str,
    min_value: float,
    max_value: float,
) -> dict:
    """
    Valida que valores numéricos estén en rango.
    
    Args:
        df: DataFrame
        column: Nombre de columna
        min_value: Mínimo (inclusive)
        max_value: Máximo (inclusive)
    
    Returns:
        {"ok": bool, "check": "column_values_to_be_between", ...}
    """
    if column not in df.columns:
        return {
            "ok": False,
            "check": "column_values_to_be_between",
            "column": column,
            "reason": f"Column '{column}' not found",
        }
    
    out_of_range = df[
        ((df[column] < min_value) | (df[column] > max_value)) & (df[column].notnull())
    ]
    
    if len(out_of_range) > 0:
        return {
            "ok": False,
            "check": "column_values_to_be_between",
            "column": column,
            "reason": f"{len(out_of_range)} values outside [{min_value}, {max_value}]",
        }
    
    return {"ok": True, "check": "column_values_to_be_between", "column": column}