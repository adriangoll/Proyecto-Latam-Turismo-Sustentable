"""
utils_silver.py — Utilidades compartidas para transformaciones Silver
=====================================================================
Funciones:
  - read_bronze_s3()       : lee todos los Parquet de un prefijo S3
  - read_bronze_local()    : lee Parquet locales (para --dry-run / tests)
  - write_silver_s3()      : escribe Silver como Parquet único comprimido Snappy
  - write_silver_local()   : escribe Parquet local (para --dry-run)
  - apply_schema()         : cast de tipos según config
  - deduplicate()          : elimina duplicados por clave
  - drop_empty_rows()      : elimina filas donde todas las métricas son null
  - fill_gaps()            : ffill + interpolación por grupo país
  - build_quality_report() : genera dict con métricas de calidad
  - upload_quality_report(): sube el reporte JSON a S3
"""

import json
import logging
import os
from datetime import datetime, timezone

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger("silver.utils")


# ─── Lectura Bronze ───────────────────────────────────────────────────────────


def read_bronze_s3(s3_prefix: str) -> pd.DataFrame:
    """
    Lee todos los archivos Parquet bajo un prefijo S3 (con particiones).
    Soporta estructura year=X/country_code=Y/data.parquet

    Args:
        s3_prefix: ej. "s3://my-bucket/bronze/co2_emissions/"
    Returns:
        DataFrame concatenado con todos los datos
    """
    import s3fs

    fs = s3fs.S3FileSystem()
    # Remover prefijo s3:// para s3fs
    path = s3_prefix.replace("s3://", "")

    logger.info("📂 Leyendo Bronze desde s3://%s ...", path)
    try:
        dataset = pq.ParquetDataset(path, filesystem=fs)
        table = dataset.read()
        df = table.to_pandas()
        logger.info("   → %d filas, %d columnas leídas", *df.shape)
        return df
    except Exception as e:
        logger.error("❌ Error leyendo Bronze desde S3: %s", e)
        raise


def read_bronze_local(local_path: str) -> pd.DataFrame:
    """
    Lee Parquet desde una carpeta local (dry-run / tests).
    Soporta estructura plana o particionada.

    Args:
        local_path: ruta local, ej. "data/bronze/co2_emissions"
    """
    logger.info("📂 Leyendo Bronze local desde %s ...", local_path)
    if not os.path.exists(local_path):
        raise FileNotFoundError(f"Ruta local no encontrada: {local_path}")

    parquet_files = []
    for root, _dirs, files in os.walk(local_path):
        for f in files:
            if f.endswith(".parquet"):
                parquet_files.append(os.path.join(root, f))

    if not parquet_files:
        raise FileNotFoundError(f"No se encontraron archivos .parquet en {local_path}")

    dfs = [pd.read_parquet(f) for f in parquet_files]
    df = pd.concat(dfs, ignore_index=True)
    logger.info("   → %d filas, %d columnas leídas (%d archivos)", *df.shape, len(parquet_files))
    return df


# ─── Escritura Silver ─────────────────────────────────────────────────────────


def write_silver_s3(df: pd.DataFrame, s3_path: str) -> None:
    """
    Escribe el DataFrame Silver como un único Parquet comprimido con Snappy.
    Silver NO se particiona (datasets pequeños, 19 países × 11 años).

    Args:
        df:       DataFrame limpio y transformado
        s3_path:  path completo, ej. "s3://bucket/silver/co2_emissions/data.parquet"
    """
    import s3fs

    logger.info("⬆️  Escribiendo Silver → %s", s3_path)
    fs = s3fs.S3FileSystem()
    path = s3_path.replace("s3://", "")

    table = pa.Table.from_pandas(df, preserve_index=False)
    with fs.open(path, "wb") as f:
        pq.write_table(table, f, compression="snappy")

    logger.info("   → %d filas escritas", len(df))


def write_silver_local(df: pd.DataFrame, local_path: str) -> None:
    """
    Escribe Silver localmente (dry-run / tests).
    """
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    df.to_parquet(local_path, compression="snappy", index=False)
    logger.info("[DRY-RUN] Silver escrito en %s (%d filas)", local_path, len(df))


# ─── Transformaciones comunes ─────────────────────────────────────────────────


def apply_schema(df: pd.DataFrame, schema: dict) -> pd.DataFrame:
    """
    Aplica el esquema definido en config_silver:
      - Verifica columnas requeridas
      - Castea columnas numéricas
      - Castea year a int, country_code a str uppercase
    """
    # Verificar columnas requeridas
    missing = [c for c in schema["required"] if c not in df.columns]
    if missing:
        raise ValueError(f"Columnas requeridas faltantes en Bronze: {missing}")

    # Cast numérico
    for col in schema.get("numeric", []):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Tipos base
    df["year"] = df["year"].astype(int)
    df["country_code"] = df["country_code"].astype(str).str.upper().str.strip()
    df["country"] = df["country"].astype(str).str.strip()

    return df


def deduplicate(df: pd.DataFrame, key: list[str], dataset_name: str) -> pd.DataFrame:
    """
    Elimina duplicados por clave. Conserva la primera ocurrencia.
    Registra warning si había duplicados.
    """
    n_before = len(df)
    df = df.drop_duplicates(subset=key, keep="first").reset_index(drop=True)
    n_removed = n_before - len(df)
    if n_removed > 0:
        logger.warning("⚠️  [%s] %d duplicados eliminados por %s", dataset_name, n_removed, key)
    else:
        logger.info("   [%s] Sin duplicados detectados", dataset_name)
    return df


def drop_empty_rows(df: pd.DataFrame, cols: list[str], dataset_name: str) -> pd.DataFrame:
    """
    Elimina filas donde TODAS las columnas de métricas indicadas son null.
    Una fila con país y año pero sin ningún dato útil no aporta nada en Silver.
    """
    if not cols:
        return df

    # Solo operar sobre columnas que existen
    existing_cols = [c for c in cols if c in df.columns]
    if not existing_cols:
        return df

    mask_all_null = df[existing_cols].isnull().all(axis=1)
    n_dropped = mask_all_null.sum()
    if n_dropped > 0:
        logger.warning(
            "⚠️  [%s] %d filas eliminadas (todas las métricas son null en %s)",
            dataset_name,
            n_dropped,
            existing_cols,
        )
    df = df[~mask_all_null].reset_index(drop=True)
    return df


def fill_gaps(
    df: pd.DataFrame,
    fill_forward_cols: list[str],
    interpolate_cols: list[str],
    group_col: str = "country_code",
) -> pd.DataFrame:
    """
    Rellena gaps en series temporales por grupo país:
      - fill_forward_cols  : forward-fill (ej. población — no cambia bruscamente)
      - interpolate_cols   : interpolación lineal (ej. GDP — flujo continuo)

    IMPORTANTE: solo rellena NaN internos a la serie. No extrapola hacia el futuro
    más allá del último dato conocido para evitar inventar datos.
    """
    df = df.sort_values([group_col, "year"]).reset_index(drop=True)

    for col in fill_forward_cols:
        if col in df.columns:
            df[col] = df.groupby(group_col)[col].transform(lambda s: s.ffill())
            logger.debug("   ffill aplicado en '%s'", col)

    for col in interpolate_cols:
        if col in df.columns:
            df[col] = df.groupby(group_col)[col].transform(lambda s: s.interpolate(method="linear", limit_direction="forward"))
            logger.debug("   interpolación lineal aplicada en '%s'", col)

    return df


# ─── Quality Report ───────────────────────────────────────────────────────────


def build_quality_report(
    df_before: pd.DataFrame,
    df_after: pd.DataFrame,
    dataset_name: str,
    thresholds: dict,
) -> dict:
    """
    Genera un reporte de calidad comparando antes/después de las transformaciones.

    Returns:
        dict con métricas de calidad (serializable a JSON)
    """
    report = {
        "dataset": dataset_name,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "rows_bronze": int(len(df_before)),
        "rows_silver": int(len(df_after)),
        "rows_dropped": int(len(df_before) - len(df_after)),
        "countries_covered": sorted(df_after["country_code"].unique().tolist()),
        "years_covered": sorted(df_after["year"].unique().tolist()),
        "null_pct_by_column": {},
        "quality_flags": [],
    }

    # Nulls por columna (post-transform)
    for col in df_after.columns:
        null_pct = round(df_after[col].isnull().mean() * 100, 2)
        report["null_pct_by_column"][col] = null_pct

        # Verificar contra umbrales
        if col in thresholds:
            threshold = thresholds[col]
            if null_pct > threshold:
                flag = f"WARN: '{col}' tiene {null_pct}% nulls (umbral: {threshold}%)"
                report["quality_flags"].append(flag)
                logger.warning("⚠️  %s", flag)

    # Países faltantes
    from config_silver import LATAM_ISO3

    missing_countries = sorted(LATAM_ISO3 - set(df_after["country_code"].unique()))
    if missing_countries:
        report["missing_countries"] = missing_countries
        report["quality_flags"].append(f"Países sin datos: {missing_countries}")
        logger.warning("⚠️  Países sin datos en Silver [%s]: %s", dataset_name, missing_countries)

    # Años faltantes
    from config_silver import YEAR_END, YEAR_START

    all_years = set(range(YEAR_START, YEAR_END + 1))
    covered_years = set(df_after["year"].unique())
    missing_years = sorted(all_years - covered_years)
    if missing_years:
        report["missing_years"] = missing_years
        report["quality_flags"].append(f"Años sin datos: {missing_years}")

    if not report["quality_flags"]:
        report["quality_flags"].append("OK - sin alertas")

    return report


def upload_quality_report(report: dict, s3_prefix: str, dataset_name: str) -> None:
    """
    Sube el reporte de calidad como JSON a S3.
    Nombre del archivo: <dataset>_YYYY-MM-DD.json
    """
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    s3_key = f"{s3_prefix}{dataset_name}_{date_str}.json"
    s3_key = s3_key.replace("s3://", "")

    bucket = s3_key.split("/")[0]
    key = "/".join(s3_key.split("/")[1:])

    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(report, indent=2, default=str).encode("utf-8"),
        ContentType="application/json",
    )
    logger.info("📋 Quality report subido → s3://%s/%s", bucket, key)


def save_quality_report_local(report: dict, local_dir: str, dataset_name: str) -> None:
    """
    Guarda el reporte de calidad localmente (dry-run).
    """
    os.makedirs(local_dir, exist_ok=True)
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    path = os.path.join(local_dir, f"{dataset_name}_{date_str}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, default=str)
    logger.info("[DRY-RUN] Quality report guardado en %s", path)
