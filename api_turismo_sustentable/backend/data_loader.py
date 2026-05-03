"""
data_loader.py
Lee los archivos Parquet Gold desde S3 al arrancar FastAPI.
Se carga una sola vez en memoria al inicio (lifespan).
"""

import os
import io
import logging
import boto3
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# ── Configuración S3 ──────────────────────────────────────────────────────────
S3_BUCKET        = os.getenv("S3_BUCKET", "latam-sustainability-datalake")
DIM_COUNTRY_KEY  = os.getenv("DIM_COUNTRY_KEY",  "gold/dim_country/data.parquet")
FACT_KEY         = os.getenv("FACT_KEY", "gold/fact_tourism_emissions/data.parquet")

# Dataframes globales (se llenan en load_data)
df_fact: pd.DataFrame = pd.DataFrame()
df_dim:  pd.DataFrame = pd.DataFrame()


def _read_parquet_from_s3(bucket: str, key: str) -> pd.DataFrame:
    """Lee un Parquet de S3 y lo retorna como DataFrame."""
    s3 = boto3.client(
        "s3",
        aws_access_key_id     = os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name           = os.getenv("AWS_REGION", "us-east-1"),
    )
    logger.info(f"Leyendo s3://{bucket}/{key} ...")
    response = s3.get_object(Bucket=bucket, Key=key)
    buffer   = io.BytesIO(response["Body"].read())
    df       = pd.read_parquet(buffer)
    logger.info(f"  → {len(df)} filas, columnas: {list(df.columns)}")
    return df


def load_data() -> None:
    """
    Carga los dos Parquet Gold en memoria.
    Llamar una sola vez desde el lifespan de FastAPI.
    """
    global df_fact, df_dim

    df_dim  = _read_parquet_from_s3(S3_BUCKET, DIM_COUNTRY_KEY)
    df_fact = _read_parquet_from_s3(S3_BUCKET, FACT_KEY)

    # Normalizar nombres de columna a minúsculas con guión bajo
    df_dim.columns  = [c.lower().replace(" ", "_") for c in df_dim.columns]
    df_fact.columns = [c.lower().replace(" ", "_") for c in df_fact.columns]

    # Asegurar tipos básicos
    if "year" in df_fact.columns:
        df_fact["year"] = pd.to_numeric(df_fact["year"], errors="coerce")

    logger.info("✅ Datos cargados en memoria.")
    logger.info(f"   dim_country  → {df_dim.shape}")
    logger.info(f"   fact_tourism → {df_fact.shape}")


def get_fact() -> pd.DataFrame:
    """Retorna el DataFrame de hechos (fact_tourism_emissions)."""
    return df_fact


def get_dim() -> pd.DataFrame:
    """Retorna el DataFrame de dimensión país."""
    return df_dim


def get_merged() -> pd.DataFrame:
    """
    Retorna fact + dim mergeados por country_code / iso_code (lo que exista).
    Útil para gráficos que necesitan nombre completo del país.
    """
    fact = df_fact.copy()
    dim  = df_dim.copy()

    # Detectar la columna de join automáticamente
    fact_cols = set(fact.columns)
    dim_cols  = set(dim.columns)

    join_candidates = [
        ("country_code", "country_code"),
        ("country_iso",  "country_iso"),
        ("iso_code",     "iso_code"),
        ("country_code", "iso_code"),
        ("iso_code",     "country_code"),
    ]

    join_pair = None
    for fc, dc in join_candidates:
        if fc in fact_cols and dc in dim_cols:
            join_pair = (fc, dc)
            break

    if join_pair:
        merged = fact.merge(
            dim,
            left_on  = join_pair[0],
            right_on = join_pair[1],
            how      = "left",
            suffixes = ("", "_dim"),
        )
    else:
        logger.warning("No se encontró columna de join entre fact y dim. Usando solo fact.")
        merged = fact.copy()

    return merged


def get_overview_stats() -> dict:
    """
    Calcula las 4 métricas para las cards del Home.
    Retorna dict con: n_countries, n_years, total_co2_mt, total_arrivals_m
    """
    fact = df_fact

    # Log de diagnóstico — ver columnas reales del Parquet
    logger.info(f"Columnas en fact_tourism: {list(fact.columns)}")

    # Candidatos en orden de prioridad (más específico primero)
    co2_col = _find_col(fact, [
        "co2_kt", "co2", "emissions_co2", "total_co2",
        "co2_emissions", "carbon_emissions", "co2_tourism",
        "co2_total", "emissions", "ghg",
    ])
    arrivals_col = _find_col(fact, [
        "arrivals", "international_arrivals", "tourist_arrivals",
        "inbound_arrivals", "total_arrivals", "tourists",
        "visitors", "arrivals_thousands",
    ])
    year_col    = _find_col(fact, ["year", "anio", "periodo"])
    country_col = _find_col(fact, ["country_code", "iso_code", "country", "iso3", "iso"])

    logger.info(f"  Mapeado → co2={co2_col}, arrivals={arrivals_col}, year={year_col}, country={country_col}")

    n_countries    = int(fact[country_col].nunique()) if country_col  else 0
    n_years        = int(fact[year_col].nunique())    if year_col     else 0
    total_co2      = float(fact[co2_col].sum())       if co2_col      else 0.0
    total_arrivals = float(fact[arrivals_col].sum())  if arrivals_col else 0.0

    logger.info(f"  Valores → total_co2={total_co2:.0f}, total_arrivals={total_arrivals:.0f}")

    # División: asumimos co2 en kt → dividir por 1_000 para Mt
    # Si el valor es muy pequeño ya estaba en Mt, no dividir
    co2_mt = total_co2 / 1_000 if total_co2 > 1_000 else total_co2

    # arrivals: si son millones ya, no dividir; si son miles, dividir por 1_000
    arr_m = total_arrivals / 1_000 if total_arrivals > 100_000 else total_arrivals

    return {
        "n_countries":      n_countries,
        "n_years":          n_years,
        "total_co2_mt":     round(co2_mt, 1),
        "total_arrivals_m": round(arr_m, 1),
        "co2_col":          co2_col,
        "arrivals_col":     arrivals_col,
        "year_col":         year_col,
        "country_col":      country_col,
    }


def _find_col(df: pd.DataFrame, candidates: list) -> str | None:
    """Busca la primera columna candidata que exista en el DataFrame."""
    for c in candidates:
        if c in df.columns:
            return c
    return None