"""
ingest_owid_co2.py — Ingesta del dataset CO₂ & Emissions de Our World in Data
==============================================================================
Fuente : https://github.com/owid/co2-data  (owid-co2-data.csv)
Destino: s3://latam-sustainability-datalake/
          ├── raw/owid_co2/owid-co2-data.csv          ← archivo original
          └── bronze/co2_emissions/
                year=<Y>/country_code=<ISO3>/data.parquet

Columnas seleccionadas para bronze:
  country, country_code, year,
  co2, co2_per_capita, co2_per_gdp,
  cumulative_co2,
  methane, nitrous_oxide,        ← gases complementarios
  gdp, population,
  energy_per_capita,
  share_global_co2               ← % del total mundial

Ejecución:
  python ingest_owid_co2.py
  python ingest_owid_co2.py --dry-run   # no sube a S3, solo valida localmente

Variables de entorno requeridas (o perfil AWS configurado):
  AWS_ACCESS_KEY_ID
  AWS_SECRET_ACCESS_KEY
  AWS_DEFAULT_REGION  (default: us-east-1)
"""

import argparse
import logging
import os as _os
import sys
import sys as _sys
from io import BytesIO

import pandas as pd
import requests

_HERE = _os.path.dirname(_os.path.abspath(__file__))
if _HERE not in _sys.path:
    _sys.path.insert(0, _HERE)

from config import (COUNTRY_ISO3, LATAM_COUNTRIES, NAME_ALIASES, OWID_CO2_URL,
                    S3_PATHS, YEAR_END, YEAR_START)
from utils import (log_dataframe_summary, normalize_country_name,
                   upload_parquet_partitioned, upload_raw_to_s3)

logger = logging.getLogger("ingestion.owid_co2")

# ─── Columnas que nos interesan del CSV original ───────────────────────────────
# OWID CO2 tiene ~79 columnas; tomamos solo las relevantes para el proyecto.
COLUMNS_KEEP = [
    "country",
    "year",
    "iso_code",  # ISO3 que trae OWID — lo usamos para validar
    "co2",  # Mt de CO₂ totales
    "co2_per_capita",  # t CO₂ por persona
    "co2_per_gdp",  # kg CO₂ por USD de PIB
    "cumulative_co2",  # CO₂ acumulado histórico (Mt)
    "methane",  # Emisiones metano (Mt CO₂eq)
    "nitrous_oxide",  # Óxido nitroso (Mt CO₂eq)
    "gdp",  # PIB (USD 2011 PPP)
    "population",
    "energy_per_capita",  # kWh per cápita
    "share_global_co2",  # % del CO₂ mundial
]


def download_raw(url: str) -> bytes:
    """Descarga el CSV de OWID y retorna los bytes crudos."""
    logger.info("⬇️  Descargando OWID CO2 desde %s ...", url)
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    logger.info("   → %d bytes descargados", len(resp.content))
    return resp.content


def parse_and_filter(raw_bytes: bytes) -> pd.DataFrame:
    """
    Parsea el CSV, filtra países LATAM y rango de años,
    agrega country_code (ISO3), y retiene columnas relevantes.
    """
    df = pd.read_csv(BytesIO(raw_bytes), low_memory=False)
    logger.info("   CSV original: %s", df.shape)

    # ── Normalizar nombres de país (aliases de fuente) ────────────────────────
    df["country"] = df["country"].apply(
        lambda x: normalize_country_name(x, NAME_ALIASES)
    )

    # ── Filtrar por países LATAM ──────────────────────────────────────────────
    df = df[df["country"].isin(LATAM_COUNTRIES)].copy()

    # ── Filtrar por rango de años ─────────────────────────────────────────────
    df = df[(df["year"] >= YEAR_START) & (df["year"] <= YEAR_END)].copy()

    # ── Seleccionar solo columnas disponibles (robustez ante cambios de schema) ─
    cols_available = [c for c in COLUMNS_KEEP if c in df.columns]
    cols_missing = set(COLUMNS_KEEP) - set(cols_available)
    if cols_missing:
        logger.warning("⚠️  Columnas no encontradas en fuente: %s", cols_missing)
    df = df[cols_available].copy()

    # ── Agregar country_code ISO3 desde nuestro mapping ──────────────────────
    # Preferimos nuestro mapping propio al iso_code de OWID para garantizar
    # consistencia entre datasets en los joins posteriores.
    df["country_code"] = df["country"].map(COUNTRY_ISO3)

    # Reemplazar iso_code original con el nuestro y dropear si existía
    if "iso_code" in df.columns:
        df = df.drop(columns=["iso_code"])

    # ── Tipos de datos ────────────────────────────────────────────────────────
    df["year"] = df["year"].astype(int)
    df["country_code"] = df["country_code"].astype(str)

    numeric_cols = [
        c for c in df.columns if c not in ("country", "country_code", "year")
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # ── Ordenar para mejor compresión Parquet ─────────────────────────────────
    df = df.sort_values(["country_code", "year"]).reset_index(drop=True)

    log_dataframe_summary(df, "OWID CO2 — post-filtro")
    return df


def validate(df: pd.DataFrame) -> bool:
    """Validaciones mínimas antes de subir a S3."""
    errors = []

    if df.empty:
        errors.append("DataFrame vacío")

    missing_countries = set(LATAM_COUNTRIES) - set(df["country"].unique())
    if missing_countries:
        logger.warning("⚠️  Países sin datos: %s", missing_countries)

    missing_years = set(range(YEAR_START, YEAR_END + 1)) - set(df["year"].unique())
    if missing_years:
        logger.warning("⚠️  Años sin datos: %s", sorted(missing_years))

    null_pct_co2 = df["co2"].isnull().mean() * 100
    if null_pct_co2 > 30:
        errors.append(f"co2 tiene {null_pct_co2:.1f}% nulos (umbral: 30%)")

    if errors:
        for e in errors:
            logger.error("❌ Validación fallida: %s", e)
        return False

    logger.info("✅ Validaciones OK")
    return True


def run(dry_run: bool = False) -> None:
    logger.info("=" * 60)
    logger.info("🚀 Iniciando ingesta OWID CO2")
    logger.info(
        "   Países: %d | Años: %d–%d | Dry-run: %s",
        len(LATAM_COUNTRIES),
        YEAR_START,
        YEAR_END,
        dry_run,
    )
    logger.info("=" * 60)

    # 1. Descarga
    raw_bytes = download_raw(OWID_CO2_URL)

    # 2. Subida a raw/ (archivo original intacto)
    if not dry_run:
        upload_raw_to_s3(
            content=raw_bytes,
            s3_key=S3_PATHS["raw_co2"],
            content_type="text/csv",
        )
    else:
        logger.info("[DRY-RUN] Saltando upload raw")

    # 3. Parseo y filtrado
    df = parse_and_filter(raw_bytes)

    # 4. Validación
    if not validate(df):
        logger.error("❌ Ingesta abortada por fallo de validación")
        sys.exit(1)

    # 5. Subida a bronze/ como Parquet particionado
    if not dry_run:
        n = upload_parquet_partitioned(
            df=df,
            s3_prefix=S3_PATHS["bronze_co2"],
            partition_cols=["year", "country_code"],
        )
        logger.info("✅ Ingesta CO2 completada | %d particiones", n)
    else:
        logger.info("[DRY-RUN] DataFrame listo para subir:")
        logger.info("\n%s", df.head(10).to_string())
        logger.info("   Shape final: %s", df.shape)
        logger.info("   Columnas: %s", list(df.columns))
        logger.info(
            "   Particiones estimadas: %d", df.groupby(["year", "country_code"]).ngroups
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingesta OWID CO2 → S3")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Ejecuta sin subir a S3. Útil para validar localmente.",
    )
    args = parser.parse_args()
    run(dry_run=args.dry_run)
