"""
ingest_owid_transport.py — Ingesta de Tourist Arrivals by Transport Mode (OWID)
================================================================================
Fuente : https://ourworldindata.org/grapher/tourist-arrivals-by-transport-mode.csv
         (CSV público, descarga directa sin autenticación)

Columnas del CSV original (varían según versión):
  Entity, Code, Year,
  Air transport (tourists),
  Sea transport (tourists),
  Land transport (tourists)

Destino: s3://latam-sustainability-datalake/
  ├── raw/owid_transport/tourist-arrivals-by-transport-mode.csv
  └── bronze/transport_mode/
        year=<Y>/country_code=<ISO3>/data.parquet

Nota sobre cobertura: Este dataset tiene menor cobertura que los otros dos —
muchos países LATAM no reportan desagregación por modo de transporte.
Lo registramos con warnings pero NO abortamos por eso.

Ejecución:
  python ingest_owid_transport.py
  python ingest_owid_transport.py --dry-run

Variables de entorno requeridas:
  AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION
"""

import argparse
import logging
import sys
from io import BytesIO

import pandas as pd
import requests

import os as _os
import sys as _sys
_HERE = _os.path.dirname(_os.path.abspath(__file__))
if _HERE not in _sys.path:
    _sys.path.insert(0, _HERE)

from config import (
    COUNTRY_ISO3,
    LATAM_COUNTRIES,
    NAME_ALIASES,
    OWID_TRANSPORT_URL,
    S3_PATHS,
    YEAR_END,
    YEAR_START,
)
from utils import (
    log_dataframe_summary,
    normalize_country_name,
    upload_parquet_partitioned,
    upload_raw_to_s3,
)

logger = logging.getLogger("ingestion.owid_transport")

# ─── Mapeo de columnas OWID → nombres internos ────────────────────────────────
# OWID puede cambiar los nombres de columna entre versiones del CSV.
# Manejamos ambas variantes conocidas.
COLUMN_MAPPING_CANDIDATES = [
    # Variante 1 (nombres largos actuales)
    {
        "Entity": "country",
        "Code": "iso_code_owid",
        "Year": "year",
        "Air transport (tourists)": "tourists_air",
        "Sea transport (tourists)": "tourists_sea",
        "Land transport (tourists)": "tourists_land",
    },
    # Variante 2 (nombres cortos alternativos)
    {
        "Entity": "country",
        "Code": "iso_code_owid",
        "Year": "year",
        "Air": "tourists_air",
        "Sea": "tourists_sea",
        "Land": "tourists_land",
    },
]

# Columnas finales garantizadas en el DataFrame de salida
OUTPUT_COLUMNS = [
    "country",
    "country_code",
    "year",
    "tourists_air",
    "tourists_sea",
    "tourists_land",
    "tourists_total",     # suma de los tres modos (calculado)
    "pct_air",            # porcentaje aéreo sobre total
    "pct_sea",
    "pct_land",
]


def download_raw(url: str) -> bytes:
    """Descarga el CSV de OWID Transport y retorna los bytes crudos."""
    logger.info("⬇️  Descargando OWID Transport desde %s ...", url)
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    logger.info("   → %d bytes descargados", len(resp.content))
    return resp.content


def detect_column_mapping(df: pd.DataFrame) -> dict:
    """
    Detecta qué variante de nombres de columna usa el CSV descargado.
    Prueba cada candidato hasta encontrar uno que haga match.
    """
    for mapping in COLUMN_MAPPING_CANDIDATES:
        if all(col in df.columns for col in mapping.keys()):
            logger.info("   ✅ Variante de columnas detectada: %s", list(mapping.keys()))
            return mapping

    # Si ningún candidato aplica, intentar detección flexible
    logger.warning("⚠️  Columnas no reconocidas: %s", list(df.columns))
    logger.warning("   Intentando detección flexible...")

    mapping = {"Entity": "country", "Code": "iso_code_owid", "Year": "year"}
    for col in df.columns:
        col_lower = col.lower()
        if "air" in col_lower:
            mapping[col] = "tourists_air"
        elif "sea" in col_lower or "water" in col_lower or "maritime" in col_lower:
            mapping[col] = "tourists_sea"
        elif "land" in col_lower or "road" in col_lower:
            mapping[col] = "tourists_land"

    return mapping


def parse_and_filter(raw_bytes: bytes) -> pd.DataFrame:
    """
    Parsea el CSV, detecta columnas, filtra LATAM y calcula métricas derivadas.
    """
    df_raw = pd.read_csv(BytesIO(raw_bytes), low_memory=False)
    logger.info("   CSV original: %s", df_raw.shape)
    logger.info("   Columnas: %s", list(df_raw.columns))

    # ── Detectar y renombrar columnas ─────────────────────────────────────────
    col_mapping = detect_column_mapping(df_raw)
    df = df_raw.rename(columns=col_mapping).copy()

    # Verificar columnas mínimas
    required = {"country", "year"}
    if not required.issubset(df.columns):
        logger.error("❌ Columnas mínimas no encontradas. Columnas disponibles: %s", list(df.columns))
        raise ValueError("Formato de CSV inesperado")

    # ── Normalizar nombres de país ────────────────────────────────────────────
    df["country"] = df["country"].apply(
        lambda x: normalize_country_name(str(x), NAME_ALIASES)
    )

    # ── Filtrar por países LATAM ──────────────────────────────────────────────
    df = df[df["country"].isin(LATAM_COUNTRIES)].copy()

    # ── Filtrar por rango de años ─────────────────────────────────────────────
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df = df[(df["year"] >= YEAR_START) & (df["year"] <= YEAR_END)].copy()
    df["year"] = df["year"].astype(int)

    # ── Agregar country_code ──────────────────────────────────────────────────
    df["country_code"] = df["country"].map(COUNTRY_ISO3)

    # Dropear columna original de OWID (puede diferir del nuestro)
    if "iso_code_owid" in df.columns:
        df = df.drop(columns=["iso_code_owid"])

    # ── Tipos numéricos ───────────────────────────────────────────────────────
    for col in ["tourists_air", "tourists_sea", "tourists_land"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        else:
            # Si la columna no existe en esta versión del dataset, la creamos como null
            logger.warning("   ⚠️ Columna '%s' no encontrada, se crea como NaN", col)
            df[col] = float("nan")

    # ── Métricas derivadas ────────────────────────────────────────────────────
    # Total de turistas por todos los modos (suma ignorando NaN)
    df["tourists_total"] = df[["tourists_air", "tourists_sea", "tourists_land"]].sum(
        axis=1, min_count=1  # retorna NaN si TODOS son NaN
    )

    # Porcentaje por modo (solo donde hay total > 0)
    mask = df["tourists_total"] > 0
    for mode in ["air", "sea", "land"]:
        col_abs = f"tourists_{mode}"
        col_pct = f"pct_{mode}"
        df[col_pct] = float("nan")
        df.loc[mask, col_pct] = (
            df.loc[mask, col_abs] / df.loc[mask, "tourists_total"] * 100
        ).round(2)

    # ── Seleccionar columnas finales disponibles ───────────────────────────────
    cols_final = [c for c in OUTPUT_COLUMNS if c in df.columns]
    df = df[cols_final].sort_values(["country_code", "year"]).reset_index(drop=True)

    log_dataframe_summary(df, "OWID Transport — post-filtro")
    return df


def validate(df: pd.DataFrame) -> bool:
    """
    Validación permisiva: este dataset tiene cobertura parcial,
    así que solo reportamos warnings sin abortar (salvo vacío total).
    """
    if df.empty:
        logger.error("❌ DataFrame vacío — no hay datos de transporte para LATAM")
        return False

    countries_with_data = df["country"].unique()
    countries_no_data = set(LATAM_COUNTRIES) - set(countries_with_data)

    if countries_no_data:
        logger.warning(
            "⚠️  %d países sin datos de transporte (normal en este dataset): %s",
            len(countries_no_data), sorted(countries_no_data),
        )

    null_pct_total = df["tourists_total"].isnull().mean() * 100
    logger.info(
        "📊 tourists_total: %.1f%% nulos (esperado alto por cobertura parcial)",
        null_pct_total,
    )

    # Solo abortamos si hay más del 95% de nulos en tourists_total
    if null_pct_total > 95:
        logger.error("❌ tourists_total tiene %.1f%% nulos — dataset prácticamente vacío", null_pct_total)
        return False

    logger.info("✅ Validaciones OK (con advertencias de cobertura)")
    return True


def run(dry_run: bool = False) -> None:
    logger.info("=" * 60)
    logger.info("🚀 Iniciando ingesta OWID Transport Mode")
    logger.info("   Países: %d | Años: %d–%d | Dry-run: %s",
                len(LATAM_COUNTRIES), YEAR_START, YEAR_END, dry_run)
    logger.info("=" * 60)

    # 1. Descarga
    raw_bytes = download_raw(OWID_TRANSPORT_URL)

    # 2. Subida a raw/
    if not dry_run:
        upload_raw_to_s3(
            content=raw_bytes,
            s3_key=S3_PATHS["raw_transport"],
            content_type="text/csv",
        )
    else:
        logger.info("[DRY-RUN] Saltando upload raw")

    # 3. Parseo y filtrado
    df = parse_and_filter(raw_bytes)

    # 4. Validación
    if not validate(df):
        logger.error("❌ Ingesta abortada")
        sys.exit(1)

    # 5. Subida a bronze/
    if not dry_run:
        n = upload_parquet_partitioned(
            df=df,
            s3_prefix=S3_PATHS["bronze_transport"],
            partition_cols=["year", "country_code"],
        )
        logger.info("✅ Ingesta Transport completada | %d particiones", n)
    else:
        logger.info("[DRY-RUN] DataFrame final listo para subir:")
        logger.info("\n%s", df.head(10).to_string())
        logger.info("   Shape: %s | Columnas: %s", df.shape, list(df.columns))
        logger.info("   Particiones estimadas: %d",
                    df.dropna(subset=["country_code"]).groupby(["year", "country_code"]).ngroups)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingesta OWID Transport Mode → S3")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Ejecuta sin subir a S3.",
    )
    args = parser.parse_args()
    run(dry_run=args.dry_run)