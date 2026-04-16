"""
Fuente : UN Tourism (UNWTO) — Inbound tourism arrivals by transport mode
         (archivo Excel, estructura en formato largo / tidy data)

Estructura del dataset original:
  reporter_area_label  → país
  year                 → año
  indicator_label      → tipo de transporte (air, land, water, total)
  value                → cantidad de arribos

Nota:
  El dataset NO viene separado en columnas por transporte.
  El modo de transporte está codificado en `indicator_label`,
  por lo que requiere transformación (pivot) para obtener:

    tourists_air
    tourists_land
    tourists_sea
    tourists_total

Destino: s3://latam-sustainability-datalake/
  ├── raw/unwto_transport/unwto_transport.xlsx
  └── bronze/transport_mode/
        year=<Y>/country_code=<ISO3>/data.parquet

Nota sobre cobertura:
  Este dataset tiene cobertura parcial — muchos países LATAM
  no reportan todos los modos de transporte o tienen valores faltantes.
  Se registran warnings pero NO se aborta la ingesta.
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

from config import (
    COUNTRY_ISO3,
    LATAM_COUNTRIES,
    NAME_ALIASES,
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

logger = logging.getLogger("ingestion.unwto_transport")

# 🔴 DEFINIR LINK
UNWTO_TRANSPORT_URL = "https://pre-webunwto.s3.amazonaws.com/s3fs-public/2025-12/UN_Tourism_inbound_arrivals_by_transport_12_2025.xlsx"

OUTPUT_COLUMNS = [
    "country",
    "country_code",
    "year",
    "tourists_air",
    "tourists_sea",
    "tourists_land",
    "tourists_total",
    "pct_air",
    "pct_sea",
    "pct_land",
]


def download_raw(url: str) -> bytes:
    logger.info("⬇️ Descargando UNWTO Transport...")
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    return resp.content


def parse_and_filter(raw_bytes: bytes) -> pd.DataFrame:
    df_raw = pd.read_excel(BytesIO(raw_bytes), sheet_name="Data")

    logger.info("Excel original: %s", df_raw.shape)

    df = df_raw[
        df_raw["indicator_label"].str.contains("inbound", case=False, na=False)
    ].copy()

    def map_transport(x):
        x = str(x).lower()
        if "air" in x:
            return "air"
        if "land" in x:
            return "land"
        if "water" in x:
            return "sea"
        if "total" in x:
            return "total"
        return None

    df["transport_mode"] = df["indicator_label"].apply(map_transport)
    df = df[df["transport_mode"].notnull()].copy()

    df["country"] = df["reporter_area_label"].apply(
        lambda x: normalize_country_name(str(x), NAME_ALIASES)
    )

    df = df[df["country"].isin(LATAM_COUNTRIES)].copy()

    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    df = df[(df["year"] >= YEAR_START) & (df["year"] <= YEAR_END)].copy()

    df["year"] = df["year"].astype(int)

    df_pivot = df.pivot_table(
        index=["country", "year"],
        columns="transport_mode",
        values="value",
        aggfunc="sum",
    ).reset_index()

    df_pivot = df_pivot.rename(
        columns={
            "air": "tourists_air",
            "land": "tourists_land",
            "sea": "tourists_sea",
            "total": "tourists_total",
        }
    )

    for col in ["tourists_air", "tourists_land", "tourists_sea"]:
        if col not in df_pivot.columns:
            df_pivot[col] = float("nan")

    if "tourists_total" not in df_pivot.columns:
        df_pivot["tourists_total"] = df_pivot[
            ["tourists_air", "tourists_land", "tourists_sea"]
        ].sum(axis=1, min_count=1)

    df_pivot["country_code"] = df_pivot["country"].map(COUNTRY_ISO3)

    mask = df_pivot["tourists_total"] > 0

    for mode in ["air", "sea", "land"]:
        df_pivot[f"pct_{mode}"] = float("nan")
        df_pivot.loc[mask, f"pct_{mode}"] = (
            df_pivot.loc[mask, f"tourists_{mode}"]
            / df_pivot.loc[mask, "tourists_total"]
            * 100
        ).round(2)

    cols_final = [c for c in OUTPUT_COLUMNS if c in df_pivot.columns]

    df = (
        df_pivot[cols_final]
        .sort_values(["country_code", "year"])
        .reset_index(drop=True)
    )

    log_dataframe_summary(df, "UNWTO Transport")
    return df


def validate(df: pd.DataFrame) -> bool:
    if df.empty:
        logger.error("❌ DataFrame vacío")
        return False

    null_pct = df["tourists_total"].isnull().mean() * 100
    logger.info("Nulls tourists_total: %.1f%%", null_pct)

    if null_pct > 95:
        logger.error("❌ Dataset vacío")
        return False

    return True


def run(dry_run: bool = False):
    logger.info("🚀 Ingesta UNWTO Transport")

    raw_bytes = download_raw(UNWTO_TRANSPORT_URL)

    if not dry_run:
        upload_raw_to_s3(
            content=raw_bytes,
            s3_key=S3_PATHS["raw_transport"],
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    df = parse_and_filter(raw_bytes)

    if not validate(df):
        sys.exit(1)

    if not dry_run:
        upload_parquet_partitioned(
            df=df,
            s3_prefix=S3_PATHS["bronze_transport"],
            partition_cols=["year", "country_code"],
        )

    logger.info("✅ OK")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    run(dry_run=args.dry_run)
