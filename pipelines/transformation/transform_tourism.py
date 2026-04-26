"""
transform_tourism.py — Transformación Bronze → Silver para World Bank Tourism
=============================================================================
Entrada : s3://...../bronze/tourism_arrivals/   (Parquet particionado)
Salida  : s3://...../silver/tourism_arrivals/data.parquet

Transformaciones aplicadas:
  1. Cast de tipos (apply_schema)
  2. Deduplicación por (country_code, year)
  3. Drop filas sin tourist_arrivals
  4. Métricas derivadas:
       arrivals_growth_pct : % crecimiento turistas año a año por país
       receipts_per_tourist: ingresos turísticos por turista llegado (USD)

Ejecución:
  python transform_tourism.py
  python transform_tourism.py --dry-run --local-bronze data/bronze/tourism_arrivals
"""

import argparse
import logging
import os
import sys

import pandas as pd

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

from config_silver import (
    DEDUP_KEY,
    QUALITY_THRESHOLDS,
    S3_QUALITY_REPORTS,
    S3_SILVER,
    SCHEMA_TOURISM,
)
from utils_silver import (
    apply_schema,
    build_quality_report,
    deduplicate,
    drop_empty_rows,
    fill_gaps,
    read_bronze_local,
    read_bronze_s3,
    save_quality_report_local,
    upload_quality_report,
    write_silver_local,
    write_silver_s3,
)

logger = logging.getLogger("silver.tourism")

SILVER_S3_PATH = S3_SILVER["tourism"] + "data.parquet"
DATASET_NAME = "tourism_arrivals"


def add_derived_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Métricas derivadas para responder preguntas de negocio:

    arrivals_growth_pct:
        % de crecimiento de turistas respecto al año anterior.
        Pregunta: ¿Existe relación entre el crecimiento del turismo y las emisiones?

    receipts_per_tourist:
        Ingresos turísticos en USD dividido por llegadas.
        Indica el "valor promedio" por turista — proxy de turismo de alto vs bajo impacto.
        Pregunta: ¿Qué países tienen turismo más sostenible (más ingresos, menos turistas)?
    """
    logger.info("   Calculando métricas derivadas...")

    df = df.sort_values(["country_code", "year"]).reset_index(drop=True)

    # Crecimiento % de llegadas año a año
    df["arrivals_growth_pct"] = (df.groupby("country_code")["tourist_arrivals"].pct_change() * 100).round(2)

    # Ingresos por turista
    mask = df["tourist_arrivals"] > 0
    df["receipts_per_tourist"] = None
    df.loc[mask, "receipts_per_tourist"] = (df.loc[mask, "tourism_receipts_usd"] / df.loc[mask, "tourist_arrivals"]).round(2)

    logger.info("   ✅ Métricas derivadas calculadas")
    return df


def run(dry_run: bool = False, local_bronze: str = None) -> pd.DataFrame:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    logger.info("=" * 60)
    logger.info("🔄 Transform Bronze → Silver: %s", DATASET_NAME)
    logger.info("   Dry-run: %s", dry_run)
    logger.info("=" * 60)

    # 1. Leer Bronze
    if dry_run and local_bronze:
        df = read_bronze_local(local_bronze)
    else:
        from config_silver import S3_BRONZE

        df = read_bronze_s3(S3_BRONZE["tourism"])

    df_before = df.copy()
    logger.info("   Bronze shape: %s", df.shape)

    # 2. Esquema
    df = apply_schema(df, SCHEMA_TOURISM)

    # 3. Deduplicar
    df = deduplicate(df, DEDUP_KEY, DATASET_NAME)

    # 4. Drop filas sin arrivals
    df = drop_empty_rows(df, SCHEMA_TOURISM["drop_null_if_all"], DATASET_NAME)

    # 5. Gaps (tourism no tiene fill/interpolate por decisión: no inventar $ turísticos)
    df = fill_gaps(df, [], [])

    # 6. Métricas derivadas
    df = add_derived_metrics(df)

    # 7. Ordenar columnas
    key_cols = ["country", "country_code", "year"]
    original_metrics = [
        c
        for c in df.columns
        if c
        in [
            "tourist_arrivals",
            "tourism_receipts_usd",
            "tourist_departures",
        ]
    ]
    derived_metrics = [
        c
        for c in df.columns
        if c
        in [
            "arrivals_growth_pct",
            "receipts_per_tourist",
        ]
    ]
    other_cols = [c for c in df.columns if c not in key_cols + original_metrics + derived_metrics]
    df = df[key_cols + original_metrics + derived_metrics + other_cols]

    logger.info("   Silver shape: %s", df.shape)

    # 8. Quality report
    report = build_quality_report(
        df_before=df_before,
        df_after=df,
        dataset_name=DATASET_NAME,
        thresholds=QUALITY_THRESHOLDS.get("tourism", {}),
    )

    # 9. Escribir
    if dry_run:
        logger.info("\n[DRY-RUN] Preview Silver (primeras 10 filas):")
        logger.info("\n%s", df.head(10).to_string())
        write_silver_local(df, f"data/silver/{DATASET_NAME}/data.parquet")
        save_quality_report_local(report, "data/quality_reports/silver", DATASET_NAME)
    else:
        write_silver_s3(df, SILVER_S3_PATH)
        upload_quality_report(report, S3_QUALITY_REPORTS, DATASET_NAME)

    logger.info("✅ Transform %s completado", DATASET_NAME)
    return df


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Transform Tourism Bronze → Silver")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--local-bronze", default=None)
    args = parser.parse_args()
    run(dry_run=args.dry_run, local_bronze=args.local_bronze)
