"""
transform_transport.py — Transformación Bronze → Silver para UNWTO Transport
=============================================================================
Entrada : s3://...../bronze/transport_mode/   (Parquet particionado)
Salida  : s3://...../silver/transport_mode/data.parquet

Nota sobre cobertura:
  Este dataset tiene cobertura parcial — muchos países LATAM no reportan
  todos los modos o tienen años faltantes. Silver conserva esos nulls con
  documentación explícita en el quality report. NO se imputan valores de
  modo de transporte porque imputar % de transporte sería inventar datos.

Transformaciones aplicadas:
  1. Cast de tipos
  2. Deduplicación
  3. Drop filas donde los 3 modos son null (fila inútil)
  4. Recalcular tourists_total si está null pero hay modos disponibles
  5. Recalcular pct_air / pct_sea / pct_land para consistencia

Ejecución:
  python transform_transport.py
  python transform_transport.py --dry-run --local-bronze data/bronze/transport_mode
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
    SCHEMA_TRANSPORT,
)
from utils_silver import (
    apply_schema,
    build_quality_report,
    deduplicate,
    drop_empty_rows,
    read_bronze_local,
    read_bronze_s3,
    save_quality_report_local,
    upload_quality_report,
    write_silver_local,
    write_silver_s3,
)

logger = logging.getLogger("silver.transport")

SILVER_S3_PATH = S3_SILVER["transport"] + "data.parquet"
DATASET_NAME = "transport_mode"


def recalculate_totals_and_pcts(df: pd.DataFrame) -> pd.DataFrame:
    """
    Recalcula tourists_total y los porcentajes modales para garantizar
    consistencia interna del dataset.

    Lógica:
      - Si tourists_total es null pero hay al menos un modo con dato → sumar los disponibles
      - pct_air/sea/land se recalculan siempre desde los valores finales de tourists_total
        para evitar porcentajes que no suman 100%

    Pregunta de negocio que responde:
      ¿Qué medios de transporte turístico tienen mayor impacto ambiental?
    """
    logger.info("   Recalculando totales y porcentajes modales...")

    mode_cols = ["tourists_air", "tourists_sea", "tourists_land"]
    existing_modes = [c for c in mode_cols if c in df.columns]

    # Recalcular total donde sea null pero haya algún modo disponible
    if "tourists_total" in df.columns and existing_modes:
        mask_null_total = df["tourists_total"].isnull()
        if mask_null_total.any():
            recalc = df.loc[mask_null_total, existing_modes].sum(axis=1, min_count=1)
            df.loc[mask_null_total, "tourists_total"] = recalc
            logger.info(
                "   → tourists_total recalculado en %d filas", mask_null_total.sum()
            )

    # Recalcular porcentajes desde el total final
    mask_total_ok = df["tourists_total"].notna() & (df["tourists_total"] > 0)
    for mode, pct_col in [
        ("tourists_air", "pct_air"),
        ("tourists_sea", "pct_sea"),
        ("tourists_land", "pct_land"),
    ]:
        if mode in df.columns:
            df[pct_col] = None
            df.loc[mask_total_ok, pct_col] = (
                df.loc[mask_total_ok, mode]
                / df.loc[mask_total_ok, "tourists_total"]
                * 100
            ).round(2)

    # Clasificar modo dominante (útil para análisis en Gold)
    pct_cols = {
        "pct_air": "air",
        "pct_sea": "sea",
        "pct_land": "land",
    }
    available_pct = {k: v for k, v in pct_cols.items() if k in df.columns}

    if available_pct:
        pct_df = df[[c for c in available_pct.keys()]].copy()
        all_null_mask = pct_df.isnull().all(axis=1)

        # idxmax explota si alguna fila tiene todos nulls → aplicar solo donde hay datos
        dominant = pd.Series([None] * len(df), dtype=object)
        has_data = ~all_null_mask
        if has_data.any():
            dominant[has_data] = (
                pct_df[has_data]
                .idxmax(axis=1)
                .map({k: v for k, v in available_pct.items()})
            )
        df["dominant_transport"] = dominant

    logger.info("   ✅ Totales y porcentajes recalculados")
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
        df = read_bronze_s3(S3_BRONZE["transport"])

    df_before = df.copy()
    logger.info("   Bronze shape: %s", df.shape)

    # 2. Esquema
    df = apply_schema(df, SCHEMA_TRANSPORT)

    # 3. Deduplicar
    df = deduplicate(df, DEDUP_KEY, DATASET_NAME)

    # 4. Drop filas sin ningún modo de transporte
    df = drop_empty_rows(df, SCHEMA_TRANSPORT["drop_null_if_all"], DATASET_NAME)

    # 5. Recalcular totales y porcentajes
    df = recalculate_totals_and_pcts(df)

    # 6. Ordenar columnas
    key_cols = ["country", "country_code", "year"]
    transport_cols = [c for c in df.columns if c in [
        "tourists_air", "tourists_sea", "tourists_land", "tourists_total",
        "pct_air", "pct_sea", "pct_land", "dominant_transport",
    ]]
    other_cols = [c for c in df.columns if c not in key_cols + transport_cols]
    df = df[key_cols + transport_cols + other_cols]

    logger.info("   Silver shape: %s", df.shape)

    # 7. Quality report
    report = build_quality_report(
        df_before=df_before,
        df_after=df,
        dataset_name=DATASET_NAME,
        thresholds=QUALITY_THRESHOLDS.get("transport", {}),
    )

    # Nota especial en el reporte: cobertura parcial es esperada
    report["note"] = (
        "Dataset UNWTO tiene cobertura parcial por diseño. "
        "Nulls en modos de transporte son esperados y documentados."
    )

    # 8. Escribir
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
    parser = argparse.ArgumentParser(description="Transform Transport Bronze → Silver")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--local-bronze", default=None)
    args = parser.parse_args()
    run(dry_run=args.dry_run, local_bronze=args.local_bronze)