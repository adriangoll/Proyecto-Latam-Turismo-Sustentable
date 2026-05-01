"""
transform_co2.py — Transformación Bronze → Silver para CO₂ & Emissions.
=======================================================================
Entrada : s3://...../bronze/co2_emissions/  (Parquet particionado)
Salida  : s3://...../silver/co2_emissions/data.parquet

Transformaciones aplicadas :
  1. Cast de tipos (apply_schema).
  2. Deduplicación por (country_code, year)
  3. Drop filas donde co2 + gdp + population son todos null
  4. Forward-fill población, interpolación lineal GDP (gaps pequeños)
  5. Métricas derivadas:
       co2_per_capita_calc  : co2 (Mt) / population * 1_000_000  → t CO₂ por persona
       co2_intensity_gdp    : co2 (Mt) * 1e9 / gdp (USD PPP)     → kg CO₂ por USD
       gdp_per_capita       : gdp / population
       gdp_growth_pct       : % cambio GDP año a año por país

Ejecución:
  python transform_co2.py
  python transform_co2.py --dry-run --local-bronze data/bronze/co2_emissions
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
    SCHEMA_CO2,
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

logger = logging.getLogger("silver.co2")

SILVER_S3_PATH = S3_SILVER["co2"] + "data.parquet"
DATASET_NAME = "co2_emissions"


def add_derived_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula métricas derivadas que serán clave para responder las preguntas de negocio:

    co2_per_capita_calc:
        Emisiones de CO₂ por habitante en toneladas.
        Permite comparar países de diferente tamaño poblacional.
        Pregunta: ¿Qué países tienen mayor huella per cápita?

    co2_intensity_gdp:
        Kilogramos de CO₂ por dólar de PIB generado.
        Cuanto menor, más eficiente energéticamente es la economía.
        Pregunta: ¿Qué países logran crecimiento con menor impacto ambiental?

    gdp_per_capita:
        PIB por habitante — proxy de nivel de desarrollo económico.

    gdp_growth_pct:
        Variación % del PIB respecto al año anterior, por país.
        Pregunta: ¿Cómo correlaciona el crecimiento económico con las emisiones?
    """
    logger.info("   Calculando métricas derivadas...")

    # co2 está en Mt (megatoneladas), population en personas
    # → co2_per_capita en toneladas por persona
    mask_pop = df["population"] > 0
    df["co2_per_capita_calc"] = None
    df.loc[mask_pop, "co2_per_capita_calc"] = (df.loc[mask_pop, "co2"] * 1_000_000 / df.loc[mask_pop, "population"]).round(4)

    # gdp en USD 2011 PPP, co2 en Mt → kg CO₂ por USD
    mask_gdp = df["gdp"] > 0
    df["co2_intensity_gdp"] = None
    df.loc[mask_gdp, "co2_intensity_gdp"] = (df.loc[mask_gdp, "co2"] * 1e9 / df.loc[mask_gdp, "gdp"]).round(6)

    # GDP per cápita
    df["gdp_per_capita"] = None
    df.loc[mask_pop & mask_gdp, "gdp_per_capita"] = (df.loc[mask_pop & mask_gdp, "gdp"] / df.loc[mask_pop & mask_gdp, "population"]).round(2)

    # GDP growth % — ordenar por país y año antes de calcular
    df = df.sort_values(["country_code", "year"]).reset_index(drop=True)
    df["gdp_growth_pct"] = (df.groupby("country_code")["gdp"].pct_change() * 100).round(2)

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

        df = read_bronze_s3(S3_BRONZE["co2"])

    df_before = df.copy()
    logger.info("   Bronze shape: %s", df.shape)

    # 2. Aplicar esquema (cast de tipos, verificar columnas requeridas)
    df = apply_schema(df, SCHEMA_CO2)

    # 3. Deduplicar
    df = deduplicate(df, DEDUP_KEY, DATASET_NAME)

    # 4. Eliminar filas sin ninguna métrica útil
    df = drop_empty_rows(df, SCHEMA_CO2["drop_null_if_all"], DATASET_NAME)

    # 5. Rellenar gaps en series temporales
    df = fill_gaps(
        df,
        fill_forward_cols=SCHEMA_CO2["fill_forward"],
        interpolate_cols=SCHEMA_CO2["interpolate"],
    )

    # 6. Métricas derivadas
    df = add_derived_metrics(df)

    # 7. Ordenar columnas — primero las claves, luego métricas originales, luego derivadas
    key_cols = ["country", "country_code", "year"]
    original_metrics = [
        c
        for c in df.columns
        if c
        in [
            "co2",
            "co2_per_capita",
            "co2_per_gdp",
            "cumulative_co2",
            "methane",
            "nitrous_oxide",
            "gdp",
            "population",
            "energy_per_capita",
            "share_global_co2",
        ]
    ]
    derived_metrics = [
        c
        for c in df.columns
        if c
        in [
            "co2_per_capita_calc",
            "co2_intensity_gdp",
            "gdp_per_capita",
            "gdp_growth_pct",
        ]
    ]
    other_cols = [c for c in df.columns if c not in key_cols + original_metrics + derived_metrics]
    df = df[key_cols + original_metrics + derived_metrics + other_cols]

    logger.info("   Silver shape: %s", df.shape)
    logger.info("   Columnas: %s", list(df.columns))

    # 8. Quality report
    report = build_quality_report(
        df_before=df_before,
        df_after=df,
        dataset_name=DATASET_NAME,
        thresholds=QUALITY_THRESHOLDS.get("co2", {}),
    )

    # 9. Escribir Silver
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
    parser = argparse.ArgumentParser(description="Transform CO2 Bronze → Silver")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--local-bronze",
        default=None,
        help="Ruta local Bronze para dry-run. Ej: data/bronze/co2_emissions",
    )
    args = parser.parse_args()
    run(dry_run=args.dry_run, local_bronze=args.local_bronze)
