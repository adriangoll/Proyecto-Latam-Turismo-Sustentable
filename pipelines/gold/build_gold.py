"""
build_gold.py — Construye las tablas Gold desde Silver
=======================================================
Entrada : silver/co2_emissions/  silver/tourism_arrivals/   silver/transport_mode/
Salida : gold/fact_tourism_emissions/data.parquet,
            gold/dim_country/data.parquet

fact_tourism_emissions:
    JOIN de los 3 datasets Silver por (country_code, year).
    Una fila por país × año con todas las métricas relevantes.
    Es la tabla base para todas las preguntas de negocio.

dim_country:
    Dimensión estática con metadata de cada país.
    ISO2, ISO3, nombre, región LATAM.

Ejecución:
    python build_gold.py
    python build_gold.py --dry-run
"""

import argparse
import logging
import os
import sys

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

from config_gold import (
    LATAM_COUNTRIES_META,
    S3_GOLD,
    S3_SILVER,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("gold.build")


# ─── Lectura Silver ───────────────────────────────────────────────────────────


def read_silver(path: str, label: str) -> pd.DataFrame:
    import pyarrow.dataset as ds
    import s3fs

    fs = s3fs.S3FileSystem()
    clean_path = path.replace("s3://", "")
    logger.info("📂 Leyendo Silver [%s] ...", label)

    dataset = ds.dataset(clean_path, filesystem=fs, format="parquet")
    table = dataset.to_table()

    dict_map = {
        pa.dictionary(pa.int32(), pa.int32()): pd.Int64Dtype(),
        pa.dictionary(pa.int32(), pa.int64()): pd.Int64Dtype(),
        pa.dictionary(pa.int32(), pa.string()): pd.StringDtype(),
        pa.dictionary(pa.int8(), pa.string()): pd.StringDtype(),
    }
    df = table.to_pandas(types_mapper=dict_map.get)

    if "year" in df.columns:
        df["year"] = df["year"].astype("int64")
    if "country_code" in df.columns:
        df["country_code"] = df["country_code"].astype(str)

    logger.info("   → %d filas, %d columnas", *df.shape)
    return df


def read_silver_local(path: str, label: str) -> pd.DataFrame:
    logger.info("📂 [LOCAL] Leyendo Silver [%s] desde %s ...", label, path)
    df = pd.read_parquet(path)
    logger.info("   → %d filas", len(df))
    return df


# ─── Construcción fact ────────────────────────────────────────────────────────


def build_fact(
    df_co2: pd.DataFrame,
    df_tourism: pd.DataFrame,
    df_transport: pd.DataFrame,
) -> pd.DataFrame:
    """
    JOIN outer de los 3 datasets Silver por (country_code, year).

    Outer join para no perder filas: si un país tiene CO2 pero no tourism
    (ej. Venezuela 2021) la fila igual aparece en Gold con nulls en las
    columnas de tourism. Esto es correcto — es una limitación de la fuente,
    no del pipeline.

    Columnas finales seleccionadas para Gold:
    Solo las métricas necesarias para responder las 5 preguntas de negocio.
    Las columnas redundantes o muy técnicas quedan en Silver para DS.
    """
    logger.info("🔗 Construyendo fact_tourism_emissions ...")

    # Columnas que llevamos de CO2
    co2_cols = [
        "country",
        "country_code",
        "year",
        "co2",
        "co2_per_capita",
        "co2_per_capita_calc",
        "co2_intensity_gdp",
        "gdp",
        "gdp_per_capita",
        "gdp_growth_pct",
        "population",
        "share_global_co2",
    ]
    co2_cols = [c for c in co2_cols if c in df_co2.columns]
    df_co2_slim = df_co2[co2_cols].copy()

    # Columnas que llevamos de Tourism
    tourism_cols = [
        "country_code",
        "year",
        "tourist_arrivals",
        "tourism_receipts_usd",
        "tourist_departures",
        "arrivals_growth_pct",
        "receipts_per_tourist",
    ]
    tourism_cols = [c for c in tourism_cols if c in df_tourism.columns]
    df_tourism_slim = df_tourism[tourism_cols].copy()

    # Columnas que llevamos de Transport
    transport_cols = [
        "country_code",
        "year",
        "tourists_air",
        "tourists_sea",
        "tourists_land",
        "tourists_total",
        "pct_air",
        "pct_sea",
        "pct_land",
        "dominant_transport",
    ]
    transport_cols = [c for c in transport_cols if c in df_transport.columns]
    df_transport_slim = df_transport[transport_cols].copy()

    # JOIN secuencial
    fact = pd.merge(
        df_co2_slim,
        df_tourism_slim,
        on=["country_code", "year"],
        how="outer",
    )
    fact = pd.merge(
        fact,
        df_transport_slim,
        on=["country_code", "year"],
        how="outer",
    )

    # Rellenar country donde quedó null tras outer join
    country_map = df_co2[["country_code", "country"]].drop_duplicates()
    country_map = dict(zip(country_map["country_code"], country_map["country"]))
    fact["country"] = fact.apply(
        lambda r: r["country"] if pd.notna(r.get("country")) else country_map.get(r["country_code"]),
        axis=1,
    )

    # Métrica derivada clave para Gold:
    # co2_per_tourist — cuánto CO2 se emite por cada turista llegado
    # Responde directamente: ¿qué países tienen mayor impacto por turista?
    mask = fact["tourist_arrivals"].notna() & (fact["tourist_arrivals"] > 0)
    fact["co2_per_tourist"] = None
    fact.loc[mask, "co2_per_tourist"] = (fact.loc[mask, "co2"] * 1_000_000 / fact.loc[mask, "tourist_arrivals"]).round(4)
    # Unidad: toneladas CO2 por turista

    # Clasificación de sostenibilidad
    # Cuadrante basado en GDP growth vs CO2 growth año a año
    # Verde: crece PIB, baja o estable CO2
    # Amarillo: crece PIB, sube CO2 (normal)
    # Rojo: baja PIB, sube CO2 (peor escenario)
    # Gris: datos insuficientes
    fact = fact.sort_values(["country_code", "year"]).reset_index(drop=True)
    fact["co2_growth_pct"] = (fact.groupby("country_code")["co2"].pct_change() * 100).round(2)

    def classify(row):
        gdp_g = row.get("gdp_growth_pct")
        co2_g = row.get("co2_growth_pct")
        if pd.isna(gdp_g) or pd.isna(co2_g):
            return "sin_datos"
        if gdp_g > 0 and co2_g <= 0:
            return "verde"  # crece economía, bajan emisiones
        if gdp_g > 0 and co2_g > 0:
            return "amarillo"  # crece todo — situación común
        if gdp_g <= 0 and co2_g > 0:
            return "rojo"  # cae economía pero suben emisiones
        return "gris"  # cae todo

    fact["sustainability_label"] = fact.apply(classify, axis=1)

    # Ordenar
    fact = fact.sort_values(["country_code", "year"]).reset_index(drop=True)

    logger.info("   → fact shape: %s", fact.shape)
    logger.info("   → columnas: %s", list(fact.columns))
    return fact


# ─── Construcción dim_country ─────────────────────────────────────────────────


def build_dim_country() -> pd.DataFrame:
    """
    Dimensión estática de países LATAM.
    No depende de S3 — viene de config_gold.py.
    """
    rows = []
    for iso3, meta in LATAM_COUNTRIES_META.items():
        rows.append(
            {
                "country_code": iso3,
                "country_code_iso2": meta["iso2"],
                "country_name": meta["name"],
                "region_latam": meta["region"],
            }
        )
    df = pd.DataFrame(rows).sort_values("country_code").reset_index(drop=True)
    logger.info("   → dim_country: %d países", len(df))
    return df


# ─── Escritura ────────────────────────────────────────────────────────────────


def write_parquet_s3(df: pd.DataFrame, s3_path: str) -> None:
    import s3fs

    fs = s3fs.S3FileSystem()
    path = s3_path.replace("s3://", "")
    logger.info("⬆️  Escribiendo → s3://%s", path)
    table = pa.Table.from_pandas(df, preserve_index=False)
    with fs.open(path, "wb") as f:
        pq.write_table(table, f, compression="snappy")
    logger.info("   → %d filas escritas", len(df))


def write_parquet_local(df: pd.DataFrame, local_path: str) -> None:
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    df.to_parquet(local_path, compression="snappy", index=False)
    logger.info("[DRY-RUN] Escrito en %s (%d filas)", local_path, len(df))


# ─── Runner ───────────────────────────────────────────────────────────────────


def run(dry_run: bool = False, local_silver: dict = None) -> dict:
    logger.info("=" * 60)
    logger.info("🚀 Build Gold")
    logger.info("   Dry-run: %s", dry_run)
    logger.info("=" * 60)

    # 1. Leer Silver
    if dry_run and local_silver:
        df_co2 = read_silver_local(local_silver["co2"], "CO2")
        df_tourism = read_silver_local(local_silver["tourism"], "Tourism")
        df_transport = read_silver_local(local_silver["transport"], "Transport")
    else:
        df_co2 = read_silver(S3_SILVER["co2"], "CO2")
        df_tourism = read_silver(S3_SILVER["tourism"], "Tourism")
        df_transport = read_silver(S3_SILVER["transport"], "Transport")

    # 2. Construir tablas Gold
    fact = build_fact(df_co2, df_tourism, df_transport)
    dim_country = build_dim_country()

    # 3. Escribir
    if dry_run:
        logger.info("\n[DRY-RUN] fact_tourism_emissions (primeras 5 filas):")
        logger.info("\n%s", fact.head(5).to_string())
        logger.info("\n[DRY-RUN] dim_country:")
        logger.info("\n%s", dim_country.to_string())
        write_parquet_local(fact, "data/gold/fact_tourism_emissions/data.parquet")
        write_parquet_local(dim_country, "data/gold/dim_country/data.parquet")
    else:
        write_parquet_s3(fact, S3_GOLD["fact"])
        write_parquet_s3(dim_country, S3_GOLD["dim_country"])

    logger.info("✅ Gold construido correctamente")
    return {"fact": fact, "dim_country": dim_country}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build Gold tables")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--local-silver-co2", default=None)
    parser.add_argument("--local-silver-tourism", default=None)
    parser.add_argument("--local-silver-transport", default=None)
    args = parser.parse_args()

    local_silver = None
    if args.local_silver_co2:
        local_silver = {
            "co2": args.local_silver_co2,
            "tourism": args.local_silver_tourism,
            "transport": args.local_silver_transport,
        }

    run(dry_run=args.dry_run, local_silver=local_silver)
