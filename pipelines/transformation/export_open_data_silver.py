"""
export_open_data_silver.py — Exportación Silver → open-data/v1/silver/
=======================================================================
Lee los 3 datasets Silver desde S3 y los exporta a open-data/v1/silver/
en dos formatos:
  - Parquet (para Data Scientists — lectura rápida, tipado)
  - CSV     (para periodistas, investigadores, público general)

Por cada dataset genera también:
  - metadata.json     → descripción legible por máquinas.
  - data_dictionary.md → descripción legible por humanos

Estructura de salida :
  s3://bucket/open-data/v1/silver/
    co2_emissions/
      latam_co2_emissions_v1.parquet
      latam_co2_emissions_v1.csv
      metadata.json
      data_dictionary.md
    tourism_arrivals/
      latam_tourism_arrivals_v1.parquet
      latam_tourism_arrivals_v1.csv
      metadata.json
      data_dictionary.md
    transport_mode/
      latam_transport_mode_v1.parquet
      latam_transport_mode_v1.csv
      metadata.json
      data_dictionary.md

Ejecución:
  python export_open_data_silver.py
  python export_open_data_silver.py --dry-run
  python export_open_data_silver.py --version v2   # si hubiera cambio de schema
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone
from io import BytesIO, StringIO

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

from config_silver import S3_BUCKET, S3_SILVER

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("export.open_data_silver")

# ─── Metadatos de cada dataset ────────────────────────────────────────────────
# Editá estos dicts si cambia el schema o la fuente.

DATASET_META = {
    "co2_emissions": {
        "title": "CO₂ Emissions & Economic Indicators — Latin America",
        "description": (
            "Annual CO₂ emissions, GDP, population and derived environmental metrics "
            "for 19 Latin American countries (2013–2023) . "
            "Source: Our World in Data (OWID CO₂ dataset). "
            "Cleaned, normalized and enriched with derived metrics by the LATAM Sustainability project."
        ),
        "source": "Our World in Data — https://github.com/owid/co2-data",
        "columns": {
            "country": {
                "type": "string",
                "unit": "—",
                "description": "Country name (English)",
            },
            "country_code": {
                "type": "string",
                "unit": "ISO 3166-1 α3",
                "description": "e.g. ARG, BRA, MEX",
            },
            "year": {
                "type": "integer",
                "unit": "—",
                "description": "Reference year (2013–2023)",
            },
            "co2": {
                "type": "float",
                "unit": "Mt CO₂",
                "description": "Total CO₂ emissions",
            },
            "co2_per_capita": {
                "type": "float",
                "unit": "t CO₂/person",
                "description": "CO₂ per capita (OWID original)",
            },
            "co2_per_gdp": {
                "type": "float",
                "unit": "kg CO₂/USD",
                "description": "CO₂ intensity of GDP (OWID original)",
            },
            "cumulative_co2": {
                "type": "float",
                "unit": "Mt CO₂",
                "description": "Cumulative historical CO₂",
            },
            "methane": {
                "type": "float",
                "unit": "Mt CO₂eq",
                "description": "Methane emissions",
            },
            "nitrous_oxide": {
                "type": "float",
                "unit": "Mt CO₂eq",
                "description": "Nitrous oxide emissions",
            },
            "gdp": {
                "type": "float",
                "unit": "USD 2011 PPP",
                "description": "Gross domestic product",
            },
            "population": {
                "type": "integer",
                "unit": "persons",
                "description": "Mid-year population estimate",
            },
            "energy_per_capita": {
                "type": "float",
                "unit": "kWh/person",
                "description": "Energy consumption per capita",
            },
            "share_global_co2": {
                "type": "float",
                "unit": "%",
                "description": "Share of global CO₂ emissions",
            },
            "co2_per_capita_calc": {
                "type": "float",
                "unit": "t CO₂/person",
                "description": "Derived: co2 × 1e6 / population",
            },
            "co2_intensity_gdp": {
                "type": "float",
                "unit": "kg CO₂/USD",
                "description": "Derived: co2 × 1e9 / gdp",
            },
            "gdp_per_capita": {
                "type": "float",
                "unit": "USD PPP/person",
                "description": "Derived: gdp / population",
            },
            "gdp_growth_pct": {
                "type": "float",
                "unit": "%",
                "description": "Derived: year-on-year GDP growth by country",
            },
        },
    },
    "tourism_arrivals": {
        "title": "International Tourism Indicators — Latin America",
        "description": (
            "International tourist arrivals, tourism receipts and departures "
            "for 19 Latin American countries (2013–2023). "
            "Source: World Bank Tourism Indicators API (ST.INT.ARVL, ST.INT.RCPT.CD, ST.INT.DPRT). "
            "Cleaned and enriched with derived metrics."
        ),
        "source": "World Bank Open Data — https://api.worldbank.org/v2",
        "columns": {
            "country": {
                "type": "string",
                "unit": "—",
                "description": "Country name (English)",
            },
            "country_code": {
                "type": "string",
                "unit": "ISO α3",
                "description": "e.g. ARG, BRA, MEX",
            },
            "year": {
                "type": "integer",
                "unit": "—",
                "description": "Reference year (2013–2023)",
            },
            "tourist_arrivals": {
                "type": "integer",
                "unit": "persons",
                "description": "International tourist arrivals",
            },
            "tourism_receipts_usd": {
                "type": "float",
                "unit": "USD",
                "description": "Tourism receipts in current USD",
            },
            "tourist_departures": {
                "type": "integer",
                "unit": "persons",
                "description": "International tourist departures",
            },
            "arrivals_growth_pct": {
                "type": "float",
                "unit": "%",
                "description": "Derived: year-on-year growth in arrivals",
            },
            "receipts_per_tourist": {
                "type": "float",
                "unit": "USD",
                "description": "Derived: receipts / arrivals",
            },
        },
    },
    "transport_mode": {
        "title": "Tourist Arrivals by Transport Mode — Latin America",
        "description": (
            "International tourist arrivals disaggregated by transport mode "
            "(air, sea, land) for 19 Latin American countries (2013–2023). "
            "Source: UN Tourism / UNWTO. "
            "Note: coverage is partial — many countries do not report all modes. "
            "Null values indicate missing data, not zero."
        ),
        "source": "UN Tourism (UNWTO) — https://www.unwto.org",
        "columns": {
            "country": {
                "type": "string",
                "unit": "—",
                "description": "Country name (English)",
            },
            "country_code": {
                "type": "string",
                "unit": "ISO α3",
                "description": "e.g. ARG, BRA, MEX",
            },
            "year": {
                "type": "integer",
                "unit": "—",
                "description": "Reference year (2013–2023)",
            },
            "tourists_air": {
                "type": "float",
                "unit": "persons",
                "description": "Arrivals by air transport",
            },
            "tourists_sea": {
                "type": "float",
                "unit": "persons",
                "description": "Arrivals by sea transport",
            },
            "tourists_land": {
                "type": "float",
                "unit": "persons",
                "description": "Arrivals by land transport",
            },
            "tourists_total": {
                "type": "float",
                "unit": "persons",
                "description": "Total arrivals (sum of modes)",
            },
            "pct_air": {
                "type": "float",
                "unit": "%",
                "description": "Share of arrivals by air",
            },
            "pct_sea": {
                "type": "float",
                "unit": "%",
                "description": "Share of arrivals by sea",
            },
            "pct_land": {
                "type": "float",
                "unit": "%",
                "description": "Share of arrivals by land",
            },
            "dominant_transport": {
                "type": "string",
                "unit": "—",
                "description": "Mode with highest share (air/sea/land)",
            },
        },
    },
}

# ─── Helpers S3 ───────────────────────────────────────────────────────────────


def read_silver_parquet(silver_s3_path: str) -> pd.DataFrame:
    """Lee el Parquet Silver desde S3."""
    import s3fs

    fs = s3fs.S3FileSystem()
    path = silver_s3_path.replace("s3://", "")
    logger.info("📂 Leyendo Silver: s3://%s", path)
    dataset = pq.ParquetDataset(path, filesystem=fs)
    df = dataset.read().to_pandas()
    logger.info("   → %d filas, %d columnas", *df.shape)
    return df


def upload_bytes(content: bytes, bucket: str, key: str, content_type: str) -> None:
    s3 = boto3.client("s3")
    s3.put_object(Bucket=bucket, Key=key, Body=content, ContentType=content_type)
    logger.info("   ⬆️  s3://%s/%s", bucket, key)


# ─── Generadores de contenido ─────────────────────────────────────────────────


def df_to_parquet_bytes(df: pd.DataFrame) -> bytes:
    buf = BytesIO()
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, buf, compression="snappy")
    return buf.getvalue()


def df_to_csv_bytes(df: pd.DataFrame) -> bytes:
    buf = StringIO()
    df.to_csv(buf, index=False)
    return buf.getvalue().encode("utf-8")


def build_metadata_json(
    df: pd.DataFrame,
    dataset_key: str,
    version: str,
    bucket: str,
    open_data_prefix: str,
) -> bytes:
    meta_def = DATASET_META[dataset_key]
    filename_base = f"latam_{dataset_key}_{version}"
    base_url = f"https://{bucket}.s3.amazonaws.com/{open_data_prefix}"

    # Calcular null % por columna para el metadata
    null_pct = {col: round(df[col].isnull().mean() * 100, 1) for col in df.columns}

    metadata = {
        "dataset_id": f"latam_{dataset_key}",
        "version": version,
        "title": meta_def["title"],
        "description": meta_def["description"],
        "license": "Creative Commons Attribution 4.0 International (CC BY 4.0)",
        "license_url": "https://creativecommons.org/licenses/by/4.0/",
        "source": meta_def["source"],
        "maintained_by": "Grupo 1 — LATAM Sustainability Data Lake — Henry Data Engineering 2026",
        "repository": "https://github.com/adriangoll/Proyecto-Latam-Turismo-Sustentable",
        "last_updated": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "coverage": {
            "countries": sorted(df["country_code"].dropna().unique().tolist()),
            "year_start": int(df["year"].min()),
            "year_end": int(df["year"].max()),
            "rows": int(len(df)),
            "columns": int(len(df.columns)),
        },
        "null_coverage_pct": null_pct,
        "formats": {
            "parquet": f"{base_url}/{filename_base}.parquet",
            "csv": f"{base_url}/{filename_base}.csv",
        },
        "schema": {
            col: {
                "type": meta_def["columns"].get(col, {}).get("type", "unknown"),
                "unit": meta_def["columns"].get(col, {}).get("unit", "—"),
                "description": meta_def["columns"].get(col, {}).get("description", ""),
                "null_pct": null_pct.get(col, 0),
            }
            for col in df.columns
        },
    }
    return json.dumps(metadata, indent=2, ensure_ascii=False).encode("utf-8")


def build_data_dictionary_md(
    df: pd.DataFrame,
    dataset_key: str,
    version: str,
) -> bytes:
    meta_def = DATASET_META[dataset_key]
    null_pct = {col: round(df[col].isnull().mean() * 100, 1) for col in df.columns}

    lines = [
        f"# Data Dictionary — latam_{dataset_key}_{version}",
        "",
        f"**Title:** {meta_def['title']}",
        "",
        f"**Description:** {meta_def['description']}",
        "",
        f"**Source:** {meta_def['source']}",
        "",
        "**License:** [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/)",
        "",
        f"**Coverage:** {int(df['year'].min())}–{int(df['year'].max())} · {df['country_code'].nunique()} countries · {len(df):,} rows",
        "",
        "---",
        "",
        "## Columns",
        "",
        "| Column | Type | Unit | Description | Null % |",
        "|--------|------|------|-------------|--------|",
    ]

    for col in df.columns:
        col_meta = meta_def["columns"].get(col, {})
        col_type = col_meta.get("type", "—")
        col_unit = col_meta.get("unit", "—")
        col_desc = col_meta.get("description", "—")
        col_null = f"{null_pct.get(col, 0):.1f}%"
        lines.append(f"| `{col}` | {col_type} | {col_unit} | {col_desc} | {col_null} |")

    lines += [
        "",
        "---",
        "",
        "## Notes",
        "",
        "- Null values represent missing data in the source, not zero.",
        "- `country_code` follows ISO 3166-1 alpha-3 standard.",
        "- Derived columns (suffixed `_calc`, `_pct`, `_growth`) are computed by the pipeline.",
        "- This dataset is part of the LATAM Sustainability Data Lake open data initiative.",
        "",
        f"*Generated automatically on {datetime.now(timezone.utc).strftime('%Y-%m-%d')} by export_open_data_silver.py*",
    ]

    return "\n".join(lines).encode("utf-8")


# ─── Runner principal ─────────────────────────────────────────────────────────


def export_dataset(
    dataset_key: str,
    silver_s3_path: str,
    version: str,
    dry_run: bool,
) -> None:
    logger.info("─" * 50)
    logger.info("📤 Exportando: %s (version=%s)", dataset_key, version)

    df = read_silver_parquet(silver_s3_path)

    filename_base = f"latam_{dataset_key}_{version}"
    open_data_prefix = f"open-data/{version}/silver/{dataset_key}"

    outputs = {
        f"{open_data_prefix}/{filename_base}.parquet": (
            df_to_parquet_bytes(df),
            "application/octet-stream",
        ),
        f"{open_data_prefix}/{filename_base}.csv": (
            df_to_csv_bytes(df),
            "text/csv; charset=utf-8",
        ),
        f"{open_data_prefix}/metadata.json": (
            build_metadata_json(df, dataset_key, version, S3_BUCKET, open_data_prefix),
            "application/json",
        ),
        f"{open_data_prefix}/data_dictionary.md": (
            build_data_dictionary_md(df, dataset_key, version),
            "text/markdown; charset=utf-8",
        ),
    }

    if dry_run:
        logger.info("[DRY-RUN] Archivos que se generarían:")
        for key, (content, ctype) in outputs.items():
            logger.info("   → s3://%s/%s (%d bytes, %s)", S3_BUCKET, key, len(content), ctype)

        # En dry-run guardamos localmente para inspección
        local_dir = os.path.join("data", "open-data", version, "silver", dataset_key)
        os.makedirs(local_dir, exist_ok=True)
        for key, (content, _) in outputs.items():
            fname = os.path.basename(key)
            fpath = os.path.join(local_dir, fname)
            with open(fpath, "wb") as f:
                f.write(content)
            logger.info("[DRY-RUN] Guardado localmente: %s", fpath)
    else:
        for key, (content, content_type) in outputs.items():
            upload_bytes(content, S3_BUCKET, key, content_type)

    logger.info("✅ %s exportado correctamente", dataset_key)


def run(dry_run: bool = False, version: str = "v1", source: str = None) -> None:
    logger.info("=" * 60)
    logger.info("🚀 Export Open Data — Silver")
    logger.info("   Version: %s | Dry-run: %s", version, dry_run)
    logger.info("=" * 60)

    datasets = {
        "co2_emissions": S3_SILVER["co2"] + "data.parquet",
        "tourism_arrivals": S3_SILVER["tourism"] + "data.parquet",
        "transport_mode": S3_SILVER["transport"] + "data.parquet",
    }

    results = {}
    for key, silver_path in datasets.items():
        if source and key != source:
            continue
        try:
            export_dataset(key, silver_path, version, dry_run)
            results[key] = "OK"
        except Exception as e:
            logger.exception("❌ Error exportando %s: %s", key, e)
            results[key] = f"ERROR: {e}"

    logger.info("\n%s", "=" * 60)
    logger.info("RESUMEN Export Silver")
    all_ok = True
    for key, status in results.items():
        icon = "OK  " if status == "OK" else "FAIL"
        logger.info("  [%s] %s", icon, key)
        if status != "OK":
            logger.info("       └─ %s", status)
            all_ok = False
    logger.info("=" * 60)

    if not all_ok:
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Export Silver → open-data/v1/silver/")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--version", default="v1", help="Version tag (default: v1)")
    parser.add_argument(
        "--source",
        choices=["co2_emissions", "tourism_arrivals", "transport_mode"],
        default=None,
        help="Exportar solo un dataset",
    )
    args = parser.parse_args()
    run(dry_run=args.dry_run, version=args.version, source=args.source)
