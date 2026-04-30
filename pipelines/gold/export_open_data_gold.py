"""
export_open_data_gold.py — Exporta Gold a open-data/v1/gold/
=============================================================
Mismo patrón que export_open_data_silver.py.
Genera CSV + Parquet + metadata.json + data_dictionary.md

Ejecución:
    python export_open_data_gold.py
    python export_open_data_gold.py --dry-run
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

from config_gold import S3_BUCKET, S3_GOLD

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("export.open_data_gold")

DATASET_META = {
    "fact_tourism_emissions": {
        "title": "Tourism Emissions Fact Table — Latin America",
        "description": (
            "Integrated dataset joining CO₂ emissions, international tourism arrivals "
            "and transport mode data for 19 Latin American countries (2013–2023). "
            "Includes sustainability classification and co2_per_tourist KPI."
        ),
        "source": "Derived from OWID CO₂, World Bank Tourism, UN Tourism (UNWTO)",
        "columns": {
            "country": {"type": "string", "unit": "—", "description": "Country name"},
            "country_code": {"type": "string", "unit": "ISO α3", "description": "ISO 3166-1 alpha-3"},
            "year": {"type": "integer", "unit": "—", "description": "Reference year"},
            "co2": {"type": "float", "unit": "Mt CO₂", "description": "Total CO₂ emissions"},
            "co2_per_capita": {"type": "float", "unit": "t/person", "description": "CO₂ per capita (OWID)"},
            "co2_per_capita_calc": {"type": "float", "unit": "t/person", "description": "CO₂ per capita (derived)"},
            "co2_intensity_gdp": {"type": "float", "unit": "kg CO₂/USD", "description": "CO₂ per unit of GDP"},
            "gdp": {"type": "float", "unit": "USD PPP", "description": "Gross domestic product"},
            "gdp_per_capita": {"type": "float", "unit": "USD PPP", "description": "GDP per capita"},
            "gdp_growth_pct": {"type": "float", "unit": "%", "description": "Year-on-year GDP growth"},
            "population": {"type": "integer", "unit": "persons", "description": "Mid-year population"},
            "share_global_co2": {"type": "float", "unit": "%", "description": "Share of global CO₂"},
            "tourist_arrivals": {"type": "integer", "unit": "persons", "description": "International tourist arrivals"},
            "tourism_receipts_usd": {"type": "float", "unit": "USD", "description": "Tourism receipts"},
            "tourist_departures": {"type": "integer", "unit": "persons", "description": "Tourist departures"},
            "arrivals_growth_pct": {"type": "float", "unit": "%", "description": "Year-on-year arrivals growth"},
            "receipts_per_tourist": {"type": "float", "unit": "USD", "description": "Revenue per tourist"},
            "tourists_air": {"type": "float", "unit": "persons", "description": "Arrivals by air"},
            "tourists_sea": {"type": "float", "unit": "persons", "description": "Arrivals by sea"},
            "tourists_land": {"type": "float", "unit": "persons", "description": "Arrivals by land"},
            "pct_air": {"type": "float", "unit": "%", "description": "Share by air"},
            "pct_sea": {"type": "float", "unit": "%", "description": "Share by sea"},
            "pct_land": {"type": "float", "unit": "%", "description": "Share by land"},
            "dominant_transport": {"type": "string", "unit": "—", "description": "Dominant mode (air/sea/land)"},
            "co2_per_tourist": {"type": "float", "unit": "t CO₂", "description": "CO₂ tons per tourist arrived"},
            "co2_growth_pct": {"type": "float", "unit": "%", "description": "Year-on-year CO₂ growth"},
            "sustainability_label": {"type": "string", "unit": "—", "description": "verde/amarillo/rojo/gris"},
        },
    },
    "dim_country": {
        "title": "Country Dimension — Latin America",
        "description": "Reference table with ISO codes and regional classification for 19 LATAM countries.",
        "source": "ISO 3166-1, internal project classification",
        "columns": {
            "country_code": {"type": "string", "unit": "ISO α3", "description": "ISO 3166-1 alpha-3"},
            "country_code_iso2": {"type": "string", "unit": "ISO α2", "description": "ISO 3166-1 alpha-2"},
            "country_name": {"type": "string", "unit": "—", "description": "Country name in English"},
            "region_latam": {"type": "string", "unit": "—", "description": "Sub-region within LATAM"},
        },
    },
}


def read_gold_s3(s3_path: str) -> pd.DataFrame:
    import s3fs

    fs = s3fs.S3FileSystem()
    path = s3_path.replace("s3://", "")
    with fs.open(path) as f:
        return pd.read_parquet(f)


def upload_bytes(content: bytes, bucket: str, key: str, content_type: str) -> None:
    boto3.client("s3").put_object(Bucket=bucket, Key=key, Body=content, ContentType=content_type)
    logger.info("   ⬆️  s3://%s/%s", bucket, key)


def df_to_parquet_bytes(df: pd.DataFrame) -> bytes:
    buf = BytesIO()
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False), buf, compression="snappy")
    return buf.getvalue()


def df_to_csv_bytes(df: pd.DataFrame) -> bytes:
    buf = StringIO()
    df.to_csv(buf, index=False)
    return buf.getvalue().encode("utf-8")


def build_metadata(df: pd.DataFrame, key: str, version: str, bucket: str, prefix: str) -> bytes:
    meta = DATASET_META[key]
    null_pct = {col: round(df[col].isnull().mean() * 100, 1) for col in df.columns}
    fname = f"latam_{key}_{version}"
    base = f"https://{bucket}.s3.amazonaws.com/{prefix}"
    obj = {
        "dataset_id": f"latam_{key}",
        "version": version,
        "title": meta["title"],
        "description": meta["description"],
        "license": "CC BY 4.0",
        "source": meta["source"],
        "maintained_by": "Grupo 1 — Henry Data Engineering 2026",
        "repository": "https://github.com/adriangoll/Proyecto-Latam-Turismo-Sustentable",
        "last_updated": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "rows": int(len(df)),
        "formats": {"parquet": f"{base}/{fname}.parquet", "csv": f"{base}/{fname}.csv"},
        "null_pct": null_pct,
        "schema": {
            col: {
                "type": meta["columns"].get(col, {}).get("type", "—"),
                "unit": meta["columns"].get(col, {}).get("unit", "—"),
                "description": meta["columns"].get(col, {}).get("description", "—"),
            }
            for col in df.columns
        },
    }
    return json.dumps(obj, indent=2, ensure_ascii=False).encode("utf-8")


def build_dictionary(df: pd.DataFrame, key: str, version: str) -> bytes:
    meta = DATASET_META[key]
    null_pct = {col: round(df[col].isnull().mean() * 100, 1) for col in df.columns}
    lines = [
        f"# Data Dictionary — latam_{key}_{version}",
        "",
        f"**{meta['title']}**",
        "",
        f"{meta['description']}",
        "",
        f"**Source:** {meta['source']}",
        "",
        "**License:** CC BY 4.0 — https://creativecommons.org/licenses/by/4.0/",
        "",
        "---",
        "",
        "| Column | Type | Unit | Description | Null % |",
        "|--------|------|------|-------------|--------|",
    ]
    for col in df.columns:
        m = meta["columns"].get(col, {})
        lines.append(f"| `{col}` | {m.get('type', '—')} | {m.get('unit', '—')} | {m.get('description', '—')} | {null_pct.get(col, 0):.1f}% |")
    lines += [
        "",
        "---",
        f"*Generated on {datetime.now(timezone.utc).strftime('%Y-%m-%d')}*",
    ]
    return "\n".join(lines).encode("utf-8")


def export_dataset(key: str, s3_gold_path: str, version: str, dry_run: bool) -> None:
    logger.info("─" * 50)
    logger.info("📤 Exportando Gold: %s", key)

    df = read_gold_s3(s3_gold_path)
    fname = f"latam_{key}_{version}"
    prefix = f"open-data/{version}/gold/{key}"

    outputs = {
        f"{prefix}/{fname}.parquet": (df_to_parquet_bytes(df), "application/octet-stream"),
        f"{prefix}/{fname}.csv": (df_to_csv_bytes(df), "text/csv; charset=utf-8"),
        f"{prefix}/metadata.json": (build_metadata(df, key, version, S3_BUCKET, prefix), "application/json"),
        f"{prefix}/data_dictionary.md": (build_dictionary(df, key, version), "text/markdown; charset=utf-8"),
    }

    if dry_run:
        local_dir = os.path.join("data", "open-data", version, "gold", key)
        os.makedirs(local_dir, exist_ok=True)
        for k, (content, _) in outputs.items():
            fpath = os.path.join(local_dir, os.path.basename(k))
            with open(fpath, "wb") as f:
                f.write(content)
            logger.info("[DRY-RUN] %s", fpath)
    else:
        for k, (content, ctype) in outputs.items():
            upload_bytes(content, S3_BUCKET, k, ctype)

    logger.info("✅ %s exportado", key)


def run(dry_run: bool = False, version: str = "v1") -> None:
    logger.info("=" * 60)
    logger.info("🚀 Export Open Data — Gold (version=%s)", version)
    logger.info("=" * 60)

    datasets = {
        "fact_tourism_emissions": S3_GOLD["fact"],
        "dim_country": S3_GOLD["dim_country"],
    }

    for key, path in datasets.items():
        try:
            export_dataset(key, path, version, dry_run)
        except Exception as e:
            logger.exception("❌ Error exportando %s: %s", key, e)
            sys.exit(1)

    logger.info("✅ Export Gold completo")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--version", default="v1")
    args = parser.parse_args()
    run(dry_run=args.dry_run, version=args.version)
