# pipelines/transformation/detect_outliers.py
"""
detect_outliers.py — Identificar valores atípicos sin eliminarlos
==================================================================
Script para detectar outliers en Silver usando IQR (Interquartile Range).
NO elimina datos, solo identifica y reporta para análisis posterior.

Ejecución:
    python detect_outliers.py --dataset co2_emissions
"""

import json
import logging
import os
import sys
from datetime import datetime, timezone

import boto3
import pandas as pd

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

from config_silver import S3_BUCKET, S3_SILVER

logger = logging.getLogger("outliers.detector")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

# ─── Configuración de outliers por dataset ─────────────────────────────────

OUTLIER_THRESHOLDS = {
    "co2_emissions": {
        "co2": {"method": "iqr", "multiplier": 1.5},  # Q1 - 1.5*IQR, Q3 + 1.5*IQR
        "gdp": {"method": "iqr", "multiplier": 1.5},
        "population": {"method": "iqr", "multiplier": 2.0},  # Más tolerante
    },
    "tourism_arrivals": {
        "tourist_arrivals": {"method": "iqr", "multiplier": 1.5},
        "tourism_receipts_usd": {"method": "iqr", "multiplier": 1.5},
    },
    "transport_mode": {
        "tourists_air": {"method": "iqr", "multiplier": 1.5},
        "tourists_sea": {"method": "iqr", "multiplier": 2.0},
        "tourists_land": {"method": "iqr", "multiplier": 1.5},
    },
}


def detect_outliers_iqr(series: pd.Series, multiplier: float = 1.5) -> dict:
    """
    Detecta outliers usando Interquartile Range (IQR).

    Outliers son valores fuera de [Q1 - m*IQR, Q3 + m*IQR]
    donde m = multiplier (típicamente 1.5 para moderado, 3.0 para extremo)
    """
    if series.isnull().all():
        return {"method": "iqr", "outliers_count": 0, "outliers": []}

    q1 = series.quantile(0.25)
    q3 = series.quantile(0.75)
    iqr = q3 - q1

    lower_bound = q1 - multiplier * iqr
    upper_bound = q3 + multiplier * iqr

    outlier_mask = (series < lower_bound) | (series > upper_bound)
    outliers = series[outlier_mask].to_list()

    return {
        "method": "iqr",
        "q1": float(q1),
        "q3": float(q3),
        "iqr": float(iqr),
        "lower_bound": float(lower_bound),
        "upper_bound": float(upper_bound),
        "multiplier": multiplier,
        "outliers_count": int(outlier_mask.sum()),
        "outlier_percentage": float(outlier_mask.sum() / len(series) * 100),
        "sample_outliers": [float(x) for x in outliers[:5]],  # Primeros 5
    }


def detect_outliers_in_df(df: pd.DataFrame, dataset_name: str) -> dict:
    """
    Detecta outliers en todas las columnas numéricas del dataset.
    """
    if dataset_name not in OUTLIER_THRESHOLDS:
        logger.warning("Dataset %s no tiene umbral configurado", dataset_name)
        return {}

    thresholds = OUTLIER_THRESHOLDS[dataset_name]
    results = {}

    for col, config in thresholds.items():
        if col not in df.columns:
            logger.warning("Columna %s no encontrada en %s", col, dataset_name)
            continue

        if config["method"] == "iqr":
            results[col] = detect_outliers_iqr(df[col], config["multiplier"])
            logger.info(
                "   %s: %d outliers (%.1f%%) detectados",
                col,
                results[col]["outliers_count"],
                results[col]["outlier_percentage"],
            )

    return results


def read_silver_s3(s3_path: str) -> pd.DataFrame:
    """Lee Parquet desde S3."""
    import s3fs

    fs = s3fs.S3FileSystem()
    path = s3_path.replace("s3://", "")
    with fs.open(path) as f:
        return pd.read_parquet(f)


def upload_report_s3(report: dict, dataset_name: str) -> None:
    """Sube reporte JSON a S3."""
    s3_client = boto3.client("s3")
    key = f"quality_reports/outliers_{dataset_name}_{datetime.now(timezone.utc).strftime('%Y-%m-%d')}.json"

    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=json.dumps(report, indent=2),
        ContentType="application/json",
    )
    logger.info("✅ Reporte guardado en s3://%s/%s", S3_BUCKET, key)


def run(dataset_name: str = "all", dry_run: bool = False) -> None:
    """
    Detecta outliers en Silver.

    Args:
        dataset_name: "co2_emissions", "tourism_arrivals", "transport_mode", o "all"
        dry_run: Si True, no sube a S3
    """
    logger.info("=" * 60)
    logger.info("🔍 Detectando valores atípicos (Outliers)")
    logger.info("=" * 60)

    datasets = {
        "co2_emissions": S3_SILVER["co2"],
        "tourism_arrivals": S3_SILVER["tourism"],
        "transport_mode": S3_SILVER["transport"],
    }

    if dataset_name != "all":
        if dataset_name not in datasets:
            logger.error("❌ Dataset inválido: %s", dataset_name)
            return
        datasets = {dataset_name: datasets[dataset_name]}

    for name, s3_path in datasets.items():
        logger.info("\n📂 Analizando: %s", name)
        try:
            df = read_silver_s3(s3_path)
            logger.info("   → %d filas leídas", len(df))

            outliers = detect_outliers_in_df(df, name)

            # Construir reporte
            report = {
                "dataset": name,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "total_rows": int(len(df)),
                "outliers_by_column": outliers,
                "summary": f"Outliers detectados en {len(outliers)} columnas",
            }

            if not dry_run:
                upload_report_s3(report, name)
            else:
                logger.info("[DRY-RUN] Reporte:")
                logger.info(json.dumps(report, indent=2))

        except Exception as e:
            logger.error("❌ Error en %s: %s", name, e)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Detectar outliers en Silver")
    parser.add_argument("--dataset", choices=["co2_emissions", "tourism_arrivals", "transport_mode", "all"], default="all")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    run(dataset_name=args.dataset, dry_run=args.dry_run)
