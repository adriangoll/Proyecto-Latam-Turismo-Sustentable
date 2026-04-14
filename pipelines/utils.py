"""
utils.py — Utilidades compartidas entre scripts de ingesta
Grupo 1 — LATAM Sustainability Data Lake
"""

import io
import logging
import os
from typing import Optional

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError

import os as _os
import sys as _sys
_HERE = _os.path.dirname(_os.path.abspath(__file__))
if _HERE not in _sys.path:
    _sys.path.insert(0, _HERE)

from config import S3_BUCKET

# ─── Logger ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("ingestion.utils")


# ─── S3 client (singleton por módulo) ────────────────────────────────────────
def get_s3_client():
    """
    Devuelve un cliente S3. Usa las credenciales del entorno:
      - Variables de entorno: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION
      - O el perfil configurado con `aws configure`
    """
    return boto3.client(
        "s3",
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
    )


# ─── Upload raw (bytes / string) ─────────────────────────────────────────────
def upload_raw_to_s3(
    content: bytes,
    s3_key: str,
    content_type: str = "text/csv",
    bucket: str = S3_BUCKET,
) -> None:
    """
    Sube bytes crudos a S3 (capa raw — sin transformar).

    Args:
        content:      Contenido del archivo como bytes.
        s3_key:       Key completa dentro del bucket (sin bucket name).
        content_type: MIME type del archivo.
        bucket:       Nombre del bucket S3.
    """
    s3 = get_s3_client()
    try:
        s3.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=content,
            ContentType=content_type,
        )
        logger.info("✅ Raw upload OK → s3://%s/%s (%d bytes)", bucket, s3_key, len(content))
    except ClientError as e:
        logger.error("❌ Error subiendo raw a S3: %s", e)
        raise


# ─── Upload Parquet particionado (capa bronze) ────────────────────────────────
def upload_parquet_partitioned(
    df: pd.DataFrame,
    s3_prefix: str,
    partition_cols: list[str],
    bucket: str = S3_BUCKET,
    compression: str = "snappy",
) -> int:
    """
    Escribe un DataFrame a S3 en formato Parquet particionado (Hive style).
    Cada partición se sube como un archivo separado.

    Estructura resultante en S3:
        s3://<bucket>/<s3_prefix>/year=2020/country_code=ARG/data.parquet

    Args:
        df:             DataFrame limpio y filtrado listo para bronze.
        s3_prefix:      Prefijo base en S3 (ej: "bronze/co2_emissions/").
        partition_cols: Columnas de partición en orden (ej: ["year", "country_code"]).
        bucket:         Bucket S3 destino.
        compression:    Compresión Parquet ("snappy" es ideal para balance
                        velocidad/tamaño; "gzip" para mayor compresión).

    Returns:
        Cantidad de archivos Parquet subidos.
    """
    s3 = get_s3_client()
    files_uploaded = 0

    # Agrupar por las columnas de partición
    grouped = df.groupby(partition_cols)

    for partition_values, partition_df in grouped:
        # Asegurar que partition_values sea siempre una tupla
        if not isinstance(partition_values, tuple):
            partition_values = (partition_values,)

        # Construir el path estilo Hive: year=2020/country_code=ARG/
        partition_path = "/".join(
            f"{col}={val}"
            for col, val in zip(partition_cols, partition_values)
        )
        s3_key = f"{s3_prefix.rstrip('/')}/{partition_path}/data.parquet"

        # Serializar a Parquet en memoria (no toca disco)
        buffer = io.BytesIO()
        table = pa.Table.from_pandas(partition_df, preserve_index=False)
        pq.write_table(table, buffer, compression=compression)
        buffer.seek(0)
        parquet_bytes = buffer.read()

        try:
            s3.put_object(
                Bucket=bucket,
                Key=s3_key,
                Body=parquet_bytes,
                ContentType="application/octet-stream",
            )
            files_uploaded += 1
            logger.debug(
                "  📦 Parquet → s3://%s/%s (rows=%d, %d bytes)",
                bucket, s3_key, len(partition_df), len(parquet_bytes),
            )
        except ClientError as e:
            logger.error("❌ Error subiendo Parquet %s: %s", s3_key, e)
            raise

    logger.info(
        "✅ Bronze upload OK → s3://%s/%s | %d particiones subidas",
        bucket, s3_prefix, files_uploaded,
    )
    return files_uploaded


# ─── Helpers ──────────────────────────────────────────────────────────────────
def normalize_country_name(name: str, aliases: dict) -> str:
    """Normaliza variantes de nombre de país al estándar del proyecto."""
    return aliases.get(name, name)


def log_dataframe_summary(df: pd.DataFrame, label: str) -> None:
    """Imprime un resumen rápido del DataFrame para trazabilidad."""
    logger.info(
        "📊 %s | shape=%s | nulls=%d | países=%s | años=%s",
        label,
        df.shape,
        df.isnull().sum().sum(),
        sorted(df["country"].unique()) if "country" in df.columns else "N/A",
        sorted(df["year"].unique()) if "year" in df.columns else "N/A",
    )