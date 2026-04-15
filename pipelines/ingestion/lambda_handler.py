"""
pipelines/ingestion/lambda_handler.py
======================================
Lambda function disparada automáticamente cuando alguien sube
un archivo nuevo al prefijo raw/ del Data Lake en S3.

Flujo completo:
  1. Usuario sube archivo a s3://bucket/raw/uploads/<dataset>/<filename>
  2. S3 Event Notification dispara esta Lambda
  3. Lambda detecta el tipo de archivo (CSV, Excel, JSON)
  4. Parsea, valida, filtra países LATAM y años 2013-2023
  5. Escribe Parquet particionado en bronze/<dataset>/year=X/country_code=Y/

Formatos soportados:
  - .csv
  - .xlsx / .xls
  - .json (records o lines)

Datasets reconocidos automáticamente:
  - owid_co2        → columnas: country, year, co2, ...
  - worldbank       → columnas: country, year, tourist_arrivals, ...
  - owid_transport  → columnas: Entity, Year, Air transport, ...
  - custom          → cualquier dataset con columnas country + year

Ejecución local para tests:
  python lambda_handler.py --local --file path/to/file.csv --dataset owid_co2
"""

import io
import json
import logging
import os
import urllib.parse
from typing import Optional

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# ── Logger ────────────────────────────────────────────────────────────────────
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ── Constantes (se pueden sobreescribir con variables de entorno en Lambda) ───
BUCKET          = os.environ.get("DATALAKE_BUCKET", "latam-sustainability-dev-datalake")
YEAR_START      = int(os.environ.get("YEAR_START", "2013"))
YEAR_END        = int(os.environ.get("YEAR_END", "2023"))
COMPRESSION     = os.environ.get("PARQUET_COMPRESSION", "snappy")

LATAM_COUNTRIES = [
    "Argentina", "Bolivia", "Brazil", "Chile", "Colombia", "Costa Rica",
    "Cuba", "Dominican Republic", "Ecuador", "El Salvador", "Guatemala",
    "Honduras", "Mexico", "Nicaragua", "Panama", "Paraguay", "Peru",
    "Uruguay", "Venezuela",
]

COUNTRY_ISO3 = {
    "Argentina": "ARG", "Bolivia": "BOL", "Brazil": "BRA", "Chile": "CHL",
    "Colombia": "COL", "Costa Rica": "CRI", "Cuba": "CUB",
    "Dominican Republic": "DOM", "Ecuador": "ECU", "El Salvador": "SLV",
    "Guatemala": "GTM", "Honduras": "HND", "Mexico": "MEX",
    "Nicaragua": "NIC", "Panama": "PAN", "Paraguay": "PRY", "Peru": "PER",
    "Uruguay": "URY", "Venezuela": "VEN",
}

NAME_ALIASES = {
    "Venezuela, RB": "Venezuela",
    "Venezuela (Bolivarian Republic of)": "Venezuela",
    "Bolivia (Plurinational State of)": "Bolivia",
    "Dominican Rep.": "Dominican Republic",
    "Brasil": "Brazil", "México": "Mexico",
    "Panamá": "Panama", "Perú": "Peru",
    # columna "Entity" de OWID
    "Entity": None,
}

# Mapeo de columnas por dataset conocido
COLUMN_MAPS = {
    "owid_co2": {"country": "country", "year": "year"},
    "worldbank": {"country": "country", "year": "year"},
    "owid_transport": {"Entity": "country", "Year": "year"},
    "custom": {},  # detección automática
}


# ═══════════════════════════════════════════════════════════════════════════════
# PUNTO DE ENTRADA — disparado por S3 Event
# ═══════════════════════════════════════════════════════════════════════════════
def handler(event: dict, context) -> dict:
    """
    Entry point de la Lambda.
    Recibe el evento S3 con la información del archivo subido.
    """
    logger.info("Evento recibido: %s", json.dumps(event))

    results = []
    for record in event.get("Records", []):
        try:
            result = _process_record(record)
            results.append(result)
        except Exception as e:
            key = record.get("s3", {}).get("object", {}).get("key", "unknown")
            logger.error("Error procesando %s: %s", key, e, exc_info=True)
            results.append({"key": key, "status": "error", "error": str(e)})

    logger.info("Resultados: %s", json.dumps(results))
    return {"statusCode": 200, "body": json.dumps(results)}


def _process_record(record: dict) -> dict:
    """Procesa un registro del evento S3."""
    bucket = record["s3"]["bucket"]["name"]
    key    = urllib.parse.unquote_plus(record["s3"]["object"]["key"])
    size   = record["s3"]["object"].get("size", 0)

    logger.info("Procesando: s3://%s/%s (%d bytes)", bucket, key, size)

    # Validar que viene de raw/uploads/
    if not key.startswith("raw/uploads/"):
        logger.warning("Archivo fuera de raw/uploads/, ignorando: %s", key)
        return {"key": key, "status": "skipped", "reason": "not in raw/uploads/"}

    # Determinar dataset desde la estructura del path:
    # raw/uploads/<dataset>/<filename>
    parts = key.split("/")
    if len(parts) < 4:
        raise ValueError(f"Path inesperado: {key}. Esperado: raw/uploads/<dataset>/<filename>")

    dataset_name = parts[2]      # ej: owid_co2, worldbank, custom_2024
    filename     = parts[-1]

    # Descargar el archivo desde S3
    raw_bytes = _download_from_s3(bucket, key)

    # Parsear según extensión
    df_raw = _parse_file(raw_bytes, filename)
    logger.info("Archivo parseado: %s filas, %s columnas", len(df_raw), len(df_raw.columns))

    # Normalizar columnas al estándar interno
    df = _normalize_columns(df_raw, dataset_name)

    # Filtrar países LATAM y rango de años
    df = _filter_latam(df)
    df = _filter_years(df)

    if df.empty:
        logger.warning("DataFrame vacio despues de filtrar. Sin datos LATAM 2013-2023.")
        return {"key": key, "status": "empty", "rows": 0}

    # Agregar country_code ISO3
    df["country_code"] = df["country"].map(COUNTRY_ISO3).fillna("UNK")

    # Tipos correctos
    df["year"] = df["year"].astype(int)

    # Validar mínimamente
    _validate(df, dataset_name)

    # Escribir a bronze/ particionado
    n_partitions = _write_bronze(df, dataset_name, bucket)

    logger.info(
        "OK: %s → bronze/%s/ | %d filas | %d particiones",
        key, dataset_name, len(df), n_partitions
    )
    return {
        "key": key,
        "status": "ok",
        "dataset": dataset_name,
        "rows": len(df),
        "partitions": n_partitions,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# PARSEO DE ARCHIVOS
# ═══════════════════════════════════════════════════════════════════════════════
def _download_from_s3(bucket: str, key: str) -> bytes:
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=bucket, Key=key)
    return obj["Body"].read()


def _parse_file(raw_bytes: bytes, filename: str) -> pd.DataFrame:
    """
    Detecta el formato por extensión y parsea el archivo.
    Soporta CSV, Excel (.xlsx/.xls) y JSON.
    """
    name_lower = filename.lower()

    if name_lower.endswith(".csv"):
        # Intentar con UTF-8, luego latin-1 como fallback
        try:
            return pd.read_csv(io.BytesIO(raw_bytes), low_memory=False)
        except UnicodeDecodeError:
            return pd.read_csv(io.BytesIO(raw_bytes), encoding="latin-1", low_memory=False)

    elif name_lower.endswith(".xlsx"):
        return pd.read_excel(io.BytesIO(raw_bytes), engine="openpyxl")

    elif name_lower.endswith(".xls"):
        return pd.read_excel(io.BytesIO(raw_bytes), engine="xlrd")

    elif name_lower.endswith(".json"):
        try:
            # Intentar JSON lines primero
            return pd.read_json(io.BytesIO(raw_bytes), lines=True)
        except ValueError:
            return pd.read_json(io.BytesIO(raw_bytes))

    else:
        raise ValueError(
            f"Formato no soportado: {filename}. "
            "Formatos validos: .csv, .xlsx, .xls, .json"
        )


# ═══════════════════════════════════════════════════════════════════════════════
# NORMALIZACIÓN DE COLUMNAS
# ═══════════════════════════════════════════════════════════════════════════════
def _normalize_columns(df: pd.DataFrame, dataset_name: str) -> pd.DataFrame:
    """
    Normaliza los nombres de columna al estándar interno.
    Detecta automáticamente las columnas country y year si no
    coinciden exactamente con el mapeo conocido.
    """
    col_map = COLUMN_MAPS.get(dataset_name, {})

    # Aplicar mapeo conocido
    if col_map:
        rename = {k: v for k, v in col_map.items() if k in df.columns}
        df = df.rename(columns=rename)

    # Detección automática si no tiene columnas estándar
    cols_lower = {c.lower(): c for c in df.columns}

    if "country" not in df.columns:
        for candidate in ["country", "entity", "nation", "pais", "country_name"]:
            if candidate in cols_lower:
                df = df.rename(columns={cols_lower[candidate]: "country"})
                logger.info("Columna country detectada como: %s", cols_lower[candidate])
                break

    if "year" not in df.columns:
        for candidate in ["year", "año", "date", "periodo", "anio"]:
            if candidate in cols_lower:
                df = df.rename(columns={cols_lower[candidate]: "year"})
                logger.info("Columna year detectada como: %s", cols_lower[candidate])
                break

    if "country" not in df.columns or "year" not in df.columns:
        raise ValueError(
            f"No se encontraron columnas 'country' y 'year' en el archivo. "
            f"Columnas disponibles: {list(df.columns)}"
        )

    # Normalizar aliases de nombres de país
    df["country"] = df["country"].astype(str).apply(
        lambda x: NAME_ALIASES.get(x, x)
    ).apply(
        lambda x: NAME_ALIASES.get(x, x)  # segunda pasada para aliases encadenados
    )

    return df


def _filter_latam(df: pd.DataFrame) -> pd.DataFrame:
    before = len(df)
    df = df[df["country"].isin(LATAM_COUNTRIES)].copy()
    logger.info("Filtro LATAM: %d → %d filas", before, len(df))
    return df


def _filter_years(df: pd.DataFrame) -> pd.DataFrame:
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df = df.dropna(subset=["year"])
    before = len(df)
    df = df[(df["year"] >= YEAR_START) & (df["year"] <= YEAR_END)].copy()
    logger.info("Filtro años %d-%d: %d → %d filas", YEAR_START, YEAR_END, before, len(df))
    return df


# ═══════════════════════════════════════════════════════════════════════════════
# VALIDACIÓN
# ═══════════════════════════════════════════════════════════════════════════════
def _validate(df: pd.DataFrame, dataset_name: str) -> None:
    """
    Validaciones mínimas antes de escribir a bronze.
    Lanza ValueError si hay un problema crítico.
    """
    errors = []

    if df.empty:
        raise ValueError(f"Validacion fallida para {dataset_name}: DataFrame vacio")

    if df.duplicated(subset=["country", "year"]).any():
        n_dups = df.duplicated(subset=["country", "year"]).sum()
        logger.warning(
            "Dataset %s: %d duplicados (country, year). Se conservan para bronze — "
            "se eliminaran en silver.", dataset_name, n_dups
        )
        # En bronze conservamos duplicados — se limpian en silver

    unknown_codes = df[df["country_code"] == "UNK"]["country"].unique()
    if len(unknown_codes) > 0:
        logger.warning("Paises sin ISO3 mapeado: %s", list(unknown_codes))

    logger.info(
        "Validacion OK — %d filas | %d paises | anos %d-%d",
        len(df), df["country"].nunique(),
        int(df["year"].min()), int(df["year"].max())
    )


# ═══════════════════════════════════════════════════════════════════════════════
# ESCRITURA A BRONZE (Parquet particionado)
# ═══════════════════════════════════════════════════════════════════════════════
def _write_bronze(df: pd.DataFrame, dataset_name: str, bucket: str) -> int:
    """
    Escribe el DataFrame a bronze/<dataset>/year=Y/country_code=X/data.parquet
    Retorna el número de particiones escritas.
    """
    s3 = boto3.client("s3")
    files_written = 0

    for (year, country_code), partition_df in df.groupby(["year", "country_code"]):
        s3_key = (
            f"bronze/{dataset_name}/"
            f"year={int(year)}/"
            f"country_code={country_code}/"
            f"data.parquet"
        )

        # Serializar a Parquet en memoria
        buf = io.BytesIO()
        table = pa.Table.from_pandas(partition_df.reset_index(drop=True), preserve_index=False)
        pq.write_table(table, buf, compression=COMPRESSION)
        buf.seek(0)

        s3.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=buf.read(),
            ContentType="application/octet-stream",
        )
        files_written += 1

    return files_written


# ═══════════════════════════════════════════════════════════════════════════════
# MODO LOCAL — para probar sin Lambda ni S3
# ═══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Test local del handler Lambda")
    parser.add_argument("--local",   action="store_true", help="Modo local (sin S3)")
    parser.add_argument("--file",    required=True,       help="Path al archivo local")
    parser.add_argument("--dataset", required=True,       help="Nombre del dataset (ej: owid_co2)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")

    with open(args.file, "rb") as f:
        raw_bytes = f.read()

    filename = os.path.basename(args.file)
    df_raw   = _parse_file(raw_bytes, filename)
    df       = _normalize_columns(df_raw, args.dataset)
    df       = _filter_latam(df)
    df       = _filter_years(df)

    if df.empty:
        print("Sin datos LATAM 2013-2023 en el archivo.")
    else:
        df["country_code"] = df["country"].map(COUNTRY_ISO3).fillna("UNK")
        df["year"] = df["year"].astype(int)
        _validate(df, args.dataset)

        print(f"\nResultado local (sin subir a S3):")
        print(f"  Filas:       {len(df)}")
        print(f"  Paises:      {sorted(df['country'].unique())}")
        print(f"  Anos:        {sorted(df['year'].unique())}")
        print(f"  Columnas:    {list(df.columns)}")
        print(f"  Particiones: {df.groupby(['year', 'country_code']).ngroups}")
        print(f"\nPrimeras filas:\n{df.head(5).to_string()}")