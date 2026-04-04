"""
ingest_worldbank_tourism.py — Ingesta de indicadores turísticos del World Bank
===============================================================================
Fuente : https://api.worldbank.org/v2  (JSON API, sin key requerida)
Indicadores:
  ST.INT.ARVL    → llegadas de turistas internacionales
  ST.INT.RCPT.CD → ingresos por turismo en USD corrientes
  ST.INT.DPRT    → salidas de turistas

Destino: s3://latam-sustainability-datalake/
  ├── raw/worldbank_tourism/<indicator>.csv     ← uno por indicador
  └── bronze/tourism_arrivals/
        year=<Y>/country_code=<ISO3>/data.parquet

La World Bank API retorna hasta 1000 registros por página.
Hacemos paginación automática para no perder datos.

Ejecución:
  python ingest_worldbank_tourism.py
  python ingest_worldbank_tourism.py --dry-run

Variables de entorno requeridas:
  AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION
"""

import argparse
import logging
import sys
import time
from io import StringIO

import pandas as pd
import requests

from config import (
    COUNTRY_ISO2,
    COUNTRY_ISO3,
    LATAM_COUNTRIES,
    NAME_ALIASES,
    S3_PATHS,
    WORLDBANK_BASE_URL,
    WORLDBANK_INDICATORS,
    YEAR_END,
    YEAR_START,
)
from utils import (
    log_dataframe_summary,
    normalize_country_name,
    upload_parquet_partitioned,
    upload_raw_to_s3,
)

logger = logging.getLogger("ingestion.worldbank_tourism")

# ─── Reverse lookup: ISO2 → country name estándar ────────────────────────────
ISO2_TO_COUNTRY = {v: k for k, v in COUNTRY_ISO2.items()}

# ─── Parámetros de la API ─────────────────────────────────────────────────────
WB_PAGE_SIZE = 1000        # máximo permitido por la API
WB_RETRY_ATTEMPTS = 3
WB_RETRY_DELAY = 2         # segundos entre reintentos


def fetch_indicator(indicator_code: str, country_iso2_list: list[str]) -> list[dict]:
    """
    Consulta un indicador del World Bank para una lista de países y todos
    los años disponibles. Maneja paginación automáticamente.

    Args:
        indicator_code:   Ej. "ST.INT.ARVL"
        country_iso2_list: Lista de códigos ISO2 (ej. ["AR", "BR", ...])

    Returns:
        Lista de dicts con los registros retornados por la API.
    """
    # La API acepta múltiples países separados por ";"
    countries_param = ";".join(country_iso2_list)
    url = (
        f"{WORLDBANK_BASE_URL}/country/{countries_param}"
        f"/indicator/{indicator_code}"
    )

    params = {
        "format": "json",
        "per_page": WB_PAGE_SIZE,
        "date": f"{YEAR_START}:{YEAR_END}",
    }

    all_records = []
    page = 1
    total_pages = 1  # se actualiza con la primera respuesta

    while page <= total_pages:
        params["page"] = page

        for attempt in range(1, WB_RETRY_ATTEMPTS + 1):
            try:
                logger.info(
                    "   → [%s] página %d/%d (intento %d)",
                    indicator_code, page, total_pages, attempt,
                )
                resp = requests.get(url, params=params, timeout=30)
                resp.raise_for_status()
                data = resp.json()
                break
            except (requests.RequestException, ValueError) as e:
                if attempt == WB_RETRY_ATTEMPTS:
                    logger.error("❌ Fallo definitivo en %s p%d: %s", indicator_code, page, e)
                    raise
                logger.warning("   ⚠️ Reintentando en %ds...", WB_RETRY_DELAY)
                time.sleep(WB_RETRY_DELAY)

        # data[0] → metadata de paginación, data[1] → registros
        if not isinstance(data, list) or len(data) < 2:
            logger.warning("   ⚠️ Respuesta inesperada de la API: %s", data)
            break

        meta = data[0]
        records = data[1]

        if records is None:
            logger.warning("   ⚠️ Sin registros para %s", indicator_code)
            break

        total_pages = meta.get("pages", 1)
        all_records.extend(records)
        page += 1

    logger.info("   → %s: %d registros totales", indicator_code, len(all_records))
    return all_records


def parse_wb_records(
    records: list[dict],
    col_name: str,
) -> pd.DataFrame:
    """
    Convierte la lista de registros crudos de la WB API a un DataFrame limpio.

    Estructura de cada registro:
    {
        "indicator": {"id": "ST.INT.ARVL", "value": "..."},
        "country": {"id": "AR", "value": "Argentina"},
        "countryiso3code": "ARG",
        "date": "2020",
        "value": 1234567,
        "unit": "",
        "obs_status": "",
        "decimal": 0
    }
    """
    rows = []
    for r in records:
        country_iso2 = r.get("country", {}).get("id", "")
        country_name_raw = r.get("country", {}).get("value", "")
        country_name = normalize_country_name(country_name_raw, NAME_ALIASES)

        # Filtrar aggregates regionales que devuelve la API (ej. "Latin America")
        # Los países tienen ISO2 de 2 letras; los aggregates suelen tener más.
        if len(country_iso2) != 2:
            continue

        # Solo países de nuestra lista
        if country_name not in LATAM_COUNTRIES:
            continue

        try:
            year = int(r.get("date", 0))
        except (ValueError, TypeError):
            continue

        if not (YEAR_START <= year <= YEAR_END):
            continue

        value = r.get("value")  # puede ser None (null en la API)

        rows.append({
            "country": country_name,
            "country_code": COUNTRY_ISO3.get(country_name, r.get("countryiso3code", "")),
            "year": year,
            col_name: float(value) if value is not None else None,
        })

    return pd.DataFrame(rows)


def build_combined_df(all_dfs: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Hace merge de todos los indicadores en un único DataFrame por (country, year).
    Usa outer join para no perder países/años con datos parciales.
    """
    if not all_dfs:
        return pd.DataFrame()

    dfs = list(all_dfs.values())
    combined = dfs[0]

    for df in dfs[1:]:
        combined = pd.merge(
            combined,
            df,
            on=["country", "country_code", "year"],
            how="outer",
        )

    combined = combined.sort_values(["country_code", "year"]).reset_index(drop=True)
    return combined


def validate(df: pd.DataFrame) -> bool:
    """Validaciones mínimas de calidad antes de subir a S3."""
    errors = []

    if df.empty:
        errors.append("DataFrame vacío")

    missing_countries = set(LATAM_COUNTRIES) - set(df.get("country", pd.Series()).unique())
    if missing_countries:
        logger.warning("⚠️  Países sin datos: %s", missing_countries)

    # ST.INT.ARVL suele tener ~60-70% de cobertura en LATAM
    if "tourist_arrivals" in df.columns:
        null_pct = df["tourist_arrivals"].isnull().mean() * 100
        if null_pct > 60:
            errors.append(f"tourist_arrivals tiene {null_pct:.1f}% nulos (umbral: 60%)")
            logger.warning("   Nota: World Bank tiene cobertura parcial para algunos países")

    if errors:
        for e in errors:
            logger.error("❌ Validación fallida: %s", e)
        return False

    logger.info("✅ Validaciones OK")
    return True


def df_to_csv_bytes(df: pd.DataFrame) -> bytes:
    """Serializa el DataFrame a CSV en memoria."""
    buf = StringIO()
    df.to_csv(buf, index=False)
    return buf.getvalue().encode("utf-8")


def run(dry_run: bool = False) -> None:
    logger.info("=" * 60)
    logger.info("🚀 Iniciando ingesta World Bank Tourism")
    logger.info("   Indicadores: %s", list(WORLDBANK_INDICATORS.keys()))
    logger.info("   Países: %d | Años: %d–%d | Dry-run: %s",
                len(LATAM_COUNTRIES), YEAR_START, YEAR_END, dry_run)
    logger.info("=" * 60)

    country_iso2_list = list(COUNTRY_ISO2.values())
    all_dfs: dict[str, pd.DataFrame] = {}

    # 1. Descargar y procesar cada indicador
    for indicator_code, col_name in WORLDBANK_INDICATORS.items():
        logger.info("📥 Indicador: %s → columna '%s'", indicator_code, col_name)

        records = fetch_indicator(indicator_code, country_iso2_list)
        df_ind = parse_wb_records(records, col_name)

        if df_ind.empty:
            logger.warning("⚠️  Sin datos para %s, se omite", indicator_code)
            continue

        # Subida raw por indicador (CSV tal como viene de la API)
        raw_bytes = df_to_csv_bytes(df_ind)
        if not dry_run:
            upload_raw_to_s3(
                content=raw_bytes,
                s3_key=f"{S3_PATHS['raw_tourism']}{indicator_code}.csv",
                content_type="text/csv",
            )
        else:
            logger.info("[DRY-RUN] Saltando upload raw para %s", indicator_code)

        all_dfs[col_name] = df_ind
        # Pequeña pausa para no saturar la API
        time.sleep(0.5)

    if not all_dfs:
        logger.error("❌ No se obtuvo ningún dato del World Bank. Abortando.")
        sys.exit(1)

    # 2. Combinar todos los indicadores
    df_combined = build_combined_df(all_dfs)
    log_dataframe_summary(df_combined, "World Bank Tourism — combinado")

    # 3. Validación
    if not validate(df_combined):
        logger.error("❌ Ingesta abortada por fallo de validación")
        sys.exit(1)

    # 4. Subida a bronze/ como Parquet particionado
    if not dry_run:
        n = upload_parquet_partitioned(
            df=df_combined,
            s3_prefix=S3_PATHS["bronze_tourism"],
            partition_cols=["year", "country_code"],
        )
        logger.info("✅ Ingesta World Bank Tourism completada | %d particiones", n)
    else:
        logger.info("[DRY-RUN] DataFrame final listo para subir:")
        logger.info("\n%s", df_combined.head(10).to_string())
        logger.info("   Shape: %s | Columnas: %s", df_combined.shape, list(df_combined.columns))
        logger.info("   Particiones estimadas: %d", df_combined.groupby(["year", "country_code"]).ngroups)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingesta World Bank Tourism → S3")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Ejecuta sin subir a S3.",
    )
    args = parser.parse_args()
    run(dry_run=args.dry_run)