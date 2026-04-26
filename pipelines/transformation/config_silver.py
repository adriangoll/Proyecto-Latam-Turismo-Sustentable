"""
config_silver.py — Constantes y reglas para la capa Silver
===========================================================
Define:
  - Paths S3 de entrada (Bronze) y salida (Silver)
  - Esquemas esperados por dataset (columnas, tipos)
  - Reglas de calidad (umbrales de nulls, rangos válidos)
  - Columnas clave para deduplicación
"""

# ─── Bucket y prefijos S3 ────────────────────────────────────────────────────

S3_BUCKET = "latam-sustainability-datalake"

S3_BRONZE = {
    "co2": f"s3://{S3_BUCKET}/bronze/co2_emissions/",
    "tourism": f"s3://{S3_BUCKET}/bronze/tourism_arrivals/",
    "transport": f"s3://{S3_BUCKET}/bronze/transport_mode/",
}

S3_SILVER = {
    "co2": f"s3://{S3_BUCKET}/silver/co2_emissions/",
    "tourism": f"s3://{S3_BUCKET}/silver/tourism_arrivals/",
    "transport": f"s3://{S3_BUCKET}/silver/transport_mode/",
}

S3_QUALITY_REPORTS = f"s3://{S3_BUCKET}/quality_reports/silver/"

# ─── Rango temporal válido ────────────────────────────────────────────────────

YEAR_START = 2013
YEAR_END = 2023

# ─── Países LATAM esperados (ISO3) ───────────────────────────────────────────

LATAM_ISO3 = {
    "ARG",
    "BOL",
    "BRA",
    "CHL",
    "COL",
    "CRI",
    "CUB",
    "DOM",
    "ECU",
    "SLV",
    "GTM",
    "HND",
    "MEX",
    "NIC",
    "PAN",
    "PRY",
    "PER",
    "URY",
    "VEN",
}

# ─── Clave de deduplicación (común a todos los datasets) ─────────────────────

DEDUP_KEY = ["country_code", "year"]

# ─── Esquemas Bronze → Silver ─────────────────────────────────────────────────
# Cada entrada define:
#   "required"  : columnas que DEBEN existir (aborta si faltan)
#   "numeric"   : columnas a castear a float
#   "drop_null_if_all" : drop fila si TODAS estas columnas son null
#   "fill_forward"     : columnas donde se aplica ffill por país (ej. población)
#   "interpolate"      : columnas donde se interpola linealmente por país

SCHEMA_CO2 = {
    "required": ["country", "country_code", "year", "co2"],
    "numeric": [
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
    ],
    "drop_null_if_all": ["co2", "gdp", "population"],
    "fill_forward": ["population"],
    "interpolate": ["gdp"],
}

SCHEMA_TOURISM = {
    "required": ["country", "country_code", "year", "tourist_arrivals"],
    "numeric": ["tourist_arrivals", "tourism_receipts_usd", "tourist_departures"],
    "drop_null_if_all": ["tourist_arrivals"],
    "fill_forward": [],
    "interpolate": [],
}

SCHEMA_TRANSPORT = {
    "required": ["country", "country_code", "year"],
    "numeric": [
        "tourists_air",
        "tourists_sea",
        "tourists_land",
        "tourists_total",
        "pct_air",
        "pct_sea",
        "pct_land",
    ],
    "drop_null_if_all": ["tourists_air", "tourists_sea", "tourists_land"],
    "fill_forward": [],
    "interpolate": [],
}

# ─── Reglas de calidad (umbrales de nulls por columna crítica) ────────────────
# Si el % de nulls supera el umbral → WARNING en el reporte (no aborta)

QUALITY_THRESHOLDS = {
    "co2": {
        "co2": 30.0,  # % máximo de nulls aceptado
        "gdp": 40.0,
        "population": 10.0,
    },
    "tourism": {
        "tourist_arrivals": 40.0,
        "tourism_receipts_usd": 60.0,
    },
    "transport": {
        "tourists_total": 60.0,  # dataset tiene cobertura parcial por diseño
    },
}
