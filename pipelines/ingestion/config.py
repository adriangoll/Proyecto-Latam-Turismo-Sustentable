"""
config.py — Configuración central del pipeline de ingesta
Grupo 1 — LATAM Sustainability Data Lake
"""

# ─── Años de interés ────────────────────────────────────────────────────────
YEAR_START = 2013
YEAR_END = 2023
YEARS = list(range(YEAR_START, YEAR_END + 1))

# ─── Países LATAM ────────────────────────────────────────────────────────────
# Nombres tal como aparecen en UNWTO y World Bank
LATAM_COUNTRIES = [
    "Argentina",
    "Bolivia",
    "Brazil",
    "Chile",
    "Colombia",
    "Costa Rica",
    "Cuba",
    "Dominican Republic",
    "Ecuador",
    "El Salvador",
    "Guatemala",
    "Honduras",
    "Mexico",
    "Nicaragua",
    "Panama",
    "Paraguay",
    "Peru",
    "Uruguay",
    "Venezuela",
]

# ISO 3166-1 alpha-3 → para particionamiento en S3 y joins
COUNTRY_ISO3 = {
    "Argentina": "ARG",
    "Bolivia": "BOL",
    "Brazil": "BRA",
    "Chile": "CHL",
    "Colombia": "COL",
    "Costa Rica": "CRI",
    "Cuba": "CUB",
    "Dominican Republic": "DOM",
    "Ecuador": "ECU",
    "El Salvador": "SLV",
    "Guatemala": "GTM",
    "Honduras": "HND",
    "Mexico": "MEX",
    "Nicaragua": "NIC",
    "Panama": "PAN",
    "Paraguay": "PRY",
    "Peru": "PER",
    "Uruguay": "URY",
    "Venezuela": "VEN",
}

# ISO 3166-1 alpha-2 → requerido por la World Bank API
COUNTRY_ISO2 = {
    "Argentina": "AR",
    "Bolivia": "BO",
    "Brazil": "BR",
    "Chile": "CL",
    "Colombia": "CO",
    "Costa Rica": "CR",
    "Cuba": "CU",
    "Dominican Republic": "DO",
    "Ecuador": "EC",
    "El Salvador": "SV",
    "Guatemala": "GT",
    "Honduras": "HN",
    "Mexico": "MX",
    "Nicaragua": "NI",
    "Panama": "PA",
    "Paraguay": "PY",
    "Peru": "PE",
    "Uruguay": "UY",
    "Venezuela": "VE",
}

# ─── Nombres alternativos en fuentes externas ─────────────────────────────────
# Algunos datasets usan variantes de nombre. Normalizamos a nuestro estándar.
NAME_ALIASES = {
    "Venezuela, RB": "Venezuela",
    "Venezuela (Bolivarian Republic of)": "Venezuela",
    "Bolivia (Plurinational State of)": "Bolivia",
    "Bolivia, Plurinational State of": "Bolivia",
    "Dominican Rep.": "Dominican Republic",
    "República Dominicana": "Dominican Republic",
    "Brasil": "Brazil",
    "México": "Mexico",
    "Panamá": "Panama",
    "Perú": "Peru",
}

# ─── S3 ───────────────────────────────────────────────────────────────────────
S3_BUCKET = "latam-sustainability-datalake"

S3_PATHS = {
    # Raw: archivo original tal como viene de la fuente
    "raw_co2": "raw/owid_co2/owid-co2-data.csv",
    "raw_tourism": "raw/worldbank_tourism/",  # un CSV por indicador
    "raw_transport": "raw/unwto_transport/unwto_transport.xlsx",
    # Bronze: particionado por year= / country_code=, formato Parquet
    "bronze_co2": "bronze/co2_emissions/",
    "bronze_tourism": "bronze/tourism_arrivals/",
    "bronze_transport": "bronze/transport_mode/",
}

# ─── URLs de las fuentes ──────────────────────────────────────────────────────
OWID_CO2_URL = (
    "https://raw.githubusercontent.com/owid/co2-data/master/owid-co2-data.csv"
)

UNWTO_TRANSPORT_URL = "https://pre-webunwto.s3.amazonaws.com/s3fs-public/2025-12/UN_Tourism_inbound_arrivals_by_transport_12_2025.xlsx"

# World Bank — indicadores requeridos
WORLDBANK_INDICATORS = {
    "ST.INT.ARVL": "tourist_arrivals",  # llegadas de turistas internacionales
    "ST.INT.RCPT.CD": "tourism_receipts_usd",  # ingresos por turismo (USD corrientes)
    "ST.INT.DPRT": "tourist_departures",  # salidas de turistas
}

WORLDBANK_BASE_URL = "https://api.worldbank.org/v2"
