"""
config_gold.py — Constantes para la capa Gold.
"""

S3_BUCKET = "latam-sustainability-datalake"

S3_SILVER = {
    "co2": f"s3://{S3_BUCKET}/silver/co2_emissions/data.parquet",
    "tourism": f"s3://{S3_BUCKET}/silver/tourism_arrivals/data.parquet",
    "transport": f"s3://{S3_BUCKET}/silver/transport_mode/data.parquet",
}

S3_GOLD = {
    "fact": f"s3://{S3_BUCKET}/gold/fact_tourism_emissions/data.parquet",
    "dim_country": f"s3://{S3_BUCKET}/gold/dim_country/data.parquet",
}

S3_QUALITY_REPORTS = f"s3://{S3_BUCKET}/quality_reports/gold/"
S3_OPEN_DATA_GOLD = f"s3://{S3_BUCKET}/open-data/v1/gold/"

YEAR_START = 2013
YEAR_END = 2023

LATAM_COUNTRIES_META = {
    "ARG": {"name": "Argentina", "iso2": "AR", "region": "South America"},
    "BOL": {"name": "Bolivia", "iso2": "BO", "region": "South America"},
    "BRA": {"name": "Brazil", "iso2": "BR", "region": "South America"},
    "CHL": {"name": "Chile", "iso2": "CL", "region": "South America"},
    "COL": {"name": "Colombia", "iso2": "CO", "region": "South America"},
    "CRI": {"name": "Costa Rica", "iso2": "CR", "region": "Central America"},
    "CUB": {"name": "Cuba", "iso2": "CU", "region": "Caribbean"},
    "DOM": {"name": "Dominican Republic", "iso2": "DO", "region": "Caribbean"},
    "ECU": {"name": "Ecuador", "iso2": "EC", "region": "South America"},
    "SLV": {"name": "El Salvador", "iso2": "SV", "region": "Central America"},
    "GTM": {"name": "Guatemala", "iso2": "GT", "region": "Central America"},
    "HND": {"name": "Honduras", "iso2": "HN", "region": "Central America"},
    "MEX": {"name": "Mexico", "iso2": "MX", "region": "North America"},
    "NIC": {"name": "Nicaragua", "iso2": "NI", "region": "Central America"},
    "PAN": {"name": "Panama", "iso2": "PA", "region": "Central America"},
    "PRY": {"name": "Paraguay", "iso2": "PY", "region": "South America"},
    "PER": {"name": "Peru", "iso2": "PE", "region": "South America"},
    "URY": {"name": "Uruguay", "iso2": "UY", "region": "South America"},
    "VEN": {"name": "Venezuela", "iso2": "VE", "region": "South America"},
}
