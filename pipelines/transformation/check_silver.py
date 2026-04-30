"""
check_silver.py — Verifica cobertura de los 3 datasets Silver.
Correr desde pipelines/transformation/
"""

import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

import pandas as pd
import s3fs
from config_silver import LATAM_ISO3

fs = s3fs.S3FileSystem()

DATASETS = {
    "CO2": "latam-sustainability-datalake/silver/co2_emissions/data.parquet",
    "Tourism": "latam-sustainability-datalake/silver/tourism_arrivals/data.parquet",
    "Transport": "latam-sustainability-datalake/silver/transport_mode/data.parquet",
}

for name, path in DATASETS.items():
    with fs.open(path) as f:
        df = pd.read_parquet(f)

    years = sorted(df["year"].unique().tolist())
    countries_present = set(df["country_code"].unique())
    missing_countries = sorted(LATAM_ISO3 - countries_present)

    by_country = df.groupby("country_code")["year"].count()
    incomplete = by_country[by_country < 11].sort_values()

    print(f"\n{'=' * 50}")
    print(f"  {name} — {len(df)} filas")
    print(f"{'=' * 50}")
    print(f"  Anos cubiertos : {years}")
    print(f"  Paises totales : {len(countries_present)}/19")

    if missing_countries:
        print(f"  Sin ningun dato: {missing_countries}")
    else:
        print("  Sin ningun dato: ninguno")

    if not incomplete.empty:
        print("  Paises con anos faltantes:")
        for iso, count in incomplete.items():
            missing_years = sorted(set(range(2013, 2024)) - set(df[df["country_code"] == iso]["year"]))
            print(f"    {iso}: {count}/11 anos — faltan {missing_years}")
    else:
        print("  Cobertura de anos: completa en todos los paises")
