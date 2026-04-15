"""
tests/ingestion/test_lambda_handler.py
Tests de lógica del handler Lambda — sin S3 ni red.
Se ejecutan en el mismo job de CI que los tests de ingesta.
"""

import sys
import os
import io

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../pipelines/ingestion"))

import pandas as pd
import pytest

# Mock boto3 antes de importar el handler (no hay S3 en CI)
import unittest.mock as mock
sys.modules["boto3"] = mock.MagicMock()

from lambda_handler import (
    _parse_file,
    _normalize_columns,
    _filter_latam,
    _filter_years,
    _validate,
    LATAM_COUNTRIES,
    COUNTRY_ISO3,
    YEAR_START,
    YEAR_END,
)


# ── Fixtures de archivos mock ──────────────────────────────────────────────────
CSV_OWID = b"""country,year,co2,co2_per_capita,gdp,population
Argentina,2020,167.5,3.7,890000000000,44961000
Argentina,2021,172.3,3.8,920000000000,45276780
Brazil,2020,480.1,2.25,1440000000000,212559417
Venezuela,2015,145.2,4.9,250000000000,29955000
United States,2020,4712,14.2,21433000000000,331002647
World,2020,36700,4.7,87000000000000,7794799000
"""

CSV_TRANSPORT = b"""Entity,Code,Year,Air transport (tourists),Sea transport (tourists),Land transport (tourists)
Argentina,ARG,2020,4000000,200000,800000
Brazil,BRA,2019,3000000,500000,1500000
Germany,DEU,2020,10000000,1000000,5000000
"""

EXCEL_CONTENT_ROWS = [
    {"country": "Argentina", "year": 2020, "tourist_arrivals": 7400000},
    {"country": "Brazil",    "year": 2020, "tourist_arrivals": 6000000},
    {"country": "Chile",     "year": 2019, "tourist_arrivals": 4500000},
    {"country": "Spain",     "year": 2020, "tourist_arrivals": 83000000},
]

JSON_CONTENT = b"""[
  {"country": "Argentina", "year": 2020, "value": 100},
  {"country": "Mexico",    "year": 2021, "value": 200},
  {"country": "France",    "year": 2020, "value": 300}
]"""


# ── Tests de parseo ────────────────────────────────────────────────────────────
class TestParseFile:
    def test_parse_csv(self):
        df = _parse_file(CSV_OWID, "data.csv")
        assert isinstance(df, pd.DataFrame)
        assert "country" in df.columns
        assert len(df) == 6

    def test_parse_xlsx(self):
        import openpyxl
        buf = io.BytesIO()
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.append(list(EXCEL_CONTENT_ROWS[0].keys()))
        for row in EXCEL_CONTENT_ROWS:
            ws.append(list(row.values()))
        wb.save(buf)
        buf.seek(0)
        df = _parse_file(buf.read(), "data.xlsx")
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 4

    def test_parse_json(self):
        df = _parse_file(JSON_CONTENT, "data.json")
        assert isinstance(df, pd.DataFrame)
        assert "country" in df.columns

    def test_unsupported_format_raises(self):
        with pytest.raises(ValueError, match="Formato no soportado"):
            _parse_file(b"data", "archivo.pdf")


# ── Tests de normalización de columnas ────────────────────────────────────────
class TestNormalizeColumns:
    def test_owid_co2_known_dataset(self):
        df = _parse_file(CSV_OWID, "data.csv")
        df_norm = _normalize_columns(df, "owid_co2")
        assert "country" in df_norm.columns
        assert "year" in df_norm.columns

    def test_owid_transport_entity_column(self):
        df = _parse_file(CSV_TRANSPORT, "data.csv")
        df_norm = _normalize_columns(df, "owid_transport")
        assert "country" in df_norm.columns
        assert "year" in df_norm.columns
        # Entity debe haberse renombrado a country
        assert "Entity" not in df_norm.columns

    def test_auto_detect_pais_column(self):
        """Columna 'pais' debe detectarse automáticamente como country"""
        df = pd.DataFrame({"pais": ["Argentina", "Brazil"], "year": [2020, 2020], "val": [1, 2]})
        df_norm = _normalize_columns(df, "custom")
        assert "country" in df_norm.columns

    def test_auto_detect_año_column(self):
        """Columna 'año' debe detectarse como year"""
        df = pd.DataFrame({"country": ["Argentina"], "año": [2020], "val": [1]})
        df_norm = _normalize_columns(df, "custom")
        assert "year" in df_norm.columns

    def test_missing_country_raises(self):
        df = pd.DataFrame({"region": ["Sur"], "year": [2020]})
        with pytest.raises(ValueError, match="No se encontraron columnas"):
            _normalize_columns(df, "custom")

    def test_venezuela_alias(self):
        df = pd.DataFrame({"country": ["Venezuela, RB", "Argentina"], "year": [2020, 2020]})
        df_norm = _normalize_columns(df, "custom")
        assert "Venezuela" in df_norm["country"].values
        assert "Venezuela, RB" not in df_norm["country"].values


# ── Tests de filtrado ──────────────────────────────────────────────────────────
class TestFilters:
    def _base_df(self):
        df = _parse_file(CSV_OWID, "data.csv")
        return _normalize_columns(df, "owid_co2")

    def test_filter_latam_removes_world(self):
        df = _filter_latam(self._base_df())
        assert "World" not in df["country"].values

    def test_filter_latam_removes_usa(self):
        df = _filter_latam(self._base_df())
        assert "United States" not in df["country"].values

    def test_filter_latam_keeps_latam(self):
        df = _filter_latam(self._base_df())
        for country in ["Argentina", "Brazil"]:
            assert country in df["country"].values

    def test_filter_years_range(self):
        df = pd.DataFrame({
            "country": ["Argentina"] * 5,
            "year": [2010, 2013, 2018, 2023, 2025],
        })
        df_f = _filter_years(df)
        assert 2010 not in df_f["year"].values
        assert 2025 not in df_f["year"].values
        assert 2013 in df_f["year"].values
        assert 2023 in df_f["year"].values

    def test_filter_years_handles_non_numeric(self):
        df = pd.DataFrame({
            "country": ["Argentina", "Brazil"],
            "year": ["2020", "no_year"],
        })
        df_f = _filter_years(df)
        assert len(df_f) == 1
        assert df_f["year"].iloc[0] == 2020.0


# ── Tests de validación ───────────────────────────────────────────────────────
class TestValidate:
    def _ready_df(self):
        df = _parse_file(CSV_OWID, "data.csv")
        df = _normalize_columns(df, "owid_co2")
        df = _filter_latam(df)
        df = _filter_years(df)
        df["country_code"] = df["country"].map(COUNTRY_ISO3).fillna("UNK")
        df["year"] = df["year"].astype(int)
        return df

    def test_valid_df_passes(self):
        df = self._ready_df()
        _validate(df, "owid_co2")  # no debe lanzar excepción

    def test_empty_df_raises(self):
        with pytest.raises(ValueError, match="vacio"):
            _validate(pd.DataFrame(), "owid_co2")

    def test_duplicates_are_warned_not_raised(self):
        """En bronze los duplicados se conservan — se limpian en silver"""
        df = self._ready_df()
        df_dup = pd.concat([df, df]).reset_index(drop=True)
        # No debe lanzar excepción, solo warning
        _validate(df_dup, "owid_co2")


# ── Test de flujo completo (sin S3) ───────────────────────────────────────────
class TestFullFlow:
    def test_csv_end_to_end(self):
        raw = CSV_OWID
        filename = "owid_co2_2023.csv"
        dataset = "owid_co2"

        df = _parse_file(raw, filename)
        df = _normalize_columns(df, dataset)
        df = _filter_latam(df)
        df = _filter_years(df)

        assert not df.empty
        df["country_code"] = df["country"].map(COUNTRY_ISO3).fillna("UNK")
        df["year"] = df["year"].astype(int)
        _validate(df, dataset)

        partitions = df.groupby(["year", "country_code"]).ngroups
        assert partitions >= 1

        # Verificar estructura de partición esperada
        groups = list(df.groupby(["year", "country_code"]).groups.keys())
        assert (2020, "ARG") in groups
        assert (2020, "BRA") in groups

    def test_transport_entity_column_end_to_end(self):
        df = _parse_file(CSV_TRANSPORT, "transport.csv")
        df = _normalize_columns(df, "owid_transport")
        df = _filter_latam(df)
        df = _filter_years(df)

        assert not df.empty
        assert "Germany" not in df["country"].values
        assert "country" in df.columns
        assert "year" in df.columns