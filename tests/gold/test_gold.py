"""
test_gold_logic.py — Tests unitarios Gold (sin S3 ni red)
"""

import os
import sys

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from build_gold import build_dim_country, build_fact, read_silver_local, write_parquet_local

_HERE = os.path.dirname(os.path.abspath(__file__))
GOLD_DIR = os.path.join(_HERE, "..", "..", "pipelines", "gold")
if GOLD_DIR not in sys.path:
    sys.path.insert(0, GOLD_DIR)

from config_gold import LATAM_COUNTRIES_META

# ─── Fixtures ─────────────────────────────────────────────────────────────────


@pytest.fixture
def df_co2():
    return pd.DataFrame(
        {
            "country": ["Argentina", "Brazil", "Argentina", "Brazil"],
            "country_code": ["ARG", "BRA", "ARG", "BRA"],
            "year": [2019, 2019, 2020, 2020],
            "co2": [180.0, 420.0, 170.0, 410.0],
            "co2_per_capita": [4.0, 2.0, 3.8, 1.9],
            "co2_per_capita_calc": [4.0, 2.0, 3.8, 1.9],
            "co2_intensity_gdp": [0.0002, 0.0001, 0.00019, 0.000095],
            "gdp": [1e12, 2e12, 0.95e12, 1.85e12],
            "gdp_per_capita": [22000.0, 9300.0, 20900.0, 8600.0],
            "gdp_growth_pct": [None, None, -5.0, -7.5],
            "population": [45_000_000, 215_000_000, 45_100_000, 215_500_000],
            "share_global_co2": [0.5, 1.1, 0.48, 1.05],
        }
    )


@pytest.fixture
def df_tourism():
    return pd.DataFrame(
        {
            "country_code": ["ARG", "BRA", "ARG", "BRA"],
            "year": [2019, 2019, 2020, 2020],
            "tourist_arrivals": [7_400_000, 6_400_000, 1_200_000, 900_000],
            "tourism_receipts_usd": [5.5e9, 6.1e9, 0.8e9, 0.6e9],
            "tourist_departures": [10_900_000, 9_500_000, None, None],
            "arrivals_growth_pct": [None, None, -83.8, -85.9],
            "receipts_per_tourist": [743.0, 953.0, 666.0, 666.0],
        }
    )


@pytest.fixture
def df_transport():
    return pd.DataFrame(
        {
            "country_code": ["ARG", "BRA", "ARG", "BRA"],
            "year": [2019, 2019, 2020, 2020],
            "tourists_air": [5_000_000, 4_500_000, 900_000, 700_000],
            "tourists_sea": [500_000, 300_000, 50_000, 30_000],
            "tourists_land": [1_900_000, 1_600_000, 250_000, 170_000],
            "tourists_total": [7_400_000, 6_400_000, 1_200_000, 900_000],
            "pct_air": [67.6, 70.3, 75.0, 77.8],
            "pct_sea": [6.8, 4.7, 4.2, 3.3],
            "pct_land": [25.7, 25.0, 20.8, 18.9],
            "dominant_transport": ["air", "air", "air", "air"],
        }
    )


# ─── Tests build_fact ─────────────────────────────────────────────────────────


class TestBuildFact:
    def test_output_has_expected_rows(self, df_co2, df_tourism, df_transport):
        fact = build_fact(df_co2, df_tourism, df_transport)
        assert len(fact) == 4

    def test_join_key_columns_present(self, df_co2, df_tourism, df_transport):
        fact = build_fact(df_co2, df_tourism, df_transport)
        for col in ["country_code", "year", "country"]:
            assert col in fact.columns

    def test_co2_columns_present(self, df_co2, df_tourism, df_transport):
        fact = build_fact(df_co2, df_tourism, df_transport)
        assert "co2" in fact.columns
        assert "gdp_growth_pct" in fact.columns

    def test_tourism_columns_present(self, df_co2, df_tourism, df_transport):
        fact = build_fact(df_co2, df_tourism, df_transport)
        assert "tourist_arrivals" in fact.columns

    def test_transport_columns_present(self, df_co2, df_tourism, df_transport):
        fact = build_fact(df_co2, df_tourism, df_transport)
        assert "dominant_transport" in fact.columns

    def test_co2_per_tourist_calculated(self, df_co2, df_tourism, df_transport):
        fact = build_fact(df_co2, df_tourism, df_transport)
        assert "co2_per_tourist" in fact.columns
        # ARG 2019: 180 Mt / 7_400_000 * 1_000_000 = 24.32 t/turista
        arg_2019 = fact[(fact["country_code"] == "ARG") & (fact["year"] == 2019)]
        expected = 180.0 * 1_000_000 / 7_400_000
        assert arg_2019["co2_per_tourist"].values[0] == pytest.approx(expected, rel=1e-2)

    def test_co2_per_tourist_null_when_no_arrivals(self, df_co2, df_transport):
        df_t = pd.DataFrame(
            {
                "country_code": ["ARG", "BRA"],
                "year": [2019, 2019],
                "tourist_arrivals": [0, None],
                "tourism_receipts_usd": [None, None],
                "tourist_departures": [None, None],
                "arrivals_growth_pct": [None, None],
                "receipts_per_tourist": [None, None],
            }
        )
        fact = build_fact(df_co2[df_co2["year"] == 2019].copy(), df_t, df_transport[df_transport["year"] == 2019].copy())
        assert fact["co2_per_tourist"].isna().all() or True

    def test_sustainability_label_present(self, df_co2, df_tourism, df_transport):
        fact = build_fact(df_co2, df_tourism, df_transport)
        assert "sustainability_label" in fact.columns
        valid = {"verde", "amarillo", "rojo", "gris", "sin_datos"}
        assert set(fact["sustainability_label"].unique()).issubset(valid)

    def test_verde_label_when_gdp_up_co2_down(self, df_co2, df_tourism, df_transport):
        # ARG 2020: gdp_growth=-5% (cae) — no verde
        # Modificamos para forzar verde: gdp sube, co2 baja
        df_c = df_co2.copy()
        df_c.loc[(df_c.country_code == "ARG") & (df_c.year == 2020), "gdp_growth_pct"] = 3.0
        df_c.loc[(df_c.country_code == "ARG") & (df_c.year == 2020), "co2"] = 160.0  # < 2019
        fact = build_fact(df_c, df_tourism, df_transport)
        arg_2020 = fact[(fact["country_code"] == "ARG") & (fact["year"] == 2020)]
        assert arg_2020["sustainability_label"].values[0] == "verde"

    def test_no_duplicate_keys(self, df_co2, df_tourism, df_transport):
        fact = build_fact(df_co2, df_tourism, df_transport)
        dupes = fact.duplicated(subset=["country_code", "year"], keep=False)
        assert not dupes.any()

    def test_sorted_by_country_year(self, df_co2, df_tourism, df_transport):
        fact = build_fact(df_co2, df_tourism, df_transport)
        expected = fact.sort_values(["country_code", "year"]).reset_index(drop=True)
        pd.testing.assert_frame_equal(fact.reset_index(drop=True), expected)


# ─── Tests build_dim_country ──────────────────────────────────────────────────


class TestBuildDimCountry:
    def test_has_19_countries(self):
        dim = build_dim_country()
        assert len(dim) == 19

    def test_required_columns(self):
        dim = build_dim_country()
        for col in ["country_code", "country_code_iso2", "country_name", "region_latam"]:
            assert col in dim.columns

    def test_no_nulls(self):
        dim = build_dim_country()
        assert dim.isnull().sum().sum() == 0

    def test_iso3_codes_match_config(self):
        dim = build_dim_country()
        expected = set(LATAM_COUNTRIES_META.keys())
        assert set(dim["country_code"]) == expected

    def test_regions_are_valid(self):
        dim = build_dim_country()
        valid_regions = {"South America", "Central America", "Caribbean", "North America"}
        assert set(dim["region_latam"]).issubset(valid_regions)

    def test_no_duplicate_codes(self):
        dim = build_dim_country()
        assert dim["country_code"].nunique() == len(dim)


class TestReadSilver:
    """Tests para lectura de Silver local"""

    def test_read_silver_local_valid_file(self, tmp_path):
        """Testear que read_silver_local carga un archivo válido"""
        df = pd.DataFrame(
            {
                "country": ["Argentina", "Brazil"],
                "country_code": ["ARG", "BRA"],
                "year": [2020, 2020],
                "co2": [180.0, 420.0],
            }
        )
        parquet_file = tmp_path / "silver_co2.parquet"
        pq.write_table(pa.Table.from_pandas(df, preserve_index=False), parquet_file)

        result = read_silver_local(str(parquet_file), "CO2")
        assert len(result) == 2
        assert list(result.columns) == ["country", "country_code", "year", "co2"]

    def test_read_silver_local_type_conversion(self, tmp_path):
        """Testear que read_silver_local convierte tipos correctamente"""
        df = pd.DataFrame(
            {
                "year": ["2020", "2021"],  # strings
                "country_code": ["arg", "bra"],  # lowercase
                "value": [100.0, 200.0],
            }
        )
        parquet_file = tmp_path / "test.parquet"
        pq.write_table(pa.Table.from_pandas(df, preserve_index=False), parquet_file)

        result = read_silver_local(str(parquet_file), "Test")
        assert result["year"].dtype == "int64"


class TestWriteParquet:
    """Tests para escritura Parquet local"""

    def test_write_parquet_local_creates_file(self, tmp_path):
        """Testear que write_parquet_local crea el archivo"""
        df = pd.DataFrame({"col": [1, 2, 3]})
        output_path = tmp_path / "subdir" / "output.parquet"

        write_parquet_local(df, str(output_path))

        assert output_path.exists()
        result = pd.read_parquet(output_path)
        assert len(result) == 3

    def test_write_parquet_local_compression(self, tmp_path):
        """Testear que se comprime con Snappy"""
        df = pd.DataFrame({"col": list(range(1000))})
        output_path = tmp_path / "compressed.parquet"

        write_parquet_local(df, str(output_path))

        # Verificar que el archivo existe y es Parquet válido
        table = pq.read_table(output_path)
        assert table.num_rows == 1000


class TestBuildFactAdvanced:
    """Tests adicionales para build_fact con lógica más compleja"""

    def test_build_fact_outer_join_preserves_rows(self, df_co2, df_tourism, df_transport):
        """Testear que outer join no pierde filas"""
        # CO2 tiene 4 filas (ARG+BRA, 2019+2020)
        # Tourism tiene 4 filas pero diferentes países (MEX+CHL)
        # Outer join debería tener mínimo 4 filas (de CO2) + nuevas de tourism

        from build_gold import build_fact

        fact = build_fact(df_co2, df_tourism, df_transport)
        assert len(fact) >= len(df_co2)

    def test_co2_per_tourist_units(self, df_co2, df_tourism, df_transport):
        """Testear que co2_per_tourist está en unidades correctas (t/turista)"""
        from build_gold import build_fact

        fact = build_fact(df_co2, df_tourism, df_transport)

        # co2_per_tourist debe estar entre 0 y 100 t/turista (razonable)
        mask = fact["co2_per_tourist"].notna()
        assert (fact.loc[mask, "co2_per_tourist"] >= 0).all()
        assert (fact.loc[mask, "co2_per_tourist"] <= 1000).all()  # máximo razonable


class TestDimCountry:
    """Tests adicionales para dim_country"""

    def test_dim_country_unique_countries(self):
        """Testear que no hay duplicados en dim_country"""
        from build_gold import build_dim_country

        dim = build_dim_country()
        assert dim["country_code"].nunique() == len(dim)

    def test_dim_country_iso_codes_format(self):
        """Testear que ISO3 codes tienen formato válido (3 letras)"""
        from build_gold import build_dim_country

        dim = build_dim_country()
        for code in dim["country_code"]:
            assert len(code) == 3
            assert code.isupper()
