"""
tests/expectations/test_expectations.py — Tests unitarios para Great Expectations
==================================================================================
14 tests que validan funciones de utils_expectations sin S3 ni red.

Estructura:
  tests/expectations/test_expectations.py  ← AQUÍ
  pipelines/expectations/utils_expectations.py  ← importa ESTO

Ejecutar desde repo root:
  python -m pytest tests/expectations/test_expectations.py -v

O desde pipelines/:
  python -m pytest ../tests/expectations/test_expectations.py -v
"""

import os
import sys

import pandas as pd
import pytest

# Agrega pipelines/expectations/ a sys.path para poder importar directamente
_HERE = os.path.dirname(os.path.abspath(__file__))  # tests/expectations/
_PIPELINES_EXPECTATIONS = os.path.abspath(os.path.join(_HERE, "../../pipelines/expectations"))

if _PIPELINES_EXPECTATIONS not in sys.path:
    sys.path.insert(0, _PIPELINES_EXPECTATIONS)

# ✅ FIX: importar directamente (sin "pipelines.expectations." prefix)
# porque _PIPELINES_EXPECTATIONS ya está en sys.path
from utils_expectations import (
    create_report,
    get_utc_date,
    get_utc_now,
    validate_column_between,
    validate_column_in_set,
    validate_column_not_null,
    validate_no_duplicates,
    validate_table_row_count,
)

# ─── Test fixtures ───────────────────────────────────────────────────────────


@pytest.fixture
def sample_df():
    """DataFrame de prueba: 2 países × 11 años = 22 filas únicas."""
    countries = ["ARG"] * 11 + ["BRA"] * 11
    years = list(range(2013, 2024)) * 2  # 11 años por país
    return pd.DataFrame(
        {
            "country_code": countries,
            "year": years,
            "co2": [100.5 + i for i in range(22)],
            "value": [1, 2, None] * 7 + [1],  # 7 nulos
        }
    )


@pytest.fixture
def sample_df_with_dupes():
    """DataFrame con duplicados deliberados."""
    return pd.DataFrame(
        {
            "country_code": ["ARG", "ARG", "BRA"],
            "year": [2020, 2020, 2020],  # ARG-2020 duplicado
            "value": [100, 100, 200],
        }
    )


# ─── Tests: Timestamps ──────────────────────────────────────────────────────


def test_get_utc_now_format():
    """get_utc_now() retorna ISO 8601 con Z."""
    ts = get_utc_now()
    assert isinstance(ts, str)
    assert ts.endswith("Z")
    assert "T" in ts
    # Formato: "2026-04-26T15:30:00Z"
    assert len(ts.split("T")) == 2


def test_get_utc_date_format():
    """get_utc_date() retorna YYYY-MM-DD."""
    date = get_utc_date()
    assert isinstance(date, str)
    assert len(date) == 10
    assert date.count("-") == 2
    # Formato: "2026-04-26"
    parts = date.split("-")
    assert len(parts[0]) == 4  # Año
    assert len(parts[1]) == 2  # Mes
    assert len(parts[2]) == 2  # Día


# ─── Tests: Validaciones ───────────────────────────────────────────────────


def test_validate_table_row_count_exact(sample_df):
    """Valida cantidad exacta de filas."""
    result = validate_table_row_count(sample_df, expected=22)
    assert result["ok"] is True
    assert result["check"] == "table_row_count"


def test_validate_table_row_count_exact_fails(sample_df):
    """Falla si cantidad no coincide."""
    result = validate_table_row_count(sample_df, expected=50)
    assert result["ok"] is False
    assert "Expected 50 rows, got 22" in result["reason"]


def test_validate_table_row_count_min(sample_df):
    """Valida mínimo de filas."""
    result = validate_table_row_count(sample_df, min_value=10)
    assert result["ok"] is True


def test_validate_table_row_count_min_fails(sample_df):
    """Falla si no alcanza mínimo."""
    result = validate_table_row_count(sample_df, min_value=150)
    assert result["ok"] is False
    assert "Expected min 150 rows, got 22" in result["reason"]


def test_validate_column_not_null_passes(sample_df):
    """Columna sin nulos pasa validación."""
    result = validate_column_not_null(sample_df, column="country_code")
    assert result["ok"] is True
    assert result["column"] == "country_code"


def test_validate_column_not_null_fails(sample_df):
    """Columna con nulos falla validación."""
    result = validate_column_not_null(sample_df, column="value")
    assert result["ok"] is False
    assert "nulls" in result["reason"].lower()


def test_validate_no_duplicates_passes(sample_df):
    """Sin duplicados pasa validación."""
    result = validate_no_duplicates(sample_df, subset=["country_code", "year"])
    assert result["ok"] is True


def test_validate_no_duplicates_fails(sample_df_with_dupes):
    """Con duplicados falla validación."""
    result = validate_no_duplicates(sample_df_with_dupes, subset=["country_code", "year"])
    assert result["ok"] is False
    assert "duplicate" in result["reason"].lower()


def test_validate_column_in_set_passes(sample_df):
    """Valores en set permitido pasan."""
    result = validate_column_in_set(sample_df, column="country_code", value_set={"ARG", "BRA", "CHL"})
    assert result["ok"] is True


def test_validate_column_in_set_fails():
    """Valores fuera del set fallan."""
    df = pd.DataFrame({"status": ["high", "low", "invalid"]})
    result = validate_column_in_set(df, column="status", value_set={"high", "medium", "low"})
    assert result["ok"] is False
    assert "invalid" in str(result["reason"]).lower()


def test_validate_column_between_passes(sample_df):
    """Valores en rango pasan."""
    result = validate_column_between(sample_df, column="co2", min_value=100.0, max_value=200.0)
    assert result["ok"] is True


def test_validate_column_between_fails(sample_df):
    """Valores fuera de rango fallan."""
    result = validate_column_between(sample_df, column="co2", min_value=0.0, max_value=50.0)
    assert result["ok"] is False
    assert "values outside" in result["reason"]


# ─── Tests: Reportes ───────────────────────────────────────────────────────


def test_create_report_structure():
    """create_report() genera estructura correcta."""
    report = create_report(
        dataset="co2_emissions",
        layer="bronze",
        total_checks=5,
        passed=4,
        failed=1,
        failures=[{"check": "test", "reason": "failed"}],
        table_stats={"rows": 100, "cols": 8},
    )

    assert report["dataset"] == "co2_emissions"
    assert report["layer"] == "bronze"
    assert report["total_checks"] == 5
    assert report["passed"] == 4
    assert report["failed"] == 1
    assert "timestamp" in report
    assert report["summary"] == "⚠️ 1/5 checks FAILED"
    assert report["table_stats"]["rows"] == 100


def test_create_report_all_passed():
    """Summary correcto cuando todos los checks pasan."""
    report = create_report(
        dataset="test",
        layer="silver",
        total_checks=3,
        passed=3,
        failed=0,
        failures=[],
        table_stats={"rows": 50, "cols": 5},
    )

    assert report["summary"] == "3/3 checks OK"
    assert report["failed"] == 0
