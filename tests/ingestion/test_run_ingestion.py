import sys
import types
import pytest

# Import del runner
from pipelines.ingestion import run_ingestion


def test_run_all_dry_run_ok(monkeypatch):
    """
    Verifica que run_all ejecuta todas las fuentes en modo dry-run
    sin errores.
    """

    # Creamos módulos mock con función run()
    mock_module = types.SimpleNamespace(run=lambda dry_run: None)

    # Reemplazamos SOURCES por mocks
    monkeypatch.setattr(
        run_ingestion,
        "SOURCES",
        {
            "co2": (mock_module, "CO2"),
            "worldbank": (mock_module, "WorldBank"),
            "transport": (mock_module, "Transport"),
        },
    )

    # Ejecutamos
    run_ingestion.run_all(dry_run=True, sources=[])


def test_run_all_with_error(monkeypatch):
    """
    Verifica que si una fuente falla, el proceso termina con exit(1)
    """

    def failing_run(dry_run):
        raise Exception("boom")

    mock_ok = types.SimpleNamespace(run=lambda dry_run: None)
    mock_fail = types.SimpleNamespace(run=failing_run)

    monkeypatch.setattr(
        run_ingestion,
        "SOURCES",
        {
            "co2": (mock_ok, "CO2"),
            "worldbank": (mock_fail, "WorldBank"),
        },
    )

    with pytest.raises(SystemExit) as e:
        run_ingestion.run_all(dry_run=True, sources=[])

    assert e.value.code == 1