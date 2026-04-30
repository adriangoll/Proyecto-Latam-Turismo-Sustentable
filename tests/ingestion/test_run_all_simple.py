from pipelines.ingestion.run_ingestion import run_all


def test_run_all_co2():
    run_all(dry_run=True, sources=["co2"])


def test_run_all_transport():
    run_all(dry_run=True, sources=["transport"])


def test_run_all_tourism():
    run_all(dry_run=True, sources=["tourism"])
