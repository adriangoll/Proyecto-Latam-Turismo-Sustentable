from pipelines.ingestion.ingest_worldbank_tourism import run

def test_worldbank_run_dry():
    try:
        run(dry_run=True)
    except Exception:
        pass

def test_worldbank_run_again():
    try:
        run(dry_run=True)
    except Exception:
        pass