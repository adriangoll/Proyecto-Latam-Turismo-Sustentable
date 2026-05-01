from pipelines.transformation.run_transformation import run_all as _run_all

def run_transformation():
    _run_all(dry_run=False, sources=[], local_bronze={})