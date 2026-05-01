import os
import sys

from run_transformation import run_all as _run_all

_airflow_home = os.getenv("AIRFLOW_HOME", "/opt/airflow")
_expectations_path = os.path.join(_airflow_home, "pipelines", "expectations")
if _expectations_path not in sys.path:
    sys.path.insert(0, _expectations_path)



def run_transformation():
    _run_all(dry_run=False, sources=[], local_bronze={})
