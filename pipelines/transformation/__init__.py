import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

from run_transformation import run_all as _run_all


def run_transformation():
    _run_all(dry_run=False, sources=[], local_bronze={})
