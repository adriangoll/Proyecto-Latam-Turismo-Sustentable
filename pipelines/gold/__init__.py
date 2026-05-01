import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

from run_gold import run_all as _run_all


def run_gold():
    _run_all(dry_run=False)
