"""
tests/conftest.py — Configuración global de pytest
==================================================
Asegura que sys.path incluya pipelines/expectations/ para que los tests
puedan importar utils_expectations sin problemas.

Ejecutar desde:
  pytest tests/expectations/test_expectations.py -v
"""

import sys
import os

# Agregar pipelines/expectations a sys.path antes de que pytest cargue los tests
_repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_pipelines_expectations = os.path.join(_repo_root, "pipelines/expectations")

if _pipelines_expectations not in sys.path:
    sys.path.insert(0, _pipelines_expectations)