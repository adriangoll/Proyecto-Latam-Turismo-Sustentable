# 🐛 Problemas encontrados y soluciones — Pipeline de Validaciones

Registro de errores reales encontrados durante la implementación del módulo Great Expectations y sus soluciones.

---

## 1. `ValueError: All arrays must be of the same length` — Fixture de tests

**Contexto:** `tests/expectations/test_expectations.py`

**Causa:** El fixture `sample_df` definía `year` con `range(2013, 2063)` = 50 items, pero las otras columnas tenían 100 items.

**Solución:** Rediseñar el fixture para que sea consistente y realista (2 países × 11 años = 22 filas únicas):

```python
@pytest.fixture
def sample_df():
    return pd.DataFrame({
        "country_code": ["ARG"] * 11 + ["BRA"] * 11,
        "year": list(range(2013, 2024)) * 2,
        "co2": [100.5 + i for i in range(22)],
        "value": [1, 2, None] * 7 + [1],
    })
```

---

## 2. `ValueError: @pytest.fixture aplicado más de una vez` — Fixture duplicado

**Contexto:** `test_expectations.py`

**Causa:** Al editar el archivo quedaron dos definiciones de `sample_df` con `@pytest.fixture`.

**Solución:** Eliminar la definición duplicada — dejar solo una.

---

## 3. `AssertionError: '⚠️ 1 checks FAILED' != '⚠️ 1/5 checks FAILED'` — Summary incompleto

**Contexto:** `utils_expectations.py` → función `create_report()`

**Causa:** El summary del caso fallido no incluía `total_checks`.

**Solución:**
```python
# ❌ Antes
summary = f"⚠️ {failed} checks FAILED"

# ✅ Después
summary = f"⚠️ {failed}/{total_checks} checks FAILED"
```

---

## 4. Import incorrecto en tests — `ModuleNotFoundError`

**Contexto:** `test_expectations.py`

**Causa:** Se agregaba `pipelines/expectations/` al `sys.path` pero se importaba con el path completo `from pipelines.expectations.utils_expectations import ...`, lo cual es contradictorio.

**Solución:** Si el directorio está en `sys.path`, importar directamente:
```python
# ✅ Correcto
sys.path.insert(0, _PIPELINES_EXPECTATIONS)
from utils_expectations import create_report, validate_table_row_count, ...
```

---

## 5. `io.UnsupportedOperation: seek` — Lectura de Parquet desde S3

**Contexto:** `utils_expectations.py` → `read_parquet_s3()`, Silver y Gold

**Causa:** `boto3` devuelve un `StreamingBody` que no soporta `seek()`, necesario para `pyarrow`.

**Solución:** Leer todo el stream a memoria con `BytesIO` antes de pasarlo a pandas:
```python
import io

# ❌ Antes
return pd.read_parquet(obj["Body"])

# ✅ Después
return pd.read_parquet(io.BytesIO(obj["Body"].read()))
```

---

## 6. `UnboundLocalError: df` — Variable no asignada en Bronze

**Contexto:** `bronze_expectations.py`

**Causa:** El bloque `else` de lectura S3 tenía solo un comentario y `pass`, nunca asignaba `df`. Al llegar a `len(df)` más abajo, la variable no existía.

**Solución:** Implementar la lectura real en el bloque `else`:
```python
else:
    s3_uri = f"s3://latam-sustainability-datalake/bronze/{dataset_name}/"
    dataset = ds.dataset(s3_uri, format="parquet", partitioning="hive")
    df = dataset.to_table().to_pandas()
```

---

## 7. `NoSuchKey` — Ruta S3 incorrecta en Bronze

**Contexto:** `bronze_expectations.py`

**Causa:** El código intentaba leer `bronze/co2_emissions/data.parquet` pero Bronze está **particionado** — no existe un archivo único.

**Diagnóstico:**
```bash
aws s3 ls s3://latam-sustainability-datalake/bronze/co2_emissions/
# Resultado: PRE year=2013/, PRE year=2014/, ...
```

**Solución:** Usar `pyarrow.dataset` con particionado hive en vez de `get_object`:
```python
import pyarrow.dataset as ds

s3_uri = f"s3://latam-sustainability-datalake/bronze/{dataset_name}/"
dataset = ds.dataset(s3_uri, format="parquet", partitioning="hive", schema=schema_override)
df = dataset.to_table().to_pandas()
```

---

## 8. `ArrowTypeError: Field year has incompatible types: int64 vs int32`

**Contexto:** `bronze_expectations.py` — lectura de datos particionados

**Causa:** Distintas particiones (`year=2013/`, `year=2014/`, etc.) guardaron el campo `year` con tipos diferentes (`int32` vs `int64`). PyArrow no puede mergearlos automáticamente.

**Solución:** Pasar un schema explícito al leer el dataset para forzar `year` a `int64` desde el inicio:
```python
import pyarrow as pa

schema_override = pa.schema([
    ("country", pa.string()),
    ("year", pa.int64()),          # ← forzar int64
    ("co2", pa.float64()),
    ("co2_per_capita", pa.float64()),
    ("co2_per_gdp", pa.float64()),
    ("cumulative_co2", pa.float64()),
    ("methane", pa.float64()),
    ("nitrous_oxide", pa.float64()),
    ("gdp", pa.float64()),
    ("population", pa.float64()),
    ("energy_per_capita", pa.float64()),
    ("share_global_co2", pa.float64()),
    ("country_code", pa.string()),
])

dataset = ds.dataset(s3_uri, format="parquet", partitioning="hive", schema=schema_override)
df = dataset.to_table().to_pandas()
```

---

## 9. `AttributeError: 'FileSystemDataset' has no attribute 'to_pandas'`

**Contexto:** `bronze_expectations.py`

**Causa:** `pyarrow.dataset.Dataset` no tiene `to_pandas()` directo — hay que pasar primero por `to_table()`.

**Solución:**
```python
# ❌ Antes
df = dataset.to_pandas()

# ✅ Después
df = dataset.to_table().to_pandas()
```

---

## 10. `No expectations para co2` — Nombre de source incorrecto

**Contexto:** `run_validation.py --layer bronze --source co2`

**Causa:** El source debe coincidir exactamente con la key en `config_expectations.py`.

**Solución:** Usar el nombre completo:
```bash
# ❌
python run_validation.py --layer bronze --source co2

# ✅
python run_validation.py --layer bronze --source co2_emissions
```

---

## 11. `s3` no definido al guardar reporte en Bronze

**Contexto:** `bronze_expectations.py`

**Causa:** Al refactorizar la lectura para usar `pyarrow.dataset`, se eliminó `s3 = boto3.client("s3")` que antes estaba en el bloque de lectura. El `put_object` al final del archivo quedó sin la variable definida.

**Solución:** Definir `s3` justo antes de usarlo para subir el reporte:
```python
if not dry_run:
    s3 = boto3.client("s3")   # ← definir acá
    s3.put_object(
        Bucket="latam-sustainability-datalake",
        Key=f"quality_reports/bronze_{dataset_name}_{timestamp_file}.json",
        Body=json.dumps(report, indent=2)
    )
```