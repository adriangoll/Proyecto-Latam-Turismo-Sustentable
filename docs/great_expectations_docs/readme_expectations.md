# Great Expectations — Módulo de Validaciones

Validaciones automáticas de integridad y calidad de datos para las capas **Bronze**, **Silver** y **Gold** del Data Lake.

## 📋 Descripción

Este módulo implementa validaciones custom inspiradas en **Great Expectations** para validar datos en cada etapa del pipeline:

- **Bronze**: Validar schema, completitud, tipos de datos (datos particionados por `year/country_code`)
- **Silver**: Validar métricas derivadas, duplicados, rangos numéricos (archivo `data.parquet` plano)
- **Gold**: Validar integridad relacional (joins, PKs, FKs) (archivo `data.parquet` plano)

Cada validación genera un **reporte JSON** en `s3://latam-sustainability-datalake/quality_reports/` que documenta qué pasó, qué falló y por qué.

---

## 📁 Estructura

```
pipelines/expectations/
├── __init__.py                      # Módulo Python (vacío)
├── config_expectations.py           # Diccionarios de expectativas por capa/dataset
├── utils_expectations.py            # Helpers reutilizables (S3, validaciones, reportes)
├── bronze_expectations.py           # Validaciones para Bronze
├── silver_expectations.py           # Validaciones para Silver
├── gold_expectations.py             # Validaciones para Gold
└── run_validation.py                # Runner CLI: ejecuta validaciones

tests/expectations/
├── __init__.py
└── test_expectations.py             # 14 tests unitarios (sin S3, sin red)
```

### Archivos principales

| Archivo | Responsabilidad |
|---------|-----------------|
| `config_expectations.py` | Define qué se valida (umbrales, checks) |
| `utils_expectations.py` | Helpers: I/O S3, timestamps, validaciones genéricas |
| `bronze_expectations.py` | Lógica de validación Bronze (particionado hive) |
| `silver_expectations.py` | Lógica de validación Silver |
| `gold_expectations.py` | Lógica de validación Gold |
| `run_validation.py` | Orquestador CLI |

---

## 🗂️ Estructura real del Data Lake

> ⚠️ Importante: Bronze está **particionado por año y país**, Silver y Gold tienen un único `data.parquet`.

```
s3://latam-sustainability-datalake/
├── bronze/
│   └── co2_emissions/
│       ├── year=2013/
│       │   ├── country_code=ARG/data.parquet
│       │   └── country_code=BRA/data.parquet
│       └── year=2023/...
├── silver/
│   ├── co2_emissions/data.parquet
│   ├── tourism_arrivals/data.parquet
│   └── transport_mode/data.parquet
└── gold/
    ├── fact_tourism_emissions/data.parquet
    └── dim_country/data.parquet
```

---

## 🚀 Uso

### Validar contra S3

```bash
cd pipelines/expectations

# Bronze
python run_validation.py --layer bronze --source co2_emissions

# Silver
python run_validation.py --layer silver --source all

# Gold
python run_validation.py --layer gold
```

> ⚠️ `--source co2` no funciona — usar el nombre exacto: `co2_emissions`, `tourism_arrivals`, `transport_mode`

### Opciones

```
--layer {bronze, silver, gold}     Capa a validar (requerido)
--source <nombre_dataset>          Dataset específico (Bronze/Silver)
--dry-run                          Lee local en vez de S3 (requiere parquet local)
```

---

## 🔍 Expectativas por capa

### Bronze — `co2_emissions`

| Check | Detalle |
|-------|---------|
| `table_row_count` | min 100 filas |
| `column_count` | 8 columnas exactas |
| `column_values_to_not_be_null` | `country_code`, `year` |
| `column_values_in_set` | `country_code` ∈ 19 países LATAM |
| `column_values_type` | `year`=int, `co2`=float |

### Silver — `co2_emissions`

| Check | Detalle |
|-------|---------|
| `table_row_count` | 209 exactas (19 países × 11 años) |
| `column_values_to_not_be_null` | `co2` |
| `column_values_type` | `gdp_growth_pct`=float |

### Silver — `transport_mode`

| Check | Detalle |
|-------|---------|
| `table_row_count` | 190 exactas (19 países × 10 años) |
| `column_values_to_not_be_null` | `country_code`, `year`, `dominant_transport`, `tourists_total` |
| `column_values_to_be_between` | `pct_air`, `pct_land` ∈ [0, 100] |
| `column_values_in_set` | `dominant_transport` ∈ {"air", "land", "sea"} |

### Gold — `fact_tourism_emissions`

| Check | Detalle |
|-------|---------|
| `table_row_count` | min 100 filas (outer join de 3 silver) |
| `column_values_to_not_be_null` | `co2_per_tourist` |
| `column_values_to_be_between` | `co2_per_tourist` ∈ [0, 10000] |
| `column_values_in_set` | `sustainability_label` ∈ {"high", "medium", "low"} |

### Gold — `dim_country`

| Check | Detalle |
|-------|---------|
| `table_row_count` | 19 exactas (19 países LATAM) |
| `column_values_to_not_be_null` | `country_code`, `country_name`, `country_code_iso2` |
| `column_values_in_set` | `country_code` ∈ 19 códigos ISO3 LATAM |
| `column_values_in_set` | `region_latam` ∈ {"South America", "Central America", "Caribbean", "North America"} |

---

## 📊 Reportes

Los reportes se guardan automáticamente en S3 al correr cada validación:

```
s3://latam-sustainability-datalake/quality_reports/
├── bronze_co2_emissions_2026-04-26.json
├── silver_co2_emissions_2026-04-26.json
├── silver_tourism_arrivals_2026-04-26.json
├── silver_transport_mode_2026-04-26.json
├── gold_fact_tourism_emissions_2026-04-26.json
└── gold_dim_country_2026-04-26.json
```

**Estructura de reporte:**
```json
{
  "dataset": "co2_emissions",
  "layer": "silver",
  "timestamp": "2026-04-26T15:30:00Z",
  "total_checks": 5,
  "passed": 5,
  "failed": 0,
  "failures": [],
  "table_stats": {"rows": 209, "cols": 10},
  "summary": "5/5 checks OK"
}
```

---

## 🧪 Tests

**14 tests unitarios** — no requieren S3, red ni credenciales AWS.

```bash
# Desde repo root
python -m pytest tests/expectations/test_expectations.py -v

# Con coverage
python -m pytest tests/expectations/test_expectations.py -v --cov=pipelines/expectations --cov-report=term-missing
```

**Cobertura:**
- 2 tests de timestamps
- 8 tests de validaciones (row count, nulls, duplicates, ranges, sets)
- 4 tests de reportes (estructura, summary)

---

## 🔌 Integración CI/CD

```yaml
- name: Validate Silver with Great Expectations
  run: |
    cd pipelines
    python expectations/run_validation.py --layer silver --source all
  env:
    AWS_ACCESS_KEY_ID: ${{ secrets.AWS_ACCESS_KEY_ID }}
    AWS_SECRET_ACCESS_KEY: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
```

---

## 🐛 Troubleshooting

| Error | Causa | Solución |
|-------|-------|----------|
| `No expectations para co2` | Nombre incorrecto de source | Usar `co2_emissions` (nombre exacto del key en config) |
| `io.UnsupportedOperation: seek` | `boto3` StreamingBody no soporta seek | Envolver con `io.BytesIO(obj["Body"].read())` |
| `ArrowTypeError: year int64 vs int32` | Tipos inconsistentes entre particiones hive | Leer con `ds.dataset(..., schema=schema_override)` y castear `year` a int64 |
| `UnboundLocalError: df` | Bloque S3 tenía `pass` sin asignar df | Implementar lectura real en el bloque `else` |
| `NoSuchKey` | Ruta S3 incorrecta | Verificar con `aws s3 ls s3://bucket/path/` |
| `ModuleNotFoundError: config_expectations` | sys.path incorrecto | Agregar `pipelines/expectations/` al path |

---

## 📝 Agregar nuevas expectativas

Editar `config_expectations.py`:

```python
EXPECTATIONS = {
    "silver": {
        "mi_dataset": {
            "checks": [
                {"type": "table_row_count", "expected_value": 209},
                {"type": "column_values_to_not_be_null", "column": "mi_columna"},
                {"type": "column_values_to_be_between", "column": "valor",
                 "min_value": 0, "max_value": 100},
            ]
        }
    }
}
```

**Tipos de check disponibles:**

| Tipo | Parámetros |
|------|-----------|
| `table_row_count` | `expected_value` o `min_value` |
| `column_values_to_not_be_null` | `column`, `threshold_pct` |
| `no_duplicates` | `subset` (lista de columnas) |
| `column_values_in_set` | `column`, `value_set` |
| `column_values_to_be_between` | `column`, `min_value`, `max_value` |

---

**Última actualización:** 2026-04-26  
**Autor:** Grupo 1 - Data Engineering Henry  
**Estado:** ✅ Producción