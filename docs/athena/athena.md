# Athena — Proyecto Latam Turismo Sustentable

## 1. Objetivo

Este documento describe la configuración de Amazon Athena para el proyecto
**Proyecto-Latam-Turismo-Sustentable**.

Athena permite consultar las tablas de la capa Gold directamente desde S3,
sin necesidad de mover los datos, utilizando SQL estándar e integrándose con
el Glue Data Catalog donde están registradas las tablas.

## 2. Workgroup

El workgroup fue provisionado con Terraform en `infra/modules/athena/`.

| Parámetro | Valor |
|---|---|
| Nombre | `latam-sustainable-tourism` |
| Output location | `s3://latam-sustainability-datalake/athena-results/` |
| Engine version | Athena engine version 3 |
| Estado | ENABLED |

Los resultados de cada query se guardan automáticamente en:
```text
s3://latam-sustainability-datalake/athena-results/
```

## 3. Tablas consultables

Las tablas de Gold registradas en el Glue Data Catalog y disponibles en Athena son:

| Tabla | Capa | Registros | Formato |
|---|---|---|---|
| `dim_country` | Gold | 19 | Parquet |
| `fact_tourism_emissions` | Gold | 209 | Parquet |

### Esquema de `dim_country`

| Columna | Tipo |
|---|---|
| `country_code` | string |
| `country_code_iso2` | string |
| `country_name` | string |
| `region_latam` | string |

### Esquema de `fact_tourism_emissions`

| Columna | Tipo |
|---|---|
| `country` | string |
| `country_code` | string |
| `year` | bigint |
| `co2` | double |
| `co2_per_capita` | double |
| `co2_intensity_gdp` | double |
| `gdp` | double |
| `gdp_per_capita` | double |
| `gdp_growth_pct` | double |
| `population` | double |
| `share_global_co2` | double |
| `tourist_arrivals` | double |
| `tourism_receipts_usd` | double |
| `tourist_departures` | double |
| `arrivals_growth_pct` | double |
| `receipts_per_tourist` | double |
| `tourists_air` | double |
| `tourists_sea` | double |
| `tourists_land` | double |
| `pct_air` | double |
| `pct_sea` | double |
| `pct_land` | double |
| `dominant_transport` | string |
| `co2_per_tourist` | double |
| `co2_growth_pct` | double |
| `sustainability_label` | string |

## 4. Validación realizada

Se validó que ambas tablas son consultables desde Athena ejecutando queries de prueba.

### Validación de `dim_country`
```sql
SELECT * FROM latam_sustainable_tourism.dim_country LIMIT 5
```
- Estado: `SUCCEEDED`
- Tiempo de ejecución: 618 ms

### Validación de `fact_tourism_emissions`
```sql
SELECT * FROM latam_sustainable_tourism.fact_tourism_emissions LIMIT 5
```
- Estado: `SUCCEEDED`
- Tiempo de ejecución: 676 ms

[CAPTURA: Query Editor de Athena mostrando las dos tablas en el panel izquierdo]

[CAPTURA: Resultado de query de validación sobre dim_country]

[CAPTURA: Resultado de query de validación sobre fact_tourism_emissions]

## 5. Notas

- Las queries siempre deben incluir el prefijo `latam_sustainable_tourism.`
  antes del nombre de la tabla.
- El workgroup `latam-sustainable-tourism` debe estar seleccionado en el
  Query Editor antes de ejecutar cualquier consulta.