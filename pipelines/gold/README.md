# 🥇 GOLD Layer – Data Products

La capa **GOLD** representa la capa final del Data Lake, donde los datos son transformados en **productos analíticos listos para consumo**.

Estos datasets están diseñados para responder preguntas de negocio, alimentar modelos de Data Science y soportar dashboards.

---

## 🎯 Objetivo

Consolidar múltiples fuentes de datos (emisiones, turismo y transporte) en datasets estructurados, consistentes y optimizados para análisis.

---

## 📦 Datasets

### 🌱 fact_tourism_emissions

Tabla de hechos que integra métricas de:

* Emisiones de CO₂
* Actividad turística
* Medios de transporte

**Grano:**
Una fila por `(country_code, year)`

---

### 🌎 dim_country

Dimensión con información de países LATAM.

**Grano:**
Una fila por país

---

## 🔗 Modelo de datos

* `fact_tourism_emissions.country_code`
  → FK a `dim_country.country_code`

---

## 🔄 Origen de datos

Los datos provienen de la capa **SILVER**, almacenados en S3:

* CO₂ emissions
* Tourism arrivals
* Transport mode

---

## ⚙️ Procesamiento

### 🔹 Orquestación

* `run_gold.py`

### 🔹 Transformaciones

* `build_gold.py`

### 🔹 Configuración

* `config_gold.py`

---

## ☁️ Arquitectura

* Storage: Amazon S3
* Formato: Parquet (Snappy)
* Procesamiento: Python (Pandas + PyArrow)
* Orquestación: Airflow

---

## ⏱️ Frecuencia

* Ejecución mensual
* DAG: `dag_datalake_core_monthly`

---

## 📁 Outputs

* `s3://latam-sustainability-datalake/gold/fact_tourism_emissions/data.parquet`
* `s3://latam-sustainability-datalake/gold/dim_country/data.parquet`

---

## 🧠 Lógica de negocio

### 🔗 Integración

* JOIN (outer) de datasets SILVER por `(country_code, year)`
* Se preservan registros aunque haya datos faltantes

---

### 📊 Métricas derivadas

* `co2_per_tourist`
* `co2_growth_pct`

---

### 🏷️ Clasificación de sostenibilidad

* `verde` → crecimiento económico sin aumento de emisiones
* `amarillo` → crecimiento con aumento de emisiones
* `rojo` → caída económica con aumento de emisiones
* `gris` → datos insuficientes

---

## 📊 Casos de uso

* Análisis de sostenibilidad
* Evaluación de impacto ambiental del turismo
* Modelos de Data Science
* Dashboards de BI
* Publicación de Open Data

---

## ⚠️ Consideraciones

* Se utiliza **outer join** para no perder información
* Valores nulos reflejan limitaciones de las fuentes
* Los datos están optimizados para análisis, no para ingestión

---

## 📚 Documentación adicional

Cada dataset incluye:

* `data_dictionary.md`
* `metadata.json`

---

## 🚀 Ejecución

```bash id="run-gold"
python run_gold.py
```

```bash id="dry-run-gold"
python run_gold.py --dry-run
```

---

## 👤 Owner

Data Engineering

