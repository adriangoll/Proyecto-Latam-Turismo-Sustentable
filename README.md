# 🌎 LATAM Sustainable Tourism — Data Platform

![Python](https://img.shields.io/badge/Python-3.11-blue)
![AWS](https://img.shields.io/badge/AWS-S3%20%7C%20Glue%20%7C%20Athena-orange)
![Airflow](https://img.shields.io/badge/Airflow-Orchestration-red)
![Terraform](https://img.shields.io/badge/Terraform-IaC-purple)

---

## 🚀 TL;DR

Plataforma de Data Engineering en AWS que integra datos de **CO₂, turismo y transporte en LATAM**, implementando arquitectura **Medallion (Bronze → Silver → Gold)**, con validaciones automáticas, Open Data y dashboard interactivo.

👉 End-to-end: **Ingest → Transform → Validate → Serve → Visualize**

---

## 📌 Overview

El proyecto implementa una plataforma de datos que transforma múltiples fuentes externas en un **data product analítico listo para toma de decisiones**.

Integra:

* Emisiones de CO₂
* Turismo internacional
* Transporte

Permite analizar la sostenibilidad del turismo en LATAM con un enfoque de negocio.

---

## 🏗️ Arquitectura

```text
APIs / Files (OWID, World Bank, UNWTO)
                ↓
        🥉 Bronze (Raw - S3)
                ↓
        🥈 Silver (Clean)
                ↓
        🥇 Gold (Business)
                ↓
   📊 Streamlit / Power BI / Open Data
```

---

## 📁 Project Structure

```bash
├── pipelines/        # lógica de datos
├── dags/             # orquestación (Airflow)
├── infra/            # infraestructura (Terraform)
├── docs/             # documentación técnica
├── queries/          # SQL (Athena)
├── streamlit/        # dashboard
├── docker/           # entorno de ejecución
```

---

## 🥉 Bronze — Data Ingestion

### 📥 Fuentes

* Our World in Data (CO₂)
* World Bank (Tourism)
* UNWTO (Transport)

### ⚙️ Características

* Datos sin transformar
* Validaciones básicas
* Persistencia en S3

📍 Output:

```
s3://latam-sustainability-datalake/raw/
```

---

## 🥈 Silver — Data Transformation

### 🔧 Procesamiento

* Limpieza y estandarización
* Eliminación de duplicados
* Manejo de nulos
* Métricas derivadas

### 📂 Datasets

* co2_emissions
* tourism_arrivals
* transport_mode

📍 Output:

```
s3://latam-sustainability-datalake/silver/
```

---

## 🥇 Gold — Business Layer

### 📊 Modelos

#### 🧾 fact_tourism_emissions

* Integración completa de datasets
* KPIs:

  * CO₂ per tourist
  * GDP metrics
  * Tourism growth
  * Sustainability label

#### 🌎 dim_country

* Dimensión geográfica LATAM

📍 Output:

```
s3://latam-sustainability-datalake/gold/
```

---

## 📊 Analytical Queries

Consultas SQL en Athena para análisis de negocio:

* Turismo vs CO₂
* Impacto del transporte
* Crecimiento económico vs emisiones
* Evolución de emisiones
* Países con turismo sostenible

📁 Ubicación:

```
queries/
```

---

## 📤 Open Data

Publicación automática de datasets:

* CSV
* Parquet
* metadata.json
* data_dictionary.md

📍 Ubicación:

```
s3://<bucket>/open-data/v1/gold/
```

---

## 📊 Dashboard — Streamlit

Aplicación interactiva para explorar:

* Emisiones de CO₂
* Turismo internacional
* KPIs de sostenibilidad

▶️ Ejecutar:

```bash
streamlit run app.py
```

👉 El proyecto evoluciona de pipeline a **data product**

---

## 🧪 Data Quality

Validaciones automáticas (tipo Great Expectations):

* Schema
* Nulls
* Rangos
* Integridad en Gold

📍 Reportes:

```
s3://latam-sustainability-datalake/quality_reports/
```

---

## ☁️ Infraestructura (Terraform)

Provisionada como código:

* S3 (Data Lake)
* EC2 (Airflow + Docker)
* Glue (Data Catalog)
* Athena (Query Engine)
* EventBridge (Scheduling)
* CloudWatch (Logs)
* AWS Budget (cost control)

---

## 🔐 Security & Governance

Buenas prácticas implementadas:

* IAM Roles (sin credenciales hardcodeadas)
* Principio de mínimo privilegio
* `.env` no versionado (`.env.example` incluido)
* Control de acceso a S3
* Logs en CloudWatch
* Control de costos con AWS Budget

⚠️ Nota:
Proyecto académico — no incluye KMS ni VPC privada (roadmap futuro)

---

## 🚀 CI/CD

GitHub Actions ejecuta:

* Lint (black)
* Tests (pytest)
* Coverage
* Validación de datos

---

## 🧰 Tech Stack

* Python
* pandas / PyArrow
* boto3
* AWS (S3, Glue, Athena, EC2)
* Streamlit
* Terraform
* Docker
* GitHub Actions

---

## 🚀 Run End-to-End

```bash
# 1. Ingesta
python run_ingestion.py

# 2. Transformación
python run_transformation.py

# 3. Validación
python pipelines/expectations/run_validation.py --layer silver --source all

# 4. Export Open Data
python export_open_data_gold.py

# 5. Dashboard
streamlit run app.py
```

---

## 🤝 Work Distribution

El desarrollo se organizó en:

* Ingesta (Bronze)
* Transformación (Silver)
* Modelado (Gold)
* Infraestructura (Terraform + AWS)
* Visualización (Streamlit)

Cada módulo fue desarrollado de forma independiente y validado en conjunto.

---

## ⚠️ Limitations

* Dataset UNWTO parcial
* Datos faltantes en algunos países
* Orquestación en evolución

---

## 🔮 Roadmap

* Airflow completo / Step Functions
* Dashboard en Power BI
* Data contracts
* API pública

---

## 👨‍💻 Team

| Nombre             | Rol                                |
| ------------------ | ---------------------------------- |
| Adrian Sosa        | Scrum Master                       |
| Martin Tedesco     | Data Engineer                      |
| Mariana Gil        | AWS Infrastructure                 |
| Luis Ramón Buruato | Data Engineer (Bronze & Pipelines) |

---

## ⭐ Final Thoughts

El proyecto evoluciona desde un pipeline hacia una **plataforma de datos completa**.

Incluye:

* ✔️ Ingesta confiable
* ✔️ Transformación con calidad
* ✔️ Modelado analítico
* ✔️ Validación automática
* ✔️ Open Data
* ✔️ Dashboard interactivo

👉 Resultado: una solución end-to-end para análisis de sostenibilidad en turismo LATAM.

---

