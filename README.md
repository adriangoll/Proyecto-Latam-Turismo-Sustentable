🌎 LATAM Sustainable Tourism — Data Platform

📌 Overview

Plataforma de Data Engineering end-to-end que integra datos de emisiones, turismo y transporte en LATAM para generar insights sobre sostenibilidad.

Construida bajo arquitectura Medallion (Bronze → Silver → Gold) y publicada como Open Data + Dashboard interactivo (Streamlit).

👉 Objetivo: transformar datos dispersos en un data product analítico listo para toma de decisiones.


🏗️ Arquitectura
APIs / Files (OWID, World Bank, UNWTO)
                ↓
        🥉 Bronze (Raw - S3)
                ↓
        🥈 Silver (Clean)
                ↓
        🥇 Gold (Business)
                ↓
   📊 Streamlit / Power BI / Open Data
   
🥉 Bronze — Data Ingestion
Ingesta desde:
Our World in Data (CO₂)
World Bank (Tourism)
UNWTO (Transport)
Sin transformaciones
Validaciones básicas

📍 Output:

s3://latam-sustainability-datalake/raw/
🥈 Silver — Data Transformation
Limpieza y estandarización
Eliminación de duplicados
Manejo de nulos
Métricas derivadas
📂 Datasets
co2_emissions
tourism_arrivals
transport_mode

📍 Output:

silver/*.parquet
🥇 Gold — Business Layer
📂 Modelos
🧾 fact_tourism_emissions
Integración completa de datasets
KPIs:
CO₂ per tourist
GDP metrics
Tourism growth
Sustainability label
🌎 dim_country
Dimensión geográfica LATAM

📍 Output:

gold/*.parquet
📤 Open Data

Datasets publicados automáticamente en formato abierto.

📦 Incluye:

CSV
Parquet
metadata.json
data_dictionary.md

📍 Ubicación:

s3://<bucket>/open-data/v1/gold/

Generado con:

python export_open_data_gold.py
📊 Dashboard — Streamlit
📌 Descripción

Aplicación interactiva para explorar:

Emisiones de CO₂ por país
Turismo internacional
Relación CO₂ vs turismo
KPI de sostenibilidad
▶️ Ejecutar local
streamlit run app.py
🌐 Funcionalidades
Filtros por país y año
Visualizaciones interactivas
KPIs dinámicos
Comparaciones entre países

👉 Aquí es donde el proyecto deja de ser pipeline y se vuelve producto.

🧪 Data Quality

Validaciones automáticas en todas las capas
(sistema tipo Great Expectations)

✔️ Schema
✔️ Nulls
✔️ Rangos
✔️ Integridad en Gold

📍 Reportes:

s3://latam-sustainability-datalake/quality_reports/
🚀 CI/CD

GitHub Actions:

Lint (black)
Tests (pytest)
Coverage
Validación de datos
🧰 Tech Stack
Python
pandas
boto3
PyArrow
AWS S3
Streamlit
GitHub Actions
🧪 Run End-to-End
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
📌 Design Decisions
Bronze inmutable (Single Source of Truth)
No se inventan datos
Silver optimizado (datasets pequeños)
Gold orientado a negocio
Open Data versionado (v1)
⚠️ Limitations
UNWTO dataset parcial
Algunos países/años faltantes
No hay orquestador (aún)
🔮 Roadmap
Airflow / Step Functions
Power BI dashboard
Data contracts
API pública
👨‍💻 Team
Adrian Sosa — Scrum Master
Martin Tedesco — Data Engineer
Mariana Gil — AWS Infra
Luis Ramón Buruato — Data Engineer (Bronze / Pipelines)

⭐ Final Thoughts

Este proyecto evoluciona de:

Pipeline → Data Lake → Data Product

Incluye:

✔️ Ingesta confiable
✔️ Transformación con calidad
✔️ Modelado analítico
✔️ Validación automática
✔️ Open Data
✔️ Dashboard interactivo

👉 Resultado: una plataforma lista para análisis real de sostenibilidad en turismo LATAM.
