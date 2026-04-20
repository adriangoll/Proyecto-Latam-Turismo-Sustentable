🥉 Bronze Layer — Pipeline de Ingesta de Datos


📌 Descripción general

La Capa Bronze es responsable de ingerir datos desde fuentes externas y almacenarlos en la Capa Raw (Data Lake) sin aplicar transformaciones.

Este enfoque sigue el patrón de arquitectura Medallion:

Bronze = lógica de ingesta (pipelines)
Raw = almacenamiento físico (S3)



🧱 Arquitectura
Fuentes externas
    ↓
Capa Bronze (Pipelines de ingesta)
    ↓
Capa Raw (Almacenamiento en AWS S3)
🗄️ Capa Raw (Data Lake)

La Capa Raw está implementada en Amazon S3, donde todos los datos ingeridos se almacenan en su formato original.



📂 Estructura del bucket
s3://latam-sustainability-datalake/raw/

├── owid_co2/
│   └── owid-co2-data.csv
│
├── worldbank_tourism/
│   ├── ST.INT.ARVL.csv
│   ├── ST.INT.RCPT.CD.csv
│   └── ST.INT.DPRT.csv
│
└── unwto_transport/
    └── unwto_transport.xlsx
    
    
📌 Características
Datos inmutables (sin modificaciones, solo append o reemplazo completo)
Organización por fuente
Formatos originales preservados (CSV, XLSX)
Fuente única de la verdad (single source of truth)



🌐 Fuentes de datos
OWID CO₂ & Emissions
Formato: CSV
Alcance: Datos globales de emisiones
World Bank Tourism
Indicadores:
ST.INT.ARVL → Llegadas de turistas
ST.INT.RCPT.CD → Ingresos por turismo
ST.INT.DPRT → Salidas de turistas
UNWTO Transport Mode
Formato: Excel
Descripción: Distribución del transporte turístico


⚙️ Ejecución del pipeline
python run_ingestion.py --dry-run
Comportamiento
Extrae datos desde APIs y archivos externos
Filtra países de LATAM (19)
Filtra años (2013–2023)
Ejecuta validaciones
Sube datos a S3 (omitido en modo dry-run)


🧪 Validación de datos
Validación de valores nulos
Validación de esquema (columnas)
Filtrado por países
Validación de rango de años
Verificación de tamaño del dataset
📊 Resumen de ejecución
[OK] OWID CO2 & Emissions     (16.6s)
[OK] World Bank Tourism       (20.2s)
[OK] UNWTO Transport Mode     (12.0s)

Tiempo total: 49.0s


🔄 Pipeline CI/CD

Automatizado con GitHub Actions

Jobs
Lint → black
Tests → pytest, pytest-cov
Artifacts → reporte de cobertura
🛠️ Stack tecnológico
Python 3.11
pandas
requests
boto3
pytest
AWS S3
GitHub Actions


✅ Buenas prácticas
Separación clara: Bronze (cómputo) vs Raw (almacenamiento)
Datos raw inmutables
Pipelines modulares por fuente
Logging estructurado
Modo dry-run para pruebas seguras
CI/CD para asegurar calidad


🚀 Flujo end-to-end
Ejecutar pipeline de ingesta
Extraer datos de las fuentes
Aplicar filtros (LATAM, años)
Validar datasets
Almacenar datos en S3


📈 Estado actual
Capa	Componente	Estado
Bronze	Pipelines de ingesta	✅ Completado
Raw	Estructura en S3	✅ Completado


- Adrian Sosa (Scrum Master)  
  - Liderazgo de ceremonias ágiles y planificación de sprints  
  - Desarrollo e implementación de lógica de código en pipelines  
  - Desarrollo de CI/CD con GitHub Actions  
  - Coordinación técnica del equipo  

- Martin Tedesco  
  - Data Engineer / Data Analyst  
  - Desarrollo de lógica de procesamiento en pipelines  
  - Apoyo en transformación de datos y análisis  

- Mariana Gil  
  - Data Engineer (Infraestructura en AWS)  
  - Implementación de infraestructura con Terraform  
  - Configuración de servicios en AWS (Glue, Athena)  
  - Soporte en modelado y consulta de datos  

- Luis Ramón Buruato  
  - Data Engineer (Capa Bronze)  
  - Diseño e implementación de pipelines de ingesta  
  - Soporte en desarrollo del proyecto  
  - Documentación técnica del proyecto  

