🐳 Docker — Guía de inicio rápido
📁 Estructura de archivos
proyecto/├── docker/│   ├── Dockerfile│   ├── docker-compose.yml│   └── requirements.txt├── dags/├── pipelines/├── .env.example├── .dockerignore└── README.md

⚠️ Requisitos


Docker + Docker Compose


Instancia EC2 (Ubuntu recomendado)


Puerto 8080 abierto en Security Group



🚀 Primera vez en EC2
1. Clonar repositorio
git clone <repo_url>cd Final-Henry-Turismo-Sustentable

2. Configurar variables de entorno
cp .env.example .env
🔐 Generar claves necesarias
# FERNET KEYpython3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"# SECRET KEYopenssl rand -hex 32
👉 Pegarlas en .env:
AIRFLOW_FERNET_KEY=AIRFLOW_SECRET_KEY=AIRFLOW_ADMIN_USER=adminAIRFLOW_ADMIN_PASSWORD=<definir_password_seguro>

3. Inicializar Airflow (solo primera vez)
docker compose run --rm airflow-init

4. Levantar servicios
docker compose up -d airflow-webserver airflow-scheduler postgres

5. Verificar estado
docker compose ps

🌐 Acceso a Airflow
http://<EC2_PUBLIC_IP>:8080
Credenciales:
usuario: adminpassword: (definido en .env)

⚙️ Ejecutar pipelines manualmente
🟡 Ejecutar capa GOLD
docker compose run --rm pipeline \  python pipelines/gold/run_gold.py

🧪 Validar capa SILVER
docker compose run --rm pipeline \  python pipelines/expectations/run_validation.py --layer silver --source all

🧪 Validar capa GOLD
docker compose run --rm pipeline \  python pipelines/expectations/run_validation.py --layer gold

🛠️ Comandos útiles
Ver logs
docker compose logs -f airflow-scheduler
docker compose logs -f

Reiniciar servicios
docker compose restart airflow-webserver

Acceder a contenedor
docker exec -it airflow_webserver bash

Detener servicios
docker compose down

Borrar todo (incluye DB ⚠️)
docker compose down -v

Rebuild después de cambios
docker compose builddocker compose up -d

☁️ AWS — IAM Role
Si ejecutas en EC2 con IAM Role:
👉 NO necesitas credenciales en .env
boto3 usa automáticamente:
http://169.254.169.254/

🔑 Permisos mínimos requeridos
s3:GetObject       → latam-sustainability-datalake/*s3:PutObject       → latam-sustainability-datalake/quality_reports/*s3:ListBucket      → latam-sustainability-datalake

🔐 Seguridad
❗ Importante


❌ NO subir .env al repositorio


❌ NO hardcodear credenciales


✅ Usar .env.example



🧱 .dockerignore recomendado
.git__pycache__*.pyc.env*.egg-info.pytest_cache

🧠 Notas de arquitectura


Executor: LocalExecutor


Base de datos: PostgreSQL


Orquestación: Airflow


Procesamiento: Python (Pandas + PyArrow)


Storage: Amazon S3



🚨 Troubleshooting
DAG no aparece
docker compose logs airflow-scheduler

Error de permisos S3


Revisar IAM Role


Verificar bucket y rutas



Webserver no carga
docker compose logs airflow-webserver

🧩 Conclusión
Este entorno permite:


Orquestar pipelines con Airflow


Ejecutar validaciones de datos


Procesar datos en S3


Escalar fácilmente en AWS


