# 🐳 Docker — Guía de inicio rápido

## Estructura de archivos

```
proyecto/
├── Dockerfile              # Imagen para pipelines + validaciones
├── docker-compose.yml      # Orquestación de servicios
├── .env.example            # Template de variables de entorno
├── requirements.txt        # Dependencias Python
└── airflow/
    └── dags/               # DAGs de Airflow (crear esta carpeta)
```

## Primera vez en EC2

```bash
# 1. Clonar repo
git clone <repo_url>
cd Final-Henry-Turismo-Sustentable

# 2. Crear carpeta de DAGs
mkdir -p airflow/dags

# 3. Configurar variables de entorno
cp .env.example .env

# Generar FERNET_KEY
python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
# → Copiar resultado en .env como AIRFLOW_FERNET_KEY

# Generar SECRET_KEY
openssl rand -hex 32
# → Copiar resultado en .env como AIRFLOW_SECRET_KEY

# 4. Inicializar Airflow (solo la primera vez)
docker compose run --rm airflow-init

# 5. Levantar servicios
docker compose up -d airflow-webserver airflow-scheduler postgres

# 6. Verificar que están corriendo
docker compose ps
```

## Acceder a Airflow

```
http://<EC2_PUBLIC_IP>:8080
usuario: admin
password: (el que pusiste en .env)
```

> ⚠️ Asegurate de que el puerto 8080 esté abierto en el Security Group de EC2.

## Correr pipelines manualmente

```bash
# Bronze
docker compose run --rm pipeline \
  python pipelines/gold/run_gold.py

# Validar Silver
docker compose run --rm pipeline \
  python pipelines/expectations/run_validation.py --layer silver --source all

# Validar Gold
docker compose run --rm pipeline \
  python pipelines/expectations/run_validation.py --layer gold
```

## Comandos útiles

```bash
# Ver logs de Airflow
docker compose logs -f airflow-scheduler

# Reiniciar un servicio
docker compose restart airflow-webserver

# Bajar todo
docker compose down

# Bajar todo y borrar volúmenes (⚠️ borra la DB de Airflow)
docker compose down -v

# Rebuildar imágenes después de cambios en código
docker compose build
docker compose up -d
```

## IAM Role en EC2

Con IAM Role configurado en la instancia, **no necesitás pasar credenciales AWS**.
`boto3` las toma automáticamente del instance metadata (`http://169.254.169.254/...`).

Permisos mínimos necesarios en el IAM Role:
- `s3:GetObject` sobre `latam-sustainability-datalake/*`
- `s3:PutObject` sobre `latam-sustainability-datalake/quality_reports/*`
- `s3:ListBucket` sobre `latam-sustainability-datalake`