import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Obtenemos la ruta de la carpeta /dags
_HERE = os.path.dirname(os.path.abspath(__file__))
# Subimos un nivel para llegar a la raíz (donde vive /pipelines)
_ROOT = os.path.abspath(os.path.join(_HERE, ".."))

# Registramos la raíz en el sistema para poder importar
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from utils import notificar_error, wrapper_procesamiento

from pipelines.expectations.run_validation import run as run_validation
from pipelines.gold import run_gold

# ← AGREGAR ingesta
from pipelines.ingestion import run_ingestion
from pipelines.transformation import run_transformation

default_args = {
    "owner": "data_engineering",
    "start_date": datetime(2024, 1, 1),
    "on_failure_callback": notificar_error,
}

def stop_ec2():
    time.sleep(35)
    boto3.client("ec2", region_name="us-east-1").stop_instances(
        InstanceIds=["i-0292fc72c1b2a4f1b"]
    )

with DAG(
    dag_id="dag_datalake_core_monthly",
    default_args=default_args,
    description="Pipeline completo: Ingesta → Silver → Gold → Crawlers (Mensual)",
    schedule_interval="0 0 1 * *",  # Corre el día 1 de cada mes
    catchup=False,
    tags=["s3", "engineering", "monthly"],
) as dag:
    # ← NUEVA TAREA: Ingesta Bronze
    task_ingest_bronze = PythonOperator(
        task_id="task_ingest_bronze",
        python_callable=wrapper_procesamiento,
        op_kwargs={"script_func": run_ingestion},
    )

    # Tarea Capa Silver (Bronze → Silver)
    task_silver = PythonOperator(
        task_id="task_bronze_to_silver",
        python_callable=wrapper_procesamiento,
        op_kwargs={"script_func": run_transformation},
    )

    # Validar Silver
    task_validate_silver = PythonOperator(
        task_id="task_validate_silver",
        python_callable=run_validation,
        op_kwargs={"layer": "silver", "source": "all"},
    )

    # Tarea Capa Gold (Silver → Gold)
    task_gold = PythonOperator(
        task_id="task_silver_to_gold",
        python_callable=wrapper_procesamiento,
        op_kwargs={"script_func": run_gold},
    )

    # Validar Gold
    task_validate_gold = PythonOperator(
        task_id="task_validate_gold",
        python_callable=run_validation,
        op_kwargs={"layer": "gold"},
    )

    # ← NUEVA TAREA: Disparar crawlers Silver
    task_crawlers_silver = BashOperator(
        task_id="task_crawlers_silver",
        bash_command="""
            aws glue start-crawler --name latam-silver-co2-crawler
            aws glue start-crawler --name latam-silver-tourism-crawler
            aws glue start-crawler --name latam-silver-transport-crawler
            echo "✅ Crawlers Silver iniciados"
        """,
    )

    # ← NUEVA TAREA: Disparar crawlers Gold
    task_crawlers_gold = BashOperator(
        task_id="task_crawlers_gold",
        bash_command="""
            aws glue start-crawler --name latam-dev-gold-fact
            aws glue start-crawler --name latam-dev-gold-dim-country
            aws glue start-crawler --name latam-dev-open-data-gold
            echo "✅ Crawlers Gold iniciados"
        """,
    )

    task_stop_ec2 = PythonOperator(
        task_id="task_stop_ec2_instance",
        python_callable=stop_ec2,
    )

    # Dependencias: orden correcto
    task_ingest_bronze >> task_silver >> task_validate_silver >> task_crawlers_silver >> task_gold >> task_validate_gold >> task_crawlers_gold >> task_stop_ec2 
