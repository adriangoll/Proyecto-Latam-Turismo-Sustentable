from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import sys

# Obtenemos la ruta de la carpeta /dags
_HERE = os.path.dirname(os.path.abspath(__file__))
# Subimos un nivel para llegar a la raíz (donde vive /pipelines)
_ROOT = os.path.abspath(os.path.join(_HERE, '..'))

# Registramos la raíz en el sistema para poder importar
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from utils import notificar_error, wrapper_procesamiento
from pipelines.transformation import run_transformation
from pipelines.gold import run_gold
from pipelines.expectations.run_validation import run as run_validation



default_args = {
    'owner': 'data_engineering',
    'start_date': datetime(2024, 1, 1),
    'on_failure_callback': notificar_error,
}

with DAG(
    dag_id='dag_datalake_core_monthly',
    default_args=default_args,
    description='Pipeline de refinamiento de capas S3 (Mensual)',
    schedule_interval='0 0 1 * *', # Corre el día 1 de cada mes
    catchup=False,
    tags=['s3', 'engineering', 'monthly']
) as dag:

    # Tarea Capa Plata
    task_silver = PythonOperator(
        task_id='task_bronze_to_silver',
        python_callable=wrapper_procesamiento,
        op_kwargs={'script_func': run_transformation},
        ##provide_context=True
    )

    task_validate_silver = PythonOperator(
    task_id='task_validate_silver',
    python_callable=run_validation,
    op_kwargs={'layer': 'silver', 'source': 'all'},
)

    # Tarea Capa Oro
    task_gold = PythonOperator(
        task_id='task_silver_to_gold',
        python_callable=wrapper_procesamiento,
        op_kwargs={'script_func': run_gold},
        ##provide_context=True
    )

    # Dependencia: No empieza Oro si falla Plata
    task_silver >> task_gold