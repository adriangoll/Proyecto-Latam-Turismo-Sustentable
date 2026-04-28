import os
import sys
from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.python import PythonOperator
from datetime import datetime

# 1. Configuramos rutas
_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.abspath(os.path.join(_HERE, '..'))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from utils import notificar_error, wrapper_procesamiento
from pipelines.transformation import export_open_data_silver
from pipelines.gold import export_open_data_gold

default_args = {
    'owner': 'data_science',
    'start_date': datetime(2024, 1, 1),
    'on_failure_callback': notificar_error,
}

with DAG(
    dag_id='dag_ds_export_monthly',
    default_args=default_args,
    description='Pipeline de exportacion de datos para Data Science (Mensual)',
    schedule_interval='0 0 1 * *', # Corre el día 1 de cada mes
    catchup=False,
    tags=['ds', 'export', 'monthly']
) as dag:

    # SENSOR PLATA: Mira el DAG de ejecucion del pipeline y espera SOLO a la tarea de Plata
    wait_silver = ExternalTaskSensor(
        task_id='wait_for_silver',
        external_dag_id='dag_datalake_core_monthly',
        external_task_id='task_bronze_to_silver',
        mode='reschedule', # Libera recursos mientras espera
        poke_interval=300  # Pregunta cada 5 minutos
    )

    # SENSOR ORO: Mira el DAG de ejecucion del pipeline y espera SOLO a la tarea de Oro
    wait_gold = ExternalTaskSensor(
        task_id='wait_for_gold',
        external_dag_id='dag_datalake_core_monthly',
        external_task_id='task_silver_to_gold',
        mode='reschedule',
        poke_interval=300
    )

    # Tareas de exportación (usando el wrapper)
    task_export_silver = PythonOperator(
        task_id='export_silver_to_ds',
        python_callable=wrapper_procesamiento,
        op_kwargs={'script_func': export_open_data_silver},
        provide_context=True
    )

    task_export_gold = PythonOperator(
        task_id='export_gold_to_ds',
        python_callable=wrapper_procesamiento,
        op_kwargs={'script_func': export_open_data_gold},
        provide_context=True
    )

    # Flujos independientes: Plata no espera a Oro
    wait_silver >> task_export_silver
    wait_gold >> task_export_gold