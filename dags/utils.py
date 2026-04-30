import logging

from airflow.utils.email import send_email


def get_dag_logger(dag_id):
    """Configura el logger con el nombre del DAG"""
    return logging.getLogger(f"airflow.{dag_id}")


def notificar_error(context):
    """Callback que se dispara automáticamente si una tarea falla"""
    dag_id = context.get("dag_run").dag_id
    task_id = context.get("task_instance").task_id
    logical_date = context.get("ds")
    log_url = context.get("task_instance").log_url
    exception = context.get("exception")

    dag_logger = get_dag_logger(dag_id)

    sujeto = f"❌ Error en Pipeline: {task_id} ({logical_date})"
    msg_html = f"""
    ❌ <b>FALLÓ TAREA:</b> {task_id}<br>
    📅 <b>Periodo:</b> {logical_date}<br>
    ⚠️ <b>Error:</b> {exception}<br>
    🔗 <b>Logs:</b> <a href="{log_url}">Ver en Airflow</a>
    """
    dag_logger.error(msg_html)

    send_email(to="tedescomartindaniel@gmail.com", subject=sujeto, html_content=msg_html)
    print(f"Alerta enviada: {sujeto}")


def wrapper_procesamiento(script_func, **kwargs):
    """
    Función que enriquece el log y ejecuta tu script.
    """
    dag_id = kwargs.get("dag_run").dag_id if kwargs.get("dag_run") else "unknown"
    task_id = kwargs.get("task_instance").task_id if kwargs.get("task_instance") else "unknown"
    logical_date = kwargs.get("ds", "unknown")

    dag_logger = get_dag_logger(dag_id)

    dag_logger.info("=" * 60)
    dag_logger.info(f"▶️ INICIANDO: {task_id} (DAG: {dag_id})")
    dag_logger.info(f"📅 FECHA LÓGICA: {logical_date}")
    dag_logger.info("=" * 60)

    try:
        script_func()
        dag_logger.info(f"✅ TAREA {task_id} COMPLETADA")
        dag_logger.info("=" * 60)
    except Exception as e:
        dag_logger.error(f"💥 ERROR CRÍTICO EN {task_id}: {str(e)}")
        raise