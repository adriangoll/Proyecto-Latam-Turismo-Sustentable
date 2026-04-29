# Módulo utilitario para ejecutar queries en Athena
# y devolver los resultados como DataFrame de pandas.

import pandas as pd
from pyathena import connect

# Configuración de conexión a Athena
ATHENA_DATABASE = "latam_sustainable_tourism"
ATHENA_WORKGROUP = "latam-sustainable-tourism"
S3_OUTPUT = "s3://latam-sustainability-datalake/athena-results/"
AWS_REGION = "us-east-1"
AWS_PROFILE = "grupo1"


def query_athena(sql: str) -> pd.DataFrame:
    """
    Ejecuta una query SQL en Athena y devuelve un DataFrame.

    Args:
        sql: query SQL a ejecutar

    Returns:
        DataFrame con los resultados
    """
    conn = connect(
        s3_staging_dir=S3_OUTPUT,
        region_name=AWS_REGION,
        work_group=ATHENA_WORKGROUP,
        profile_name=AWS_PROFILE,
    )
    return pd.read_sql(sql, conn)