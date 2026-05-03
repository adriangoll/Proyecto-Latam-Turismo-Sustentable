# Infraestructura de Orquestación — EC2 + Airflow

## Descripción general

Este módulo de Terraform provisiona y gestiona la infraestructura de orquestación del pipeline de datos del proyecto **Latam Sustainable Tourism**. El diseño prioriza el **costo mínimo**: la instancia EC2 que corre Airflow permanece apagada la mayor parte del mes y se enciende automáticamente el día 1 de cada mes para ejecutar el pipeline completo. Una vez que el DAG finaliza, la propia última tarea apaga la máquina desde adentro.

---

## Arquitectura del ciclo de vida

```
EventBridge (cron 1° de cada mes)
        │
        ▼
  EC2 arranca (AWS-StartEC2Instance)
        │
        ▼
  Docker levanta Airflow automáticamente (systemd)
        │
        ▼
  DAG dag_datalake_core_monthly corre:
    1. task_bronze_to_silver
    2. task_validate_silver
    3. task_silver_to_gold
    4. task_stop_ec2_instance  ← apaga la instancia
        │
        ▼
  EC2 se detiene (boto3 desde el DAG)
```

---

## Componentes de infraestructura

### 1. Instancia EC2 (`aws_instance.airflow`)

La instancia corre Ubuntu con Docker. Al iniciarse por primera vez ejecuta un script de `user_data` que:

- Instala Docker, docker-compose y git
- Clona el repositorio del proyecto en `/opt/app`
- Levanta los contenedores de Airflow (webserver, scheduler, postgres)
- Registra un servicio `systemd` llamado `airflow.service` para que Docker se levante automáticamente en cada inicio de la máquina

Esto garantiza que, cada vez que EventBridge enciende la instancia, Airflow esté disponible sin intervención manual.

| Parámetro | Valor |
|---|---|
| Nombre | `latam-airflow-ec2` |
| Tipo | `t3.small` |
| AMI | `ami-0ff290337e78c83bf` (Ubuntu, us-east-1) |
| IP pública | Asignada dinámicamente |
| Región | `us-east-1` |

---

### 2. IAM Role de la EC2 (`aws_iam_role.ec2_role`)

El rol `LatamTourismEC2Role` otorga a la instancia los permisos mínimos necesarios para operar:

| Permiso | Recurso | Motivo |
|---|---|---|
| `s3:GetObject`, `s3:PutObject`, `s3:ListBucket` | `latam-sustainability-datalake` | Leer Bronze y escribir Silver/Gold en S3 |
| `ec2:StopInstances` | Instancias con tag `Name=latam-airflow-ec2` | Permite que el DAG apague la propia instancia al finalizar |
| `ec2:DescribeInstances` | `*` | Necesario para que boto3 pueda identificar la instancia |
| `logs:CreateLogGroup`, `logs:PutLogEvents`, etc. | `/latam-turismo/airflow` | Enviar logs de Docker a CloudWatch |

El permiso `ec2:StopInstances` está restringido por condición de tag (`ec2:ResourceTag/Name`) para que la instancia solo pueda apagarse a sí misma, no a otras instancias de la cuenta.

Además se adjuntan dos políticas administradas por AWS:
- `AmazonSSMManagedInstanceCore`: permite conectarse a la instancia vía Session Manager sin necesidad de SSH ni claves.
- `CloudWatchLogsFullAccess`: permite que el driver `awslogs` de Docker envíe los logs de los contenedores a CloudWatch.

---

### 3. Security Group (`aws_security_group.ec2_sg`)

| Dirección | Puerto | Protocolo | Origen | Motivo |
|---|---|---|---|---|
| Ingress | 8080 | TCP | `0.0.0.0/0` | Acceso a la UI de Airflow desde el browser |
| Egress | Todos | Todos | `0.0.0.0/0` | Salida a S3, CloudWatch, GitHub, PyPI |

No se abre el puerto 22 (SSH). El acceso a la instancia se realiza exclusivamente vía SSM Session Manager, lo que elimina la necesidad de gestionar claves SSH y reduce la superficie de ataque.

---

### 4. EventBridge — Arranque mensual (`aws_cloudwatch_event_rule.ec2_monthly_start`)

Una regla de EventBridge con expresión cron `cron(0 0 1 * ? *)` dispara el primer día de cada mes a las 00:00 UTC. El target es el documento de automatización de SSM `AWS-StartEC2Instance`, que inicia la instancia sin necesidad de código adicional.

El IAM Role asociado a EventBridge (`LatamEventBridgeEC2Role`) tiene permisos para:
- `ssm:StartAutomationExecution`: ejecutar el documento de SSM
- `ec2:StartInstances`: iniciar instancias EC2

---

### 5. Apagado automático — task_stop_ec2_instance (DAG de Airflow)

El apagado no se gestiona desde Terraform sino desde el propio DAG de Airflow. La última tarea del pipeline llama a `boto3` para detener la instancia. Incluye un delay de 35 segundos para garantizar que los logs finales se escriban en CloudWatch antes de que la máquina se detenga.

```python
def stop_ec2():
    time.sleep(35)
    boto3.client("ec2", region_name="us-east-1").stop_instances(
        InstanceIds=["i-0292fc72c1b2a4f1b"]
    )
```

Esta decisión de diseño —apagar desde el DAG en lugar de una segunda regla de EventBridge— tiene varias ventajas:

- El apagado ocurre exactamente cuando el pipeline termina, independientemente de cuánto tarde
- Si el pipeline falla, la máquina no se apaga, lo que permite al equipo ingresar a revisar los logs en Airflow
- No requiere estimar una duración fija del pipeline para programar el apagado

---

### 6. CloudWatch Logs (`aws_cloudwatch_log_group.airflow`)

Se crea un log group `/latam-turismo/airflow` con retención de 7 días y tres streams:

| Stream | Contenido |
|---|---|
| `init` | Logs del contenedor de inicialización de la base de datos |
| `webserver` | Logs del webserver de Airflow |
| `scheduler` | Logs del scheduler de Airflow |

Los contenedores de Docker están configurados con el driver `awslogs` para enviar sus logs directamente a estos streams, permitiendo monitoreo centralizado sin necesidad de SSH.

---

## Decisiones de diseño

**¿Por qué apagar la instancia desde el DAG y no con EventBridge?**
EventBridge requeriría estimar una duración fija del pipeline. Si el pipeline tarda más, la máquina se apagaría con tareas en curso. El enfoque actual es más robusto: la máquina se apaga cuando el trabajo realmente termina.

**¿Por qué SSM en lugar de SSH?**
SSM no requiere abrir el puerto 22 ni gestionar claves. El acceso queda registrado en CloudTrail y puede revocarse centralmente modificando el IAM Role, sin necesidad de rotar claves distribuidas entre el equipo.

**¿Por qué `ec2:StopInstances` con condición de tag?**
Aplicar el principio de mínimo privilegio: la instancia solo puede apagarse a sí misma. Si las credenciales de la instancia fueran comprometidas, el atacante no podría detener otras instancias de la cuenta.

---

## Outputs

| Output | Descripción |
|---|---|
| `ec2_instance_id` | ID de la instancia EC2 |
| `ec2_public_ip` | IP pública (cambia en cada Start si no hay Elastic IP) |
| `ec2_role_name` | Nombre del IAM Role de la instancia |
| `ec2_sg_id` | ID del Security Group |
| `eventbridge_rule_name` | Nombre de la regla de EventBridge |
