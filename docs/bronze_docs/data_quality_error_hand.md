📊 Data Quality & Error Handling

## Data Quality Strategy

El pipeline de ingesta implementa validaciones orientadas a garantizar consistencia básica y trazabilidad de los datos antes de su almacenamiento en el Data Lake (capa Bronze).

⚠️ Importante:
La capa Bronze sigue el principio de “raw but structured”:
- Se estandarizan y validan los datos
- No se realizan transformaciones destructivas ni limpieza agresiva
- La resolución de problemas de calidad (duplicados, imputación, etc.) se delega a capas posteriores (Silver/Gold)

---

🔹 Detección de duplicados

Se identifican posibles registros duplicados en función de claves lógicas (ej: country_code + year).

Los duplicados:
- No son eliminados en Bronze
- Son registrados mediante logging para su análisis

La deduplicación se realiza en capas posteriores donde existen reglas de negocio más claras.

---

🔹 Tratamiento de valores nulos

Se monitorean valores nulos en campos relevantes mediante métricas y umbrales.

- En campos críticos: se aplican validaciones (ej: % de nulos)
- En campos no críticos: los valores nulos se preservan

No se eliminan registros automáticamente en Bronze, ya que esto podría implicar pérdida de información.

---

🔹 Validación de tipos de datos

Se aplican conversiones explícitas (type coercion) para asegurar consistencia:

- strings → numéricos
- control de errores con coerción a null

Registros con errores de tipo se conservan con valores nulos para análisis posterior.

---

🔹 Consistencia de datos y schema evolution

Se validan estructuras esperadas de las fuentes:

- presencia de columnas clave
- manejo dinámico de columnas faltantes
- adaptación a cambios de schema sin romper el pipeline

Esto permite tolerancia a cambios en las fuentes externas.

---

## Error Handling & Resilience

El pipeline está diseñado para ser tolerante a fallos parciales:

- Cada fuente de datos se procesa de forma independiente
- Fallos en una fuente no detienen la ejecución de las demás
- Se capturan excepciones y se registran errores por dataset
- El proceso global retorna error solo si alguna ingesta falla

---

## Logging & Observability

El sistema incorpora logging estructurado mediante Python (logging), permitiendo trazabilidad completa.

🔹 Características

- Logs por etapa del pipeline
- Registro de errores y excepciones con contexto
- Métricas básicas (volumen de datos, tiempos, validaciones)
- Detección de anomalías (ej: duplicados, alta proporción de nulos)

🔹 Integración con monitoreo

Compatible con:

- Amazon CloudWatch (centralización de logs)
- EventBridge (ejecución programada)
- Alertas ante fallos o validaciones críticas

---

## Execution Model

- Ejecución programada mediante EventBridge (cron mensual)
- Soporte para modo `--dry-run` (validación sin impacto)
- Orquestación local mediante script runner
- Escalable a orquestadores como Airflow

---

## Summary

El enfoque implementado prioriza:

- Trazabilidad sobre modificación de datos
- Validación sin pérdida de información
- Tolerancia a fallos
- Observabilidad del pipeline
- Escalabilidad hacia arquitecturas multicapa (Bronze → Silver → Gold)