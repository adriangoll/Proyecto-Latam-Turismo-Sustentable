
# Diseño de Arquitectura de Data Lake  
## Pipeline de Datos para Turismo y Emisiones de CO₂ en América Latina
## Introducción
El proyecto consiste en el diseño e implementación de un pipeline de datos end-to-end para analizar la relación entre el turismo y las emisiones de CO₂ en América Latina, utilizando una arquitectura de Data Lake en AWS.
Está orientado a apoyar a una organización enfocada en sustenibilidad en la comprensión del impacto ambiental del turismo y el crecimiento económico, mediante la integración y análisis de múltiples fuentes de datos.
La solución propone una arquitectura escalable, flexible y eficiente en costos, que permite la generación de indicadores clave (KPIs) y la obtención de insights para la toma de decisiones, bajo un enfoque **zero-budget** basado en servicios cloud de pago por uso.

## Arquitectura

La solución se basa en un Data Lake implementado en Amazon S3 bajo un enfoque Medallion, el cual organiza los datos en diferentes niveles de madurez:

- **Bronze:** almacenamiento de datos crudos provenientes de distintas fuentes  
- **Silver:** datos limpios, normalizados y validados para su consumo analítico  
- **Gold:** datasets agregados y KPIs listos para análisis de negocio  

### Flujo del sistema

El pipeline sigue un flujo end-to-end completamente orquestado:

APIs → Amazon S3 (Bronze) → AWS Glue / Transformaciones (Silver) → Modelado (Gold) → Amazon Athena → Dashboard (Streamlit)

### Diagrama de Arquitectura

El siguiente diagrama representa la arquitectura general del pipeline, incluyendo los componentes de ingesta, procesamiento, almacenamiento, consulta y visualización.

Se observa la integración de herramientas como Apache Airflow para la orquestación, AWS Glue para el procesamiento de datos, Amazon Athena para consultas analíticas y Streamlit para la capa de visualización.

![Arquitectura del Data Lake](diagrama_arquitectura.png)

## Modelo de Datos

El siguiente diagrama representa el modelo conceptual de datos utilizado en la capa Gold del Data Lake.

El diseño sigue un enfoque tipo esquema analítico, donde se integran diferentes entidades clave relacionadas con turismo, emisiones y variables económicas.

Se identifican:

- Tablas de hechos como **TOURISM_FACT**, **EMISSIONS_ECONOMY_FACT** y **TOURISM_TRANSPORT_FACT**  
- Dimensiones como **COUNTRY**, **YEAR** y **TRANSPORT_TYPE**  

Este modelo permite analizar la relación entre la actividad turística, el crecimiento económico y el impacto ambiental, facilitando consultas eficientes y generación de KPIs.

![Modelo de Datos](diagrama_modelo_conceptual.png)

## Tecnologías Clave

- Amazon S3: almacenamiento escalable (Data Lake)  
- Amazon Athena: consultas SQL serverless  
- Apache Airflow: orquestación del pipeline  
- AWS Glue: procesamiento y catalogación de datos  
- Terraform: infraestructura como código  
- Streamlit: visualización de datos  




## Ventajas de AWS

El uso de AWS aporta beneficios clave frente a otras alternativas:

- Escalabilidad sin rediseñar la arquitectura  
- Modelo serverless que elimina la gestión de infraestructura  
- Optimización de costos mediante pago por uso  
- Alta disponibilidad y resiliencia  
- Integración con un ecosistema completo de servicios  

Adicionalmente, la arquitectura fue diseñada bajo un enfoque de **zero-budget**, permitiendo operar sin costos fijos significativos.

## Decisiones Tecnológicas (Trade-offs)

- Pandas sobre PySpark: simplicidad y rapidez para volúmenes moderados  
- Athena sobre Redshift: menor costo y enfoque serverless  
- S3 sobre bases de datos: mayor flexibilidad y escalabilidad  
- Airflow sobre scripts manuales: automatización y monitoreo  
- Terraform sobre configuración manual: reproducibilidad  
- Streamlit sobre BI tradicional: flexibilidad y menor dependencia  



## Seguridad

- Uso de IAM bajo el principio de mínimo privilegio  
- Control de accesos a recursos  
- Buenas prácticas en manejo de credenciales  

## Organización del Equipo
El proyecto es desarrollado por un equipo de cuatro integrantes bajo un enfoque colaborativo, donde todos pueden participar en cualquier etapa del pipeline.

### Áreas de trabajo

- **Ingesta de datos:** extracción desde APIs  
- **Transformación (Bronze → Silver):** limpieza y estructuración  
- **Modelado (Gold y KPIs):** generación de métricas  
- **Infraestructura y orquestación:** automatización del pipeline  

### Enfoque de trabajo

- Participación transversal del equipo  
- Colaboración continua  
- Transferencia de conocimiento  
- Adaptabilidad y mejora continua  

> Este enfoque colaborativo asegura la robustez del sistema, la transferencia de conocimiento y la escalabilidad en entornos de ingeniería de datos.

## Conclusión

La solución propuesta prioriza simplicidad, eficiencia en costos y escalabilidad. La arquitectura permite evolucionar hacia herramientas más avanzadas como PySpark, Amazon Redshift o dbt conforme crezcan los requerimientos del negocio y el volumen de datos.
