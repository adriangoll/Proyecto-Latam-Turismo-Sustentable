# Queries de Negocio — Proyecto Latam Turismo Sustentable

## 1. Objetivo

Este documento describe las queries SQL desarrolladas para responder las
preguntas de negocio del proyecto **Proyecto-Latam-Turismo-Sustentable**.

Las queries se ejecutan en Amazon Athena sobre las tablas de la capa Gold
registradas en el Glue Data Catalog.

## 2. Ubicación

Los archivos `.sql` se encuentran en la carpeta `queries/` del repositorio.

## 3. Cómo ejecutar una query

Desde la consola de Athena:
1. Seleccionar el workgroup `latam-sustainable-tourism`
2. Seleccionar la base de datos `latam_sustainable_tourism`
3. Pegar el contenido del archivo `.sql` en el Query Editor
4. Hacer clic en **Ejecutar**

Desde la CLI:
```bash
aws athena start-query-execution \
  --query-string "$(cat queries/nombre_archivo.sql)" \
  --work-group latam-sustainable-tourism \
  --profile grupo1 \
  --region us-east-1
```

## 4. Preguntas de negocio

### Pregunta 1 — ¿Existe relación entre el crecimiento del turismo y el aumento de emisiones de CO₂ en LATAM?

**Archivo:** `queries/q1_turismo_vs_co2.sql`

Agrupa por año la suma de llegadas de turistas y emisiones de CO₂ para
visualizar la evolución conjunta de ambas variables en la región desde el año 2000.

[CAPTURA: Resultado de la query en Athena]

---

### Pregunta 2 — ¿Qué medios de transporte turístico tienen mayor impacto ambiental?

**Archivo:** `queries/q2_transporte_impacto_ambiental.sql`

Compara el promedio de CO₂ por turista agrupado por medio de transporte dominante,
junto con el porcentaje de uso de cada medio, para identificar cuál genera
mayor huella de carbono.

[CAPTURA: Resultado de la query en Athena]

---

### Pregunta 3 — ¿Qué países logran crecimiento económico con menor impacto ambiental?

**Archivo:** `queries/q3_crecimiento_economico_vs_co2.sql`

Identifica países con crecimiento de GDP positivo y baja intensidad de CO₂,
buscando casos de desacople entre desarrollo económico y emisiones.

[CAPTURA: Resultado de la query en Athena]

---

### Pregunta 4 — ¿Cómo evolucionan las emisiones en función del turismo a lo largo del tiempo?

**Archivo:** `queries/q4_evolucion_emisiones_turismo.sql`

Analiza año a año por país la relación entre crecimiento de llegadas de turistas
y crecimiento de emisiones de CO₂ para detectar tendencias temporales.

[CAPTURA: Resultado de la query en Athena]

---

### Pregunta 5 — ¿Qué países muestran tendencias hacia un turismo más sostenible?

**Archivo:** `queries/q5_paises_turismo_sostenible.sql`

Utiliza el campo `sustainability_label` calculado en la capa Gold para identificar
qué países tienen mejor desempeño sostenible y cómo evolucionó su etiqueta
a lo largo del tiempo.

[CAPTURA: Resultado de la query en Athena]

---

## 5. Tablas utilizadas

| Tabla | Capa | Descripción |
|---|---|---|
| `fact_tourism_emissions` | Gold | Tabla de hechos principal con emisiones, turismo y transporte |
| `dim_country` | Gold | Dimensión de países de la región LATAM |

## 6. Validación

Todas las queries fueron validadas en Athena con estado `SUCCEEDED`.
Los resultados se guardan automáticamente en:
```text
s3://latam-sustainability-datalake/athena-results/
```