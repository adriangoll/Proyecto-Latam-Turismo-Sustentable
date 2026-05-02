# 📊 SQL — Queries de Negocio (Athena)

Este módulo contiene consultas SQL ejecutadas en **Amazon Athena** sobre la capa GOLD del Data Lake.

Las queries permiten analizar la relación entre turismo, emisiones de CO₂ y crecimiento económico en LATAM.

---

## 📁 Ubicación

```bash
queries/
```

---

## 🧠 Fuente de datos

Todas las consultas utilizan:

```sql
latam_sustainable_tourism.fact_tourism_emissions
```

---

## 📌 Queries disponibles

---

### 1. Turismo vs CO₂

Archivo:

```bash
01_turismo_vs_co2.sql
```

Objetivo:

* Analizar la relación entre llegadas turísticas y emisiones de CO₂

---

### 2. Impacto ambiental del transporte

Archivo:

```bash
02_transporte_impacto_ambiental.sql
```

Objetivo:

* Evaluar cómo el tipo de transporte influye en emisiones

---

### 3. Crecimiento económico vs emisiones

Archivo:

```bash
03_crecimiento_economico_vs_co2.sql
```

Objetivo:

* Analizar relación entre crecimiento del PIB y emisiones

---

### 4. Evolución de emisiones en turismo

Archivo:

```bash
04_evolucion_emisiones_turismo.sql
```

Objetivo:

* Analizar tendencia temporal de emisiones

---

### 5. Países con turismo sostenible

Archivo:

```bash
05_paises_turismo_sostenible.sql
```

Objetivo:

* Identificar países con menor impacto ambiental relativo

---

## 🚀 Uso

Las consultas pueden ejecutarse en:

* Amazon Athena
* Consola AWS
* Integración con BI tools

---

## 🧠 Valor del análisis

Estas queries permiten:

* Identificar patrones entre turismo y sostenibilidad
* Generar insights para toma de decisiones
* Validar la capa GOLD del Data Lake

---

