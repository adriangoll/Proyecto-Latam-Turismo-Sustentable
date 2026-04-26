# 📊 Open Datasets — LATAM Sustainability Data Lake

Datasets de acceso público generados por el pipeline de datos del proyecto.
Todos los archivos están alojados en AWS S3 y son descargables directamente sin autenticación.

**Cobertura:** 19 países de América Latina · 2013–2023  
**Actualización:** mensual (día 1 de cada mes, automatizado)  
**Licencia:** [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) — libre uso con atribución  
**Mantenido por:** Grupo 1 · Data Engineering · Henry · 2026  
**Repositorio:** [github.com/adriangoll/Proyecto-Latam-Turismo-Sustentable](https://github.com/adriangoll/Proyecto-Latam-Turismo-Sustentable)

---

## ¿Qué hay disponible?

| Capa | Para quién | Qué contiene |
|------|-----------|--------------|
| **Silver** | Data Scientists, analistas, investigadores | Datos limpios y normalizados con métricas derivadas. Máxima granularidad (1 fila por país × año). Sin agregaciones. |
| **Gold** | Periodistas, equipos de BI, público general | KPIs y métricas de negocio pre-calculadas. Responden directamente las preguntas del proyecto. *(próximamente)* |

---

## Silver — Datasets para análisis técnico

> Ideal para: regresiones, correlaciones, visualizaciones propias, análisis exploratorio.  
> Formato Parquet recomendado para Data Scientists. CSV disponible para usuarios de Excel / R.

### 🌿 CO₂ Emissions & Economic Indicators

Emisiones de CO₂, PIB y población con métricas derivadas de intensidad ambiental.

| | |
|--|--|
| **Fuente** | Our World in Data — [owid/co2-data](https://github.com/owid/co2-data) |
| **Filas** | ~209 (19 países × 11 años) |
| **Columnas clave** | `co2`, `gdp`, `population`, `co2_per_capita_calc`, `co2_intensity_gdp`, `gdp_growth_pct` |

| Formato | Descarga |
|---------|---------|
| Parquet (recomendado) | [latam_co2_emissions_v1.parquet](https://latam-sustainability-datalake.s3.amazonaws.com/open-data/v1/silver/co2_emissions/latam_co2_emissions_v1.parquet) |
| CSV | [latam_co2_emissions_v1.csv](https://latam-sustainability-datalake.s3.amazonaws.com/open-data/v1/silver/co2_emissions/latam_co2_emissions_v1.csv) |
| Metadata | [metadata.json](https://latam-sustainability-datalake.s3.amazonaws.com/open-data/v1/silver/co2_emissions/metadata.json) |
| Diccionario | [data_dictionary.md](https://latam-sustainability-datalake.s3.amazonaws.com/open-data/v1/silver/co2_emissions/data_dictionary.md) |

```python
# Ejemplo de uso en Python
import pandas as pd
df = pd.read_parquet(
    "https://latam-sustainability-datalake.s3.amazonaws.com"
    "/open-data/v1/silver/co2_emissions/latam_co2_emissions_v1.parquet"
)
```

---

### ✈️ International Tourism Arrivals

Llegadas de turistas internacionales, ingresos turísticos y salidas por país.

| | |
|--|--|
| **Fuente** | World Bank Open Data — [ST.INT.ARVL, ST.INT.RCPT.CD, ST.INT.DPRT](https://api.worldbank.org/v2) |
| **Filas** | ~209 (cobertura parcial según disponibilidad del Banco Mundial) |
| **Columnas clave** | `tourist_arrivals`, `tourism_receipts_usd`, `arrivals_growth_pct`, `receipts_per_tourist` |

| Formato | Descarga |
|---------|---------|
| Parquet (recomendado) | [latam_tourism_arrivals_v1.parquet](https://latam-sustainability-datalake.s3.amazonaws.com/open-data/v1/silver/tourism_arrivals/latam_tourism_arrivals_v1.parquet) |
| CSV | [latam_tourism_arrivals_v1.csv](https://latam-sustainability-datalake.s3.amazonaws.com/open-data/v1/silver/tourism_arrivals/latam_tourism_arrivals_v1.csv) |
| Metadata | [metadata.json](https://latam-sustainability-datalake.s3.amazonaws.com/open-data/v1/silver/tourism_arrivals/metadata.json) |
| Diccionario | [data_dictionary.md](https://latam-sustainability-datalake.s3.amazonaws.com/open-data/v1/silver/tourism_arrivals/data_dictionary.md) |

```python
df = pd.read_parquet(
    "https://latam-sustainability-datalake.s3.amazonaws.com"
    "/open-data/v1/silver/tourism_arrivals/latam_tourism_arrivals_v1.parquet"
)
```

---

### 🚢 Tourist Arrivals by Transport Mode

Llegadas desagregadas por modo de transporte: aéreo, marítimo y terrestre.

| | |
|--|--|
| **Fuente** | UN Tourism / UNWTO |
| **Nota** | Cobertura parcial — no todos los países reportan todos los modos. Los valores nulos indican dato no disponible, no cero. |
| **Columnas clave** | `tourists_air`, `tourists_sea`, `tourists_land`, `pct_air`, `pct_sea`, `pct_land`, `dominant_transport` |

| Formato | Descarga |
|---------|---------|
| Parquet (recomendado) | [latam_transport_mode_v1.parquet](https://latam-sustainability-datalake.s3.amazonaws.com/open-data/v1/silver/transport_mode/latam_transport_mode_v1.parquet) |
| CSV | [latam_transport_mode_v1.csv](https://latam-sustainability-datalake.s3.amazonaws.com/open-data/v1/silver/transport_mode/latam_transport_mode_v1.csv) |
| Metadata | [metadata.json](https://latam-sustainability-datalake.s3.amazonaws.com/open-data/v1/silver/transport_mode/metadata.json) |
| Diccionario | [data_dictionary.md](https://latam-sustainability-datalake.s3.amazonaws.com/open-data/v1/silver/transport_mode/data_dictionary.md) |

```python
df = pd.read_parquet(
    "https://latam-sustainability-datalake.s3.amazonaws.com"
    "/open-data/v1/silver/transport_mode/latam_transport_mode_v1.parquet"
)
```

---

## Gold — KPIs de negocio *(próximamente)*

Los datasets Gold estarán disponibles al finalizar el Sprint 2.
Responderán directamente las preguntas de negocio del proyecto:

- ¿Existe relación entre el crecimiento del turismo y el aumento de emisiones de CO₂ en LATAM?
- ¿Qué medios de transporte turístico tienen mayor impacto ambiental?
- ¿Qué países logran crecimiento económico con menor impacto ambiental?
- ¿Cómo evolucionan las emisiones en función del turismo a lo largo del tiempo?
- ¿Qué países muestran tendencias hacia un turismo más sostenible?

---

## Países cubiertos

Argentina · Bolivia · Brasil · Chile · Colombia · Costa Rica · Cuba ·
República Dominicana · Ecuador · El Salvador · Guatemala · Honduras ·
México · Nicaragua · Panamá · Paraguay · Perú · Uruguay · Venezuela

---

## Atribución

Si usás estos datasets en tu trabajo, por favor citá:

```
Grupo 1 — LATAM Sustainability Data Lake
Henry Data Engineering 2026
https://github.com/adriangoll/Proyecto-Latam-Turismo-Sustentable
Licencia: CC BY 4.0
```

Fuentes originales:
- Our World in Data: Hannah Ritchie, Max Roser et al. (2023) — *CO₂ and Greenhouse Gas Emissions*
- World Bank Open Data: World Bank Group — *World Development Indicators*
- UN Tourism: UNWTO — *Inbound Tourism Statistics*