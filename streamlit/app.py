# Página principal del dashboard
# Resume las 5 preguntas de negocio y muestra métricas generales.

import base64

import plotly.express as px
from utils.athena_client import query_athena

import streamlit as st


def get_base64_image(path: str) -> str:
    with open(path, "rb") as f:
        return base64.b64encode(f.read()).decode()


fondo = get_base64_image("assets/fondo.png")

st.set_page_config(
    page_title="Latam Turismo Sustentable",
    page_icon="🌎",
    layout="wide",
)

st.markdown(
    f"""
    <style>
    .stApp {{
        background-image: url("data:image/png;base64,{fondo}");
        background-size: 100% 100%;
        background-position: center;
        background-repeat: no-repeat;
        background-attachment: fixed;
    }}

    [data-testid="stSidebar"] {{
        background: transparent;
    }}

    /* Baja el contenido real del sidebar */
    [data-testid="stSidebar"] > div:first-child {{
        padding-top: 120px !important;
    }}

    /* Negrita para textos/widgets del sidebar */
    [data-testid="stSidebar"] * {{
        font-weight: 700 !important;
    }}
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("🌎 Latam Turismo Sustentable")
st.markdown(
    """
    Dashboard de análisis de la relación entre turismo y sostenibilidad
    ambiental en América Latina.

    Utilizá el menú de la izquierda para navegar entre las preguntas de negocio.
    """
)


# --- Carga de métricas generales ---
@st.cache_data
def cargar_metricas():
    sql = """
        SELECT
            COUNT(DISTINCT country)  AS total_paises,
            COUNT(DISTINCT year)     AS total_anios,
            SUM(tourist_arrivals)    AS total_llegadas,
            SUM(co2)                 AS total_co2
        FROM latam_sustainable_tourism.fact_tourism_emissions
        WHERE year >= 2000
    """
    return query_athena(sql)


@st.cache_data
def cargar_sostenibilidad():
    sql = """
        SELECT
            sustainability_label,
            COUNT(DISTINCT country) AS cantidad_paises
        FROM latam_sustainable_tourism.fact_tourism_emissions
        WHERE sustainability_label IS NOT NULL
        GROUP BY sustainability_label
        ORDER BY cantidad_paises DESC
    """
    return query_athena(sql)


@st.cache_data
def cargar_evolucion():
    sql = """
        SELECT
            year,
            SUM(tourist_arrivals) AS total_arrivals,
            SUM(co2)              AS total_co2
        FROM latam_sustainable_tourism.fact_tourism_emissions
        WHERE year >= 2000
        GROUP BY year
        ORDER BY year ASC
    """
    return query_athena(sql)


with st.spinner("Cargando métricas..."):
    df_metricas = cargar_metricas()
    df_sostenibilidad = cargar_sostenibilidad()
    df_evolucion = cargar_evolucion()

# --- Métricas generales ---
st.subheader("Resumen general")
col1, col2, col3, col4 = st.columns(4)
col1.metric("Países analizados", int(df_metricas["total_paises"][0]))
col2.metric("Años cubiertos", int(df_metricas["total_anios"][0]))
col3.metric(
    "Total llegadas de turistas",
    f"{df_metricas['total_llegadas'][0]:,.0f}",
)
col4.metric(
    "Total emisiones CO₂ (Mt)",
    f"{df_metricas['total_co2'][0]:,.0f}",
)

# --- Gráficos en columnas ---
st.subheader("Panorama general")
col_izq, col_der = st.columns(2)

with col_izq:
    st.markdown("**Distribución de etiquetas de sostenibilidad**")
    fig1 = px.pie(
        df_sostenibilidad,
        names="sustainability_label",
        values="cantidad_paises",
        labels={
            "sustainability_label": "Etiqueta",
            "cantidad_paises": "Cantidad de países",
        },
    )
    st.plotly_chart(fig1, use_container_width=True)

with col_der:
    st.markdown("**Evolución de turismo y CO₂ en LATAM**")
    fig2 = px.line(
        df_evolucion,
        x="year",
        y=["total_arrivals", "total_co2"],
        markers=True,
        labels={
            "year": "Año",
            "value": "Valor",
            "variable": "Métrica",
        },
    )
    st.plotly_chart(fig2, use_container_width=True)

# --- Preguntas de negocio con links ---
st.subheader("Preguntas de negocio")
st.markdown(
    """
    | # | Pregunta |
    |---|---|
    | 1 | [¿Existe relación entre el crecimiento del turismo y el aumento de emisiones de CO₂ en LATAM?](/turismo_vs_co2) |
    | 2 | [¿Qué medios de transporte turístico tienen mayor impacto ambiental?](/transporte_impacto) |
    | 3 | [¿Qué países logran crecimiento económico con menor impacto ambiental?](/crecimiento_economico) |
    | 4 | [¿Cómo evolucionan las emisiones en función del turismo a lo largo del tiempo?](/evolucion_emisiones) |
    | 5 | [¿Qué países muestran tendencias hacia un turismo más sostenible?](/turismo_sostenible) |
    """,
    unsafe_allow_html=True,
)
