# Página 4: ¿Cómo evolucionan las emisiones en función del turismo a lo largo del tiempo?

import base64

import plotly.express as px
from utils.athena_client import query_athena

import streamlit as st


def get_base64_image(path: str) -> str:
    with open(path, "rb") as f:
        return base64.b64encode(f.read()).decode()


fondo = get_base64_image("assets/fondo_claro.png")

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

st.title("Evolución de Emisiones y Turismo en el Tiempo")
st.markdown(
    """
    Analizamos año a año la relación entre el crecimiento de llegadas
    de turistas y el crecimiento de emisiones de CO₂ por país.
    """
)


# --- Carga de datos ---
@st.cache_data
def cargar_datos():
    sql = """
        SELECT
            year,
            country,
            country_code,
            tourist_arrivals,
            arrivals_growth_pct  AS crecimiento_llegadas_pct,
            co2,
            co2_growth_pct       AS crecimiento_co2_pct,
            co2_per_tourist
        FROM latam_sustainable_tourism.fact_tourism_emissions
        WHERE year >= 2000
          AND tourist_arrivals IS NOT NULL
          AND co2 IS NOT NULL
        ORDER BY country ASC, year ASC
    """
    return query_athena(sql)


with st.spinner("Consultando Athena..."):
    df = cargar_datos()

# --- Filtros en columnas ---
col1, col2 = st.columns(2)

with col1:
    paises = sorted(df["country"].unique())
    paises_seleccionados = st.multiselect(
        "Seleccioná uno o más países",
        options=paises,
        default=paises[:3],
    )

with col2:
    anio_min = int(df["year"].min())
    anio_max = int(df["year"].max())
    anio_desde, anio_hasta = st.slider(
        "Rango de años",
        min_value=anio_min,
        max_value=anio_max,
        value=(anio_min, anio_max),
    )

df_filtrado = df[(df["country"].isin(paises_seleccionados)) & (df["year"] >= anio_desde) & (df["year"] <= anio_hasta)]

# --- Selector de métrica ---
st.subheader("Explorá la evolución")
metrica = st.radio(
    "Seleccioná qué métrica querés ver",
    options=[
        "Llegadas de turistas",
        "Emisiones de CO₂",
        "CO₂ por turista",
        "Crecimiento de llegadas (%)",
        "Crecimiento de CO₂ (%)",
    ],
    horizontal=True,
)

columna_map = {
    "Llegadas de turistas": "tourist_arrivals",
    "Emisiones de CO₂": "co2",
    "CO₂ por turista": "co2_per_tourist",
    "Crecimiento de llegadas (%)": "crecimiento_llegadas_pct",
    "Crecimiento de CO₂ (%)": "crecimiento_co2_pct",
}

fig = px.line(
    df_filtrado,
    x="year",
    y=columna_map[metrica],
    color="country",
    markers=True,
    labels={
        "year": "Año",
        columna_map[metrica]: metrica,
        "country": "País",
    },
)
st.plotly_chart(fig, use_container_width=True)

# --- Datos ---
with st.expander("Ver datos completos"):
    st.dataframe(df_filtrado, use_container_width=True)
