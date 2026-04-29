# Página 3: ¿Qué países logran crecimiento económico con menor impacto ambiental?

import streamlit as st
import plotly.express as px
from utils.athena_client import query_athena
import base64

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

st.title("Crecimiento Económico vs Impacto Ambiental")
st.markdown(
    """
    Identificamos qué países de LATAM logran crecimiento económico
    con menor intensidad de emisiones de CO₂, buscando casos de desacople
    entre desarrollo económico y huella de carbono.
    """
)

# --- Carga de datos ---
@st.cache_data
def cargar_datos():
    sql = """
        SELECT
            country,
            country_code,
            AVG(gdp_growth_pct)    AS promedio_crecimiento_gdp,
            AVG(co2_intensity_gdp) AS promedio_intensidad_co2,
            AVG(co2_per_capita)    AS promedio_co2_per_capita,
            AVG(gdp_per_capita)    AS promedio_gdp_per_capita
        FROM latam_sustainable_tourism.fact_tourism_emissions
        WHERE gdp_growth_pct IS NOT NULL
          AND co2_intensity_gdp IS NOT NULL
          AND year >= 2000
        GROUP BY country, country_code
        ORDER BY promedio_crecimiento_gdp DESC, promedio_intensidad_co2 ASC
    """
    return query_athena(sql)

with st.spinner("Consultando Athena..."):
    df = cargar_datos()

# --- Filtro de países ---
paises = sorted(df["country"].unique())
paises_seleccionados = st.multiselect(
    "Filtrá por país",
    options=paises,
    default=paises,
)
df_filtrado = df[df["country"].isin(paises_seleccionados)]

# --- Métricas ---
col1, col2 = st.columns(2)
col1.metric(
    "País con mayor crecimiento GDP",
    df_filtrado.loc[df_filtrado["promedio_crecimiento_gdp"].idxmax(), "country"],
)
col2.metric(
    "País con menor intensidad CO₂",
    df_filtrado.loc[df_filtrado["promedio_intensidad_co2"].idxmin(), "country"],
)

# --- Selector de visualización ---
st.subheader("Explorá los datos")
vista = st.radio(
    "Seleccioná qué querés ver",
    options=["Dispersión GDP vs CO₂", "Ranking por GDP", "Ranking por intensidad CO₂"],
    horizontal=True,
)

if vista == "Dispersión GDP vs CO₂":
    fig = px.scatter(
        df_filtrado,
        x="promedio_crecimiento_gdp",
        y="promedio_intensidad_co2",
        text="country_code",
        size="promedio_gdp_per_capita",
        color="country",
        labels={
            "promedio_crecimiento_gdp": "Crecimiento promedio del GDP (%)",
            "promedio_intensidad_co2": "Intensidad de CO₂ promedio",
            "country": "País",
        },
    )
    fig.update_traces(textposition="top center")
    st.plotly_chart(fig, use_container_width=True)
    st.caption(
        "El tamaño del círculo representa el GDP per cápita promedio. "
        "Los países ideales están en el cuadrante superior izquierdo: "
        "alto crecimiento y baja intensidad de CO₂."
    )

elif vista == "Ranking por GDP":
    fig = px.bar(
        df_filtrado.sort_values("promedio_crecimiento_gdp", ascending=True),
        x="promedio_crecimiento_gdp",
        y="country",
        orientation="h",
        color="promedio_crecimiento_gdp",
        color_continuous_scale="Greens",
        labels={
            "country": "País",
            "promedio_crecimiento_gdp": "Crecimiento promedio del GDP (%)",
        },
    )
    st.plotly_chart(fig, use_container_width=True)

else:
    fig = px.bar(
        df_filtrado.sort_values("promedio_intensidad_co2", ascending=False),
        x="promedio_intensidad_co2",
        y="country",
        orientation="h",
        color="promedio_intensidad_co2",
        color_continuous_scale="Reds",
        labels={
            "country": "País",
            "promedio_intensidad_co2": "Intensidad de CO₂ promedio",
        },
    )
    st.plotly_chart(fig, use_container_width=True)

# --- Datos ---
with st.expander("Ver datos completos"):
    st.dataframe(df_filtrado, use_container_width=True)