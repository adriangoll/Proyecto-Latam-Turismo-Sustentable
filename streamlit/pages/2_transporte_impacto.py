# Página 2: ¿Qué medios de transporte turístico tienen mayor impacto ambiental?

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
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

st.title("Impacto Ambiental por Medio de Transporte")
st.markdown(
    """
    Comparamos el promedio de CO₂ por turista según el medio de transporte
    dominante para identificar cuál genera mayor huella de carbono en LATAM.
    """
)

# --- Carga de datos ---
@st.cache_data
def cargar_datos():
    sql = """
        SELECT
            dominant_transport,
            COUNT(*)                 AS cantidad_registros,
            AVG(co2_per_tourist)     AS promedio_co2_por_turista,
            AVG(pct_air)             AS promedio_pct_aereo,
            --AVG(pct_sea)             AS promedio_pct_maritimo,
            AVG(pct_land)            AS promedio_pct_terrestre
        FROM latam_sustainable_tourism.fact_tourism_emissions
        WHERE dominant_transport IS NOT NULL
          AND co2_per_tourist IS NOT NULL
        GROUP BY dominant_transport
        ORDER BY promedio_co2_por_turista DESC
    """
    return query_athena(sql)

with st.spinner("Consultando Athena..."):
    df = cargar_datos()

# --- Métricas ---
st.subheader("Resumen")
cols = st.columns(len(df))
for i, row in df.iterrows():
    cols[i].metric(
        label=row["dominant_transport"],
        value=f"{row['promedio_co2_por_turista']:.2f}",
        help="CO₂ promedio por turista",
    )

# --- Selector de visualización ---
st.subheader("Explorá el impacto por transporte")
vista = st.radio(
    "Seleccioná qué querés ver",
    options=["CO₂ por turista", "Distribución de medios"],
    horizontal=True,
)

if vista == "CO₂ por turista":
    fig = px.bar(
        df,
        x="dominant_transport",
        y="promedio_co2_por_turista",
        color="dominant_transport",
        text="promedio_co2_por_turista",
        labels={
            "dominant_transport": "Medio de transporte",
            "promedio_co2_por_turista": "CO₂ promedio por turista",
        },
    )
    fig.update_traces(texttemplate="%{text:.2f}", textposition="outside")
    st.plotly_chart(fig, use_container_width=True)

else:
    medios = [
        ("promedio_pct_aereo", "steelblue", "Aéreo"),
        ("promedio_pct_terrestre", "darkorange", "Terrestre"),
    ]
    fig = go.Figure()
    for medio, color, nombre in medios:
        if medio in df.columns:
            fig.add_trace(
                go.Bar(
                    x=df["dominant_transport"],
                    y=df[medio],
                    name=nombre,
                    marker_color=color,
                )
            )
    fig.update_layout(
        barmode="group",
        xaxis_title="Medio de transporte dominante",
        yaxis_title="Porcentaje promedio (%)",
    )
    st.plotly_chart(fig, use_container_width=True)

# --- Datos ---
with st.expander("Ver datos completos"):
    st.dataframe(df, use_container_width=True)