# Página 1: ¿Existe relación entre el crecimiento del turismo
# y el aumento de emisiones de CO₂ en LATAM?

import base64

import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from utils.athena_client import query_athena


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

st.title("Turismo vs Emisiones de CO₂ en LATAM")
st.markdown(
    """
    Analizamos si existe relación entre el crecimiento del turismo
    y el aumento de emisiones de CO₂ en la región LATAM desde el año 2000.
    """
)


# --- Carga de datos ---
@st.cache_data
def cargar_datos():
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


with st.spinner("Consultando Athena..."):
    df = cargar_datos()

# --- Filtro de rango de años ---
anio_min = int(df["year"].min())
anio_max = int(df["year"].max())
anio_desde, anio_hasta = st.slider(
    "Seleccioná el rango de años",
    min_value=anio_min,
    max_value=anio_max,
    value=(anio_min, anio_max),
)
df_filtrado = df[(df["year"] >= anio_desde) & (df["year"] <= anio_hasta)]

# --- Métricas ---
correlacion = df_filtrado["total_arrivals"].corr(df_filtrado["total_co2"])
col1, col2, col3 = st.columns(3)
col1.metric("Años analizados", len(df_filtrado))
col2.metric("Correlación turismo / CO₂", f"{correlacion:.2f}")
col3.metric(
    "Crecimiento llegadas",
    f"{((df_filtrado['total_arrivals'].iloc[-1] / df_filtrado['total_arrivals'].iloc[0]) - 1) * 100:.1f}%",
)

# --- Gráfico combinado con doble eje Y ---
st.subheader("Evolución conjunta de turismo y emisiones de CO₂")

fig = go.Figure()

fig.add_trace(
    go.Scatter(
        x=df_filtrado["year"],
        y=df_filtrado["total_arrivals"],
        name="Llegadas de turistas",
        mode="lines+markers",
        line=dict(color="steelblue"),
        yaxis="y1",
    )
)

fig.add_trace(
    go.Scatter(
        x=df_filtrado["year"],
        y=df_filtrado["total_co2"],
        name="Emisiones de CO₂ (Mt)",
        mode="lines+markers",
        line=dict(color="red"),
        yaxis="y2",
    )
)

fig.update_layout(
    xaxis=dict(title="Año"),
    yaxis=dict(title=dict(text="Llegadas de turistas", font=dict(color="steelblue"))),
    yaxis2=dict(
        title=dict(text="Emisiones de CO₂ (Mt)", font=dict(color="red")),
        overlaying="y",
        side="right",
    ),
    legend=dict(x=0.01, y=0.99),
    hovermode="x unified",
)
st.plotly_chart(fig, use_container_width=True)

# --- Gráfico de dispersión ---
st.subheader("Dispersión: llegadas vs emisiones")
fig2 = px.scatter(
    df_filtrado,
    x="total_arrivals",
    y="total_co2",
    text="year",
    trendline="ols",
    labels={
        "total_arrivals": "Llegadas de turistas",
        "total_co2": "Emisiones de CO₂ (Mt)",
    },
)
fig2.update_traces(textposition="top center")
st.plotly_chart(fig2, use_container_width=True)

# --- Tabla de datos ---
with st.expander("Ver datos completos"):
    st.dataframe(df_filtrado, use_container_width=True)
