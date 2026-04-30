# Página 5: ¿Qué países muestran tendencias hacia un turismo más sostenible?

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

st.title("Tendencias hacia un Turismo Sostenible")
st.markdown(
    """
    Utilizamos el campo `sustainability_label` calculado en la capa Gold
    para identificar qué países de LATAM muestran tendencias hacia un turismo
    más sostenible a lo largo del tiempo.
    """
)


# --- Carga de datos ---
@st.cache_data
def cargar_datos():
    sql = """
        SELECT
            country,
            country_code,
            year,
            sustainability_label,
            tourist_arrivals,
            co2_per_tourist,
            arrivals_growth_pct  AS crecimiento_llegadas_pct,
            co2_growth_pct       AS crecimiento_co2_pct
        FROM latam_sustainable_tourism.fact_tourism_emissions
        WHERE sustainability_label IS NOT NULL
          AND year >= 2000
        ORDER BY sustainability_label ASC, country ASC, year ASC
    """
    return query_athena(sql)


with st.spinner("Consultando Athena..."):
    df = cargar_datos()

# --- Métricas generales ---
etiquetas = sorted(df["sustainability_label"].unique())
cols = st.columns(len(etiquetas))
for i, etiqueta in enumerate(etiquetas):
    cantidad = df[df["sustainability_label"] == etiqueta]["country"].nunique()
    cols[i].metric(label=etiqueta, value=f"{cantidad} países")

# --- Filtros en columnas ---
col1, col2 = st.columns(2)

with col1:
    etiqueta_seleccionada = st.selectbox(
        "Filtrá por etiqueta de sostenibilidad",
        options=["Todas"] + etiquetas,
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

if etiqueta_seleccionada == "Todas":
    df_filtrado = df[(df["year"] >= anio_desde) & (df["year"] <= anio_hasta)]
else:
    df_filtrado = df[(df["sustainability_label"] == etiqueta_seleccionada) & (df["year"] >= anio_desde) & (df["year"] <= anio_hasta)]

# --- Selector de visualización ---
st.subheader("Explorá los datos")
vista = st.radio(
    "Seleccioná qué querés ver",
    options=[
        "Distribución de etiquetas",
        "CO₂ por turista por país",
        "Evolución temporal por país",
    ],
    horizontal=True,
)

if vista == "Distribución de etiquetas":
    conteo = df_filtrado.groupby("sustainability_label")["country"].count().reset_index()
    conteo.columns = ["sustainability_label", "cantidad"]
    fig = px.pie(
        conteo,
        names="sustainability_label",
        values="cantidad",
        labels={
            "sustainability_label": "Etiqueta",
            "cantidad": "Cantidad de registros",
        },
    )
    st.plotly_chart(fig, use_container_width=True)

elif vista == "CO₂ por turista por país":
    resumen = df_filtrado.groupby(["country", "sustainability_label"])["co2_per_tourist"].mean().reset_index()
    fig = px.bar(
        resumen.sort_values("co2_per_tourist", ascending=True),
        x="co2_per_tourist",
        y="country",
        color="sustainability_label",
        orientation="h",
        labels={
            "country": "País",
            "co2_per_tourist": "CO₂ promedio por turista",
            "sustainability_label": "Etiqueta",
        },
    )
    st.plotly_chart(fig, use_container_width=True)

else:
    paises = sorted(df_filtrado["country"].unique())
    paises_seleccionados = st.multiselect(
        "Seleccioná países",
        options=paises,
        default=paises[:3],
    )
    df_linea = df_filtrado[df_filtrado["country"].isin(paises_seleccionados)]
    fig = px.line(
        df_linea,
        x="year",
        y="co2_per_tourist",
        color="country",
        markers=True,
        labels={
            "year": "Año",
            "co2_per_tourist": "CO₂ por turista",
            "country": "País",
        },
    )
    st.plotly_chart(fig, use_container_width=True)

# --- Datos ---
with st.expander("Ver datos completos"):
    st.dataframe(df_filtrado, use_container_width=True)
