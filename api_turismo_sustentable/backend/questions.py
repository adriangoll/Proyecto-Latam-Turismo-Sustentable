"""
questions.py
Las 8 preguntas de negocio fijas.
Columnas reales del Parquet (confirmadas):
  fact: country, country_code, year, co2, co2_per_capita, co2_per_capita_calc,
        co2_intensity_gdp, gdp, gdp_per_capita, gdp_growth_pct, population,
        share_global_co2, tourist_arrivals, tourism_receipts_usd, tourist_departures,
        arrivals_growth_pct, receipts_per_tourist, tourists_air, tourists_sea,
        tourists_land, tourists_total, pct_air, pct_sea, pct_land,
        dominant_transport, co2_per_tourist, co2_growth_pct, sustainability_label
  dim:  country_code, country_code_iso2, country_name, region_latam
"""

import logging
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from data_loader import get_fact, get_dim, get_merged, _find_col

logger = logging.getLogger(__name__)

# ── Paleta ────────────────────────────────────────────────────────────────────
TEAL      = "#1a7f64"
TEAL_SOFT = "#22a882"
YELLOW    = "#f5c518"
RED       = "#e05c2a"
BG        = "#0f1923"
PAPER_BG  = "#162030"
FONT_CLR  = "#e8f0ee"

LAYOUT_BASE = dict(
    paper_bgcolor = PAPER_BG,
    plot_bgcolor  = BG,
    font          = dict(family="'DM Sans', sans-serif", color=FONT_CLR, size=13),
    margin        = dict(l=50, r=30, t=60, b=50),
    xaxis         = dict(gridcolor="#1e3040", showgrid=True),
    yaxis         = dict(gridcolor="#1e3040", showgrid=True),
)

def _apply_layout(fig, title: str) -> None:
    fig.update_layout(title=dict(text=title, font=dict(size=16, color=FONT_CLR)), **LAYOUT_BASE)

def _safe_json(fig) -> str:
    return fig.to_json()

def _country_label(df: pd.DataFrame) -> str:
    """Retorna la columna de nombre legible de país disponible."""
    # Después del merge: country_name viene de dim, country de fact
    for col in ["country_name", "country"]:
        if col in df.columns and df[col].notna().any():
            return col
    return "country_code"


# ─────────────────────────────────────────────────────────────────────────────
# Q1 — Top 10 países por emisiones CO2
# ─────────────────────────────────────────────────────────────────────────────
def q1_top10_co2(year: int | None = None) -> dict:
    df = get_merged()
    label_col = _country_label(df)

    available_years = sorted(df["year"].dropna().unique().tolist())
    if year is None:
        year = int(df["year"].max())

    filtered = (df[df["year"] == year]
                .dropna(subset=["co2"])
                .nlargest(10, "co2"))

    fig = px.bar(
        filtered,
        x      = "co2",
        y      = label_col,
        orientation = "h",
        color  = "co2",
        color_continuous_scale = [TEAL, YELLOW],
        labels = {"co2": "CO₂ (Mt)", label_col: ""},
    )
    fig.update_layout(coloraxis_showscale=False,
                      yaxis=dict(categoryorder="total ascending"))
    _apply_layout(fig, f"Top 10 países por emisiones CO₂ — {year}")

    return {
        "plotly_json":    _safe_json(fig),
        "title":          f"Top 10 países por emisiones CO₂ ({year})",
        "description":    f"Ranking de los 10 países LATAM con mayores emisiones de CO₂ del turismo en {year}.",
        "year_used":      year,
        "available_years": available_years,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Q2 — Evolución de tourist_arrivals en LATAM
# ─────────────────────────────────────────────────────────────────────────────
def q2_arrivals_evolution() -> dict:
    df = get_fact()
    agg = (df.dropna(subset=["tourist_arrivals"])
             .groupby("year")["tourist_arrivals"]
             .sum()
             .reset_index())

    fig = px.area(
        agg, x="year", y="tourist_arrivals",
        labels={"tourist_arrivals": "Llegadas internacionales", "year": "Año"},
        color_discrete_sequence=[TEAL],
    )
    fig.update_traces(fillcolor=TEAL, opacity=0.35, line=dict(color=YELLOW, width=2.5))
    _apply_layout(fig, "Evolución de llegadas internacionales — LATAM")

    return {
        "plotly_json": _safe_json(fig),
        "title":       "Evolución de llegadas internacionales en LATAM",
        "description": "Total de llegadas internacionales sumadas para toda LATAM por año.",
    }


# ─────────────────────────────────────────────────────────────────────────────
# Q3 — CO2 per cápita por país (último año)
# ─────────────────────────────────────────────────────────────────────────────
def q3_co2_per_capita() -> dict:
    df = get_merged()
    label_col = _country_label(df)
    last_year = int(df["year"].max())

    filtered = (df[df["year"] == last_year]
                .dropna(subset=["co2_per_capita"])
                .sort_values("co2_per_capita", ascending=False))

    fig = px.bar(
        filtered,
        x      = label_col,
        y      = "co2_per_capita",
        color  = "co2_per_capita",
        color_continuous_scale = [TEAL, YELLOW, RED],
        labels = {"co2_per_capita": "CO₂ per cápita (t)", label_col: ""},
    )
    fig.update_layout(coloraxis_showscale=True, xaxis_tickangle=-40)
    _apply_layout(fig, f"CO₂ per cápita turístico por país — {last_year}")

    return {
        "plotly_json": _safe_json(fig),
        "title":       f"CO₂ per cápita turístico por país ({last_year})",
        "description": "Emisiones de CO₂ del turismo divididas por llegadas internacionales.",
    }


# ─────────────────────────────────────────────────────────────────────────────
# Q4 — Crecimiento YoY de arrivals  (columna real: arrivals_growth_pct)
# ─────────────────────────────────────────────────────────────────────────────
def q4_yoy_growth() -> dict:
    df = get_merged()
    label_col = _country_label(df)
    last_year = 2019

    # Usar columna pre-calculada si existe
    if "arrivals_growth_pct" in df.columns:
        filtered = (df[df["year"] == last_year]
                    .dropna(subset=["arrivals_growth_pct"])
                    .sort_values("arrivals_growth_pct", ascending=False))
        yoy_values = filtered["arrivals_growth_pct"].tolist()
        x_values   = filtered[label_col].tolist()
    else:
        # Calcular en runtime desde tourist_arrivals
        logger.warning("Q4: arrivals_growth_pct no encontrada, calculando en runtime")
        agg = (df.groupby(["year", label_col])["tourist_arrivals"]
                 .sum().reset_index()
                 .sort_values([label_col, "year"]))
        agg["arrivals_growth_pct"] = agg.groupby(label_col)["tourist_arrivals"].pct_change() * 100
        filtered = (agg[agg["year"] == last_year]
                    .dropna(subset=["arrivals_growth_pct"])
                    .sort_values("arrivals_growth_pct", ascending=False))
        yoy_values = filtered["arrivals_growth_pct"].tolist()
        x_values   = filtered[label_col].tolist()

    if not yoy_values:
        return {
            "plotly_json": None,
            "title": f"Crecimiento YoY de llegadas ({last_year})",
            "description": "No hay datos suficientes para calcular el crecimiento interanual en el último año.",
        }
    colors = [TEAL if v >= 0 else RED for v in yoy_values]

    fig = go.Figure(go.Bar(
        x            = x_values,
        y            = yoy_values,
        marker_color = colors,
        text         = [f"{v:+.1f}%" for v in yoy_values],
        textposition = "outside",
    ))
    fig.add_hline(y=0, line_color=FONT_CLR, line_width=1, opacity=0.4)
    _apply_layout(fig, f"Crecimiento YoY de llegadas internacionales — {last_year}")
    fig.update_layout(xaxis_tickangle=-40)

    return {
        "plotly_json": _safe_json(fig),
        "title":       f"Crecimiento YoY de llegadas ({last_year})",
        "description": "Variación porcentual de llegadas internacionales respecto al año anterior.",
    }


# ─────────────────────────────────────────────────────────────────────────────
# Q5 — Scatter: tourist_arrivals vs co2
# ─────────────────────────────────────────────────────────────────────────────
def q5_arrivals_vs_co2() -> dict:
    df = get_merged()
    label_col = _country_label(df)
    last_year = int(df["year"].max())

    filtered = (df[df["year"] == last_year]
                .dropna(subset=["co2", "tourist_arrivals", label_col])
                .query("co2 > 0 and tourist_arrivals > 0")
                .copy())

    # Si el último año está vacío, buscar el año con más datos
    if filtered.empty:
        logger.warning("Q5: último año sin datos, buscando año anterior con datos")
        df_valid = (df.dropna(subset=["co2", "tourist_arrivals"])
                     .query("co2 > 0 and tourist_arrivals > 0"))
        if df_valid.empty:
            raise ValueError("Q5: no hay datos válidos de co2 y tourist_arrivals")
        last_year = int(df_valid["year"].max())
        filtered  = df_valid[df_valid["year"] == last_year].copy()

    fig = px.scatter(
        filtered,
        x        = "tourist_arrivals",
        y        = "co2",
        text     = label_col,
        size     = "co2",
        color    = "co2",
        color_continuous_scale = [TEAL, YELLOW, RED],
        labels   = {"tourist_arrivals": "Llegadas internacionales", "co2": "CO₂ (Mt)"},
        size_max = 60,
        hover_data = {label_col: True, "co2": ":.2f", "tourist_arrivals": ":,.0f"},
    )
    fig.update_traces(
        textposition = "top center",
        textfont     = dict(size=10, color=FONT_CLR),
    )
    _apply_layout(fig, f"Relación: Llegadas vs Emisiones CO₂ — {last_year}")

    return {
        "plotly_json": _safe_json(fig),
        "title":       f"Llegadas vs Emisiones CO₂ ({last_year})",
        "description": "Correlación entre volumen de turistas y emisiones de CO₂ por país.",
    }


# ─────────────────────────────────────────────────────────────────────────────
# Q6 — Participación % en emisiones LATAM (pie)
# ─────────────────────────────────────────────────────────────────────────────
def q6_share_pie() -> dict:
    df = get_merged()
    label_col = _country_label(df)
    last_year = int(df["year"].max())

    agg = (df[df["year"] == last_year]
           .dropna(subset=["co2"])
           .groupby(label_col)["co2"]
           .sum()
           .reset_index()
           .sort_values("co2", ascending=False))

    top10  = agg.head(10)
    others = agg.iloc[10:]
    if not others.empty:
        top10 = pd.concat([
            top10,
            pd.DataFrame([{label_col: "Otros", "co2": others["co2"].sum()}])
        ], ignore_index=True)

    fig = px.pie(
        top10, names=label_col, values="co2",
        color_discrete_sequence=px.colors.sequential.Teal,
        hole=0.45,
    )
    fig.update_traces(textinfo="percent+label", textfont_size=11)
    _apply_layout(fig, f"Participación % en emisiones CO₂ LATAM — {last_year}")

    return {
        "plotly_json": _safe_json(fig),
        "title":       f"Participación % en emisiones CO₂ ({last_year})",
        "description": "Peso relativo de cada país en las emisiones totales de CO₂ del turismo.",
    }


# ─────────────────────────────────────────────────────────────────────────────
# Q7 — Pre/post pandemia: 2019 vs 2022
# ─────────────────────────────────────────────────────────────────────────────
def q7_pre_post_pandemic() -> dict:
    df = get_merged()
    label_col = _country_label(df)
    available  = df["year"].dropna().unique()

    pre_year  = 2019 if 2019 in available else int(min(available))
    post_year = 2022 if 2022 in available else int(max(available))

    pre  = (df[df["year"] == pre_year]
            .groupby(label_col)["tourist_arrivals"].sum().reset_index())
    post = (df[df["year"] == post_year]
            .groupby(label_col)["tourist_arrivals"].sum().reset_index())

    merged = pre.merge(post, on=label_col, suffixes=(f"_{pre_year}", f"_{post_year}"))
    merged = merged.sort_values(f"tourist_arrivals_{post_year}", ascending=False).head(15)

    fig = go.Figure()
    fig.add_trace(go.Bar(
        name=str(pre_year),
        x=merged[label_col],
        y=merged[f"tourist_arrivals_{pre_year}"],
        marker_color=TEAL,
    ))
    fig.add_trace(go.Bar(
        name=str(post_year),
        x=merged[label_col],
        y=merged[f"tourist_arrivals_{post_year}"],
        marker_color=YELLOW,
    ))
    fig.update_layout(barmode="group", xaxis_tickangle=-40)
    _apply_layout(fig, f"Llegadas: {pre_year} vs {post_year} (pre/post pandemia)")

    return {
        "plotly_json": _safe_json(fig),
        "title":       f"Comparación {pre_year} vs {post_year}",
        "description": f"Recuperación del turismo comparando {pre_year} (pre-pandemia) con {post_year}.",
    }


# ─────────────────────────────────────────────────────────────────────────────
# Q8 — Reducción de emisiones CO2 entre primer y último año
# ─────────────────────────────────────────────────────────────────────────────
def q8_emission_reduction() -> dict:
    df = get_merged()
    label_col = _country_label(df)
    years = sorted(df["year"].dropna().unique())

    if len(years) < 2:
        raise ValueError("Q8: se necesitan al menos 2 años de datos")

    first_year, last_year = int(years[0]), int(years[-1])

    first = (df[df["year"] == first_year]
             .groupby(label_col)["co2"].sum().reset_index())
    last  = (df[df["year"] == last_year]
             .groupby(label_col)["co2"].sum().reset_index())

    merged = first.merge(last, on=label_col, suffixes=("_first", "_last"))
    merged = merged[merged["co2_first"] > 0]  # evitar división por cero
    merged["reduction_pct"] = ((merged["co2_first"] - merged["co2_last"])
                                / merged["co2_first"] * 100)
    merged = merged.sort_values("reduction_pct", ascending=False)

    colors = [TEAL if v > 0 else RED for v in merged["reduction_pct"]]

    fig = go.Figure(go.Bar(
        x            = merged[label_col],
        y            = merged["reduction_pct"],
        marker_color = colors,
        text         = [f"{v:+.1f}%" for v in merged["reduction_pct"]],
        textposition = "outside",
    ))
    fig.add_hline(y=0, line_color=FONT_CLR, line_width=1, opacity=0.4)
    _apply_layout(fig, f"Reducción de emisiones CO₂ — {first_year} a {last_year}")
    fig.update_layout(xaxis_tickangle=-40)

    return {
        "plotly_json": _safe_json(fig),
        "title":       f"Reducción de emisiones ({first_year}→{last_year})",
        "description": f"Variación % en emisiones CO₂ del turismo entre {first_year} y {last_year}.",
    }


# ─────────────────────────────────────────────────────────────────────────────
# Dispatcher
# ─────────────────────────────────────────────────────────────────────────────
QUESTIONS_META = [
    {"id": 1, "title": "Top 10 países por emisiones CO₂",        "description": "Ranking anual de los mayores emisores del turismo en LATAM."},
    {"id": 2, "title": "Evolución de llegadas en LATAM",          "description": "Serie temporal de llegadas internacionales 2010–2023."},
    {"id": 3, "title": "CO₂ per cápita por país",                 "description": "Emisiones de CO₂ divididas por llegadas internacionales."},
    {"id": 4, "title": "Crecimiento YoY de llegadas",             "description": "Variación porcentual de llegadas respecto al año anterior."},
    {"id": 5, "title": "Llegadas vs Emisiones (scatter)",         "description": "Correlación entre volumen de turistas y huella de carbono."},
    {"id": 6, "title": "Participación % en emisiones LATAM",      "description": "Peso relativo de cada país en el total regional."},
    {"id": 7, "title": "Comparación pre/post pandemia",           "description": "Recuperación del turismo: 2019 vs 2022."},
    {"id": 8, "title": "Países con mayor reducción de emisiones", "description": "Quién más redujo su huella de carbono turística."},
]

QUESTION_HANDLERS = {i: f for i, f in enumerate([
    None,
    q1_top10_co2, q2_arrivals_evolution, q3_co2_per_capita,
    q4_yoy_growth, q5_arrivals_vs_co2, q6_share_pie,
    q7_pre_post_pandemic, q8_emission_reduction,
], start=0)}

def get_question(question_id: int, params: dict = None) -> dict:
    if question_id not in QUESTION_HANDLERS or QUESTION_HANDLERS[question_id] is None:
        raise ValueError(f"Pregunta {question_id} no existe. Válidas: 1-8")
    return QUESTION_HANDLERS[question_id](**(params or {}))