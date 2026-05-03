"""
custom_query.py
Construye el prompt con datos reales → manda a Gemini → exec() controlado → retorna Plotly JSON.
"""

import os
import re
import logging
import pandas as pd
import plotly.express as px
from google import genai
from data_loader import get_merged, _find_col

logger = logging.getLogger(__name__)

# Configurar cliente Gemini (nueva SDK google-genai)
_GEMINI_KEY = os.getenv("GEMINI_API_KEY", "")
_client = genai.Client(api_key=_GEMINI_KEY) if _GEMINI_KEY else None


# ── Constantes de seguridad ────────────────────────────────────────────────
# Lista negra de tokens que NO deben aparecer en el código generado
FORBIDDEN_TOKENS = [
    "import os", "import sys", "import subprocess", "open(",
    "__import__", "eval(", "exec(", "shutil", "requests.",
    "urllib", "socket", "pathlib", "boto3", "s3",
]

LAYOUT_INJECT = """
fig.update_layout(
    paper_bgcolor='#162030',
    plot_bgcolor='#0f1923',
    font=dict(family="'DM Sans', sans-serif", color='#e8f0ee', size=13),
    margin=dict(l=50, r=30, t=60, b=50),
)
fig.update_xaxes(gridcolor='#1e3040')
fig.update_yaxes(gridcolor='#1e3040')
"""


def _build_prompt(df: pd.DataFrame, countries: list, metric: str, year_range: list) -> str:
    """
    Construye el prompt para Gemini con un sample del DataFrame real.
    """
    # Muestra representativa: máx 30 filas
    sample = df.head(30).to_string(index=False)

    metric_names = {
        "co2":           "emisiones de CO₂ (kt)",
        "arrivals":      "llegadas internacionales",
        "co2_per_capita": "CO₂ per cápita turístico",
        "yoy":           "crecimiento YoY de llegadas (%)",
    }
    metric_desc = metric_names.get(metric, metric)

    country_str = ", ".join(countries) if countries else "todos los países"

    prompt = f"""Eres un experto en visualización de datos con Plotly Express.

Tengo un DataFrame de pandas llamado `df` con las siguientes columnas:
{list(df.columns)}

Muestra de los datos (primeras filas):
{sample}

TAREA: Escribí SOLO el código Python que cree una figura Plotly Express llamada `fig`.
La figura debe mostrar: {metric_desc} para {country_str}, entre {year_range[0]} y {year_range[1]}.

REGLAS ESTRICTAS:
1. La variable final DEBE llamarse `fig`
2. NO importes nada — pandas (pd), plotly.express (px) y plotly.graph_objects (go) ya están disponibles
3. El DataFrame ya existe como `df` — NO lo redefinás ni lo cargués de ningún lado
4. NO uses open(), os, sys, subprocess ni accedas a archivos o redes
5. Usá colores del tema oscuro: background '#0f1923', acentos '#1a7f64' o '#f5c518'
6. Respondé ÚNICAMENTE con el bloque de código Python, sin explicaciones, sin markdown
7. Si la métrica tiene muchos países, usá un gráfico de líneas o barras agrupadas

Código Python:"""

    return prompt


def _validate_code(code: str) -> tuple[bool, str]:
    """
    Valida que el código no contenga tokens peligrosos.
    Retorna (is_safe, reason)
    """
    code_lower = code.lower()
    for token in FORBIDDEN_TOKENS:
        if token.lower() in code_lower:
            return False, f"Token prohibido detectado: '{token}'"
    # El código debe crear `fig`
    if "fig" not in code:
        return False, "El código no crea una variable 'fig'"
    return True, "ok"


def _extract_code_block(text: str) -> str:
    """Extrae el bloque de código Python de la respuesta de Gemini."""
    # Intentar extraer de bloque markdown
    match = re.search(r"```(?:python)?\n?(.*?)```", text, re.DOTALL)
    if match:
        return match.group(1).strip()
    # Si no hay markdown, asumir que es código directo
    return text.strip()


def run_custom_query(
    countries: list[str],
    metric:    str,
    year_range: list[int],
) -> dict:
    """
    Pipeline completo:
    1. Filtra el DataFrame según los parámetros
    2. Construye prompt con datos reales
    3. Llama a Gemini
    4. Valida el código generado
    5. exec() en namespace controlado
    6. Retorna Plotly JSON o error

    Retorna dict con 'plotly_json' o 'error'.
    """

    if not _client:
        return {"error": "GEMINI_API_KEY no configurada. Agregala en las variables de entorno."}

    # ── 1. Filtrar DataFrame ──────────────────────────────────────────────────
    df_full  = get_merged()
    year_col = _find_col(df_full, ["year"])
    if not year_col:
        return {"error": "No se encontró columna 'year' en los datos."}

    df_filtered = df_full[
        (df_full[year_col] >= year_range[0]) &
        (df_full[year_col] <= year_range[1])
    ].copy()

    if countries:
        # Intentar filtrar por country_code o country_name
        country_col = _find_col(df_filtered, ["country_code", "iso_code", "country_name", "name", "country"])
        if country_col:
            df_filtered = df_filtered[df_filtered[country_col].isin(countries)]

    if df_filtered.empty:
        return {"error": "No hay datos para los filtros seleccionados."}

    # ── 2. Construir prompt ───────────────────────────────────────────────────
    prompt = _build_prompt(df_filtered, countries, metric, year_range)

    # ── 3. Llamar a Gemini (nueva SDK google-genai) ───────────────────────────
    try:
        response = _client.models.generate_content(
            model    = "gemini-3-flash-preview",  
            contents = prompt,
)
        raw_text = response.text
        logger.info(f"Gemini respondió ({len(raw_text)} chars)")
    except Exception as e:
        logger.error(f"Error llamando a Gemini: {e}")
        return {"error": f"Error al contactar Gemini: {str(e)}"}

    # ── 4. Extraer y validar código ───────────────────────────────────────────
    code = _extract_code_block(raw_text)
    is_safe, reason = _validate_code(code)
    if not is_safe:
        logger.warning(f"Código de Gemini rechazado: {reason}\nCódigo:\n{code}")
        return {"error": "El código generado no pasó la validación de seguridad. Reintentá con otros parámetros."}

    # ── 5. Ejecutar en namespace controlado ──────────────────────────────────
    namespace = {
        "df": df_filtered,
        "pd": pd,
        "px": px,
    }

    try:
        exec(code + "\n" + LAYOUT_INJECT, namespace)  # noqa: S102
    except Exception as e:
        logger.error(f"Error ejecutando código de Gemini: {e}\nCódigo:\n{code}")
        return {"error": f"Error al generar el gráfico: {str(e)}. Reintentá."}

    fig = namespace.get("fig")
    if fig is None:
        return {"error": "Gemini no generó una figura válida. Reintentá con otros parámetros."}

    # ── 6. Retornar JSON ──────────────────────────────────────────────────────
    try:
        plotly_json = fig.to_json()
    except Exception as e:
        return {"error": f"Error serializando el gráfico: {str(e)}"}

    return {
        "plotly_json": plotly_json,
        "code_used":   code,  # útil para debug, puede ocultarse en prod
    }