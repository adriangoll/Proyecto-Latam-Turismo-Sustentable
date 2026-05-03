"""
main.py
FastAPI app principal.
Endpoints:
  GET  /                        → health check
  GET  /api/overview            → métricas para cards del Home
  GET  /api/questions           → lista de preguntas fijas disponibles
  GET  /api/question/{id}       → Plotly JSON de una pregunta fija
  POST /api/custom              → Custom query via Gemini
"""

import logging
import os
from contextlib import asynccontextmanager
from typing import Optional

import boto3
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

import data_loader
from data_loader import get_overview_stats
from questions import QUESTIONS_META, get_question
from custom_query import run_custom_query

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(name)s | %(message)s")
logger = logging.getLogger(__name__)


# ── Lifespan: carga los datos UNA sola vez al arrancar ───────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("🚀 Arrancando LATAM Tourism API — cargando datos de S3...")
    data_loader.load_data()
    yield
    logger.info("👋 Cerrando API.")


# ── Aplicación ────────────────────────────────────────────────────────────────
app = FastAPI(
    title       = "LATAM Tourism Emissions API",
    description = "API que expone los datos Gold del pipeline LATAM Tourism Emissions.",
    version     = "1.0.0",
    lifespan    = lifespan,
)

# CORS — permitir el frontend de Netlify (y localhost para dev)
CORS_ORIGINS = [
    o.strip()
    for o in os.getenv("CORS_ORIGIN", "http://localhost:5500,http://127.0.0.1:5500").split(",")
    if o.strip()
]

app.add_middleware(
    CORSMiddleware,
    allow_origins     = CORS_ORIGINS,
    allow_credentials = True,
    allow_methods     = ["*"],
    allow_headers     = ["*"],
)


# ── Schemas ───────────────────────────────────────────────────────────────────
class CustomQueryRequest(BaseModel):
    countries:  list[str]  = Field(default_factory=list, description="Lista de ISO codes, ej: ['ARG','BRA']")
    metric:     str         = Field("co2",  description="co2 | arrivals | co2_per_capita | yoy")
    year_range: list[int]  = Field(default_factory=lambda: [2010, 2023], description="[año_inicio, año_fin]")

    model_config = {"json_schema_extra": {
        "example": {
            "countries":  ["ARG", "BRA", "MEX"],
            "metric":     "co2_per_capita",
            "year_range": [2015, 2022],
        }
    }}


# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.get("/", tags=["health"])
def root():
    return {"status": "ok", "service": "LATAM Tourism Emissions API", "version": "1.0.0"}


@app.get("/health", tags=["health"])
def health():
    """Ping para mantener Render despierto."""
    fact = data_loader.get_fact()
    return {
        "status":         "ok",
        "fact_rows":      len(fact),
        "fact_loaded":    not fact.empty,
    }


@app.get("/api/overview", tags=["data"])
def overview():
    """
    Métricas generales para las 4 cards del Home.
    Retorna: n_countries, n_years, total_co2_mt, total_arrivals_m
    """
    try:
        stats = get_overview_stats()
        return {
            "n_countries":       stats["n_countries"],
            "n_years":           stats["n_years"],
            "total_co2_mt":      stats["total_co2_mt"],
            "total_arrivals_m":  stats["total_arrivals_m"],
        }
    except Exception as e:
        logger.error(f"Error en /api/overview: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/questions", tags=["questions"])
def list_questions():
    """Lista de las 8 preguntas fijas disponibles."""
    return {"questions": QUESTIONS_META}


@app.get("/api/question/{question_id}", tags=["questions"])
def get_question_endpoint(
    question_id: int,
    year: Optional[int] = Query(None, description="Año a filtrar (solo para Q1)"),
):
    """
    Retorna el Plotly JSON de una pregunta fija.
    - question_id: 1–8
    - year: opcional, solo aplica a la pregunta 1
    """
    params = {}
    if year and question_id == 1:
        params["year"] = year

    try:
        result = get_question(question_id, params)
        return result
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Error en /api/question/{question_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Error generando pregunta {question_id}: {str(e)}")


@app.post("/api/custom", tags=["custom"])
def custom_query(req: CustomQueryRequest):
    """
    Genera un gráfico personalizado via Gemini.
    Recibe países, métrica y rango de años.
    Retorna Plotly JSON o mensaje de error.
    """
    if len(req.year_range) != 2:
        raise HTTPException(status_code=400, detail="year_range debe tener exactamente 2 elementos: [inicio, fin]")
    if req.year_range[0] > req.year_range[1]:
        raise HTTPException(status_code=400, detail="year_range[0] debe ser <= year_range[1]")

    result = run_custom_query(
        countries  = req.countries,
        metric     = req.metric,
        year_range = req.year_range,
    )

    if "error" in result:
        # 422 para errores de generación (no 500, ya que el server está bien)
        raise HTTPException(status_code=422, detail=result["error"])

    return result


# ── Descarga de datos Gold/Silver ─────────────────────────────────────────────
# Paths en S3
DOWNLOAD_FILES = {
    "gold": {
        "key":   "open-data/v1/gold/fact_tourism_emissions/latam_fact_tourism_emissions_v1.csv",
        "label": "Gold — fact_tourism_emissions",
    },
    "silver": {
        "key":   os.getenv("SILVER_CSV_KEY", "silver/fact_tourism_emissions/data.csv"),
        "label": "Silver — fact_tourism_emissions",
    },
}

@app.get("/api/download/{layer}", tags=["data"])
def download_csv(layer: str):
    """
    Genera una URL pre-firmada de S3 para descargar el CSV de la capa indicada.
    layer: 'gold' | 'silver'
    La URL expira en 10 minutos.
    """
    if layer not in DOWNLOAD_FILES:
        raise HTTPException(status_code=404, detail=f"Capa '{layer}' no disponible. Usá 'gold' o 'silver'.")

    info = DOWNLOAD_FILES[layer]

    try:
        s3 = boto3.client(
            "s3",
            aws_access_key_id     = os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name           = os.getenv("AWS_REGION", "us-east-1"),
        )
        url = s3.generate_presigned_url(
            "get_object",
            Params     = {"Bucket": os.getenv("S3_BUCKET", "latam-sustainability-datalake"), "Key": info["key"]},
            ExpiresIn  = 600,  # 10 minutos
        )
        return {"url": url, "label": info["label"], "expires_in": 600}

    except Exception as e:
        logger.error(f"Error generando URL firmada para {layer}: {e}")
        raise HTTPException(status_code=500, detail=f"No se pudo generar el enlace de descarga: {str(e)}")