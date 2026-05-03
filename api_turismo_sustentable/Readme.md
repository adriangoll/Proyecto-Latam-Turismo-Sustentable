# LATAM Tourism Emissions App

App web interactiva que expone los datos Gold del pipeline LATAM Tourism Emissions.

**Stack:** FastAPI · pandas · boto3 · Plotly.js · Google `gemini-3-flash-preview`  
**Deploy:** Backend → Render free tier · Frontend → Netlify (drag & drop)  
**Live:** https://latam-turismo-sustentable.netlify.app

---

## Estructura

```
api_turismo_sustentable/
├── backend/
│   ├── main.py           # FastAPI app, CORS, todos los endpoints
│   ├── data_loader.py    # Lee Parquet de S3 al arrancar (carga única en memoria)
│   ├── questions.py      # 8 consultas predefinidas → Plotly JSON
│   ├── custom_query.py   # Gemini + exec() controlado → Plotly JSON
│   ├── requirements.txt
│   ├── .python-version   # Fija Python 3.11.9 para Render
│   └── .env.example      # Copiar como .env y completar
└── frontend/
    ├── index.html        # Pantalla 1: Home + KPI cards + Top 10
    ├── explorar.html     # Pantalla 2: Consultas + visualización con IA
    ├── css/style.css
    └── js/
        ├── config.js     # API_URL — cambiar para producción
        ├── main.js       # Lógica del Home
        └── explorar.js   # Lógica de Explorar
```

---

## Setup local

### 1. Backend

```bash
cd backend
python -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate
pip install -r requirements.txt

# Configurar variables de entorno
cp .env.example .env
# Editar .env con tus credenciales AWS y Gemini API key

# Arrancar
uvicorn main:app --reload --port 8000
```

Verificar en: http://localhost:8000/docs

### 2. Frontend

Opción simple: abrir `frontend/index.html` directamente en el browser.

Con Live Server (VS Code):
```bash
# Click derecho en index.html → Open with Live Server
# Corre en http://localhost:5500
```

Verificar que `frontend/js/config.js` tenga:
```js
API_URL: "http://localhost:8000"
```

---

## Variables de entorno

| Variable | Descripción |
|---|---|
| `AWS_ACCESS_KEY_ID` | Credencial AWS |
| `AWS_SECRET_ACCESS_KEY` | Credencial AWS |
| `AWS_REGION` | `us-east-1` |
| `S3_BUCKET` | `latam-sustainability-datalake` |
| `DIM_COUNTRY_KEY` | `gold/dim_country/data.parquet` |
| `FACT_KEY` | `gold/fact_tourism_emissions/data.parquet` |
| `GEMINI_API_KEY` | Google AI Studio → https://aistudio.google.com |
| `CORS_ORIGIN` | URL del frontend en Netlify (sin barra al final) |

---

## Deploy

### Backend → Render.com

1. El backend vive en `api_turismo_sustentable/backend/` dentro del repo del pipeline
2. Render → New Web Service → conectar repo
3. **Root Directory:** `api_turismo_sustentable/backend`
4. **Runtime:** Python
5. **Build command:** `pip install -r requirements.txt`
6. **Start command:** `uvicorn main:app --host 0.0.0.0 --port $PORT`
7. Agregar todas las variables de entorno en el panel de Render
8. Render asigna URL tipo: `https://latam-tourism-api.onrender.com`

### Frontend → Netlify

1. Editar `frontend/js/config.js`:
   ```js
   API_URL: "https://latam-tourism-api.onrender.com"
   ```
2. netlify.com → Add new site → Deploy manually → arrastrar carpeta `frontend/`
3. Netlify asigna URL pública instantánea
4. Copiar esa URL y actualizar `CORS_ORIGIN` en las variables de entorno de Render

### Mantener Render activo (anti-hibernate)

El frontend hace ping automático a `/health` cada 14 minutos.  
Para garantizar disponibilidad 24/7 (ej. durante un examen), configurar **UptimeRobot**:
- Monitor type: HTTP
- URL: `https://tu-servicio.onrender.com/health`
- Intervalo: 5 minutos

---

## Endpoints

| Método | Endpoint | Descripción |
|---|---|---|
| GET | `/` | Health check básico |
| GET | `/health` | Estado + filas cargadas desde S3 |
| GET | `/api/overview` | KPIs para las 4 cards del Home |
| GET | `/api/questions` | Lista de las 8 consultas predefinidas |
| GET | `/api/question/{id}` | Plotly JSON de una consulta (1-8) |
| GET | `/api/question/1?year=2022` | Consulta 1 filtrada por año |
| POST | `/api/custom` | Visualización personalizada vía Gemini |
| GET | `/api/download/gold` | URL firmada S3 → CSV Gold |
| GET | `/api/download/silver_co2` | URL firmada S3 → CSV Silver CO₂ |
| GET | `/api/download/silver_arrivals` | URL firmada S3 → CSV Silver llegadas |
| GET | `/api/download/silver_transport` | URL firmada S3 → CSV Silver transporte |

### Ejemplo POST /api/custom

```json
{
  "countries": ["ARG", "BRA", "MEX"],
  "metric": "co2_per_capita",
  "year_range": [2015, 2022]
}
```

Métricas disponibles: `co2` · `arrivals` · `co2_per_capita` · `yoy`

---

## Notas técnicas

- **Render free tier** hiberna tras 15 min sin requests → el frontend hace ping cada 14 min. Usar UptimeRobot para disponibilidad continua.
- **Gemini custom query** genera código Python en runtime que corre en un namespace controlado — sin acceso a filesystem, OS ni red. Tokens prohibidos validados antes del `exec()`.
- **Datos en memoria** — los dos Parquet Gold se cargan una sola vez al arrancar FastAPI (`lifespan`). Con 209 filas el cold start es < 5 segundos.
- **URLs firmadas S3** expiran en 10 minutos. Si la descarga falla, hacer click de nuevo para generar una nueva URL.