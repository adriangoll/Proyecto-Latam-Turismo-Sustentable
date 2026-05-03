# LATAM Tourism Emissions App

App web interactiva que expone los datos Gold del pipeline LATAM Tourism Emissions.

**Stack:** FastAPI · pandas · boto3 · Plotly.js · Google gemini-3-flash-preview  
**Deploy:** Backend → Render free tier · Frontend → Netlify (drag & drop)

---

## Estructura

```
latam-tourism-app/
├── backend/
│   ├── main.py           # FastAPI app, CORS, endpoints
│   ├── data_loader.py    # Lee Parquet de S3 al arrancar
│   ├── questions.py      # 8 preguntas fijas → Plotly JSON
│   ├── custom_query.py   # Gemini + exec() → Plotly JSON
│   ├── requirements.txt
│   └── .env.example      # Copiar como .env y completar
└── frontend/
    ├── index.html        # Pantalla 1: Home + metric cards
    ├── explorar.html     # Pantalla 2: Preguntas + custom query
    ├── css/style.css
    └── js/
        ├── config.js     # API_URL (cambiar para producción)
        ├── main.js
        └── explorar.js
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

Opción con live-server (VS Code extension):
```bash
# Instalar Live Server en VS Code, luego click derecho en index.html → Open with Live Server
# Por defecto corre en http://localhost:5500
```

Asegurarse que en `frontend/js/config.js`:
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
| `CORS_ORIGIN` | URL del frontend en Netlify |

---

## Deploy

### Backend → Render.com

1. Subir `/backend` a GitHub
2. Render → New Web Service → conectar repo
3. **Runtime:** Python
4. **Start command:** `uvicorn main:app --host 0.0.0.0 --port $PORT`
5. **Build command:** `pip install -r requirements.txt`
6. Agregar todas las variables de entorno en el panel de Render
7. Render asigna URL: `https://latam-tourism-api.onrender.com`

### Frontend → Netlify

1. Editar `frontend/js/config.js`:
   ```js
   API_URL: "https://latam-tourism-api.onrender.com"
   ```
2. netlify.com → Add new site → Deploy manually → drag `/frontend`
3. Netlify asigna URL pública instantánea

4. Copiar la URL de Netlify y actualizar `CORS_ORIGIN` en Render.

---

## Endpoints

| Método | Endpoint | Descripción |
|---|---|---|
| GET | `/` | Health check |
| GET | `/health` | Estado del servidor + filas cargadas |
| GET | `/api/overview` | KPIs para cards del Home |
| GET | `/api/questions` | Lista de 8 preguntas fijas |
| GET | `/api/question/{id}` | Plotly JSON de una pregunta |
| GET | `/api/question/1?year=2022` | Q1 con año específico |
| POST | `/api/custom` | Custom query via Gemini |

### Ejemplo POST /api/custom

```json
{
  "countries": ["ARG", "BRA", "MEX"],
  "metric": "co2_per_capita",
  "year_range": [2015, 2022]
}
```

---

## Notas

- **Render free tier hiberna** tras 15 min sin requests → el frontend hace un ping cada 14 min automáticamente.
- **exec() de Gemini** corre en namespace controlado sin acceso a filesystem, OS ni red.
- Los Parquet se cargan en memoria al arrancar FastAPI. Si en el futuro crecen mucho, considerar lazy loading por año.