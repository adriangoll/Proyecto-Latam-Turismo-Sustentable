// explorar.js — Lógica de la pantalla Explorar

// ── Lista de países LATAM para el selector ─────────────────────────────────
// Se usa como fallback si la API no retorna la lista dinámica
const LATAM_COUNTRIES = [
  { code: "ARG", name: "Argentina" },
  { code: "BOL", name: "Bolivia" },
  { code: "BRA", name: "Brasil" },
  { code: "CHL", name: "Chile" },
  { code: "COL", name: "Colombia" },
  { code: "CRI", name: "Costa Rica" },
  { code: "CUB", name: "Cuba" },
  { code: "DOM", name: "República Dominicana" },
  { code: "ECU", name: "Ecuador" },
  { code: "SLV", name: "El Salvador" },
  { code: "GTM", name: "Guatemala" },
  { code: "HND", name: "Honduras" },
  { code: "MEX", name: "México" },
  { code: "NIC", name: "Nicaragua" },
  { code: "PAN", name: "Panamá" },
  { code: "PRY", name: "Paraguay" },
  { code: "PER", name: "Perú" },
  { code: "PRI", name: "Puerto Rico" },
  { code: "URY", name: "Uruguay" },
  { code: "VEN", name: "Venezuela" },
];

let activeQuestionId = null;
let questionsData    = [];

document.addEventListener("DOMContentLoaded", async () => {
  await loadQuestions();
  populateCountrySelect();
  bindCustomQuery();

  // Ping anti-hibernate
  setInterval(() => {
    fetch(`${CONFIG.API_URL}/health`).catch(() => {});
  }, 14 * 60 * 1000);
});

// ── A. Preguntas fijas ──────────────────────────────────────────────────────

async function loadQuestions() {
  const grid = document.getElementById("questions-grid");
  try {
    const res  = await fetch(`${CONFIG.API_URL}/api/questions`);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();
    questionsData = data.questions;
    renderQuestionCards(questionsData);
  } catch (err) {
    console.warn("No se pudieron cargar las preguntas:", err);
    grid.innerHTML = `<p style="color:var(--text-muted); font-size:13px; padding:12px;">
      No se pudo conectar con la API. Verificá que el backend esté corriendo en
      <code style="color:var(--teal-light)">${CONFIG.API_URL}</code>.
    </p>`;
  }
}

function renderQuestionCards(questions) {
  const grid = document.getElementById("questions-grid");
  grid.innerHTML = questions.map(q => `
    <div class="q-card fade-up" data-id="${q.id}" onclick="selectQuestion(${q.id})">
      <div class="q-num">Pregunta ${String(q.id).padStart(2, "0")}</div>
      <div class="q-title">${q.title}</div>
      <div class="q-desc">${q.description}</div>
    </div>
  `).join("");
}

async function selectQuestion(id) {
  // Toggle: si ya está activa, la cierra
  if (activeQuestionId === id) {
    activeQuestionId = null;
    document.querySelectorAll(".q-card").forEach(c => c.classList.remove("active"));
    document.getElementById("fixed-chart-area").classList.remove("visible");
    return;
  }

  activeQuestionId = id;

  // Marcar card activa
  document.querySelectorAll(".q-card").forEach(c => {
    c.classList.toggle("active", parseInt(c.dataset.id) === id);
  });

  // Mostrar área del gráfico
  const area    = document.getElementById("fixed-chart-area");
  const errEl   = document.getElementById("fixed-chart-error");
  const titleEl = document.getElementById("fixed-chart-title");
  const descEl  = document.getElementById("fixed-chart-desc");
  const yearFilter = document.getElementById("year-filter");

  area.classList.add("visible");
  errEl.classList.remove("visible");
  titleEl.textContent = "Cargando...";
  descEl.textContent  = "";
  document.getElementById("fixed-chart-container").innerHTML = "";

  // Filtro de año solo para Q1
  yearFilter.style.display = id === 1 ? "block" : "none";

  await fetchAndRenderFixed(id);

  // Hacer scroll suave al gráfico
  area.scrollIntoView({ behavior: "smooth", block: "start" });
}

async function fetchAndRenderFixed(id, extraParams = "") {
  const errEl   = document.getElementById("fixed-chart-error");
  const titleEl = document.getElementById("fixed-chart-title");
  const descEl  = document.getElementById("fixed-chart-desc");

  try {
    const url = `${CONFIG.API_URL}/api/question/${id}${extraParams}`;
    const res = await fetch(url);
    if (!res.ok) {
      const err = await res.json();
      throw new Error(err.detail || `HTTP ${res.status}`);
    }
    const data = await res.json();

    titleEl.textContent = data.title || `Pregunta ${id}`;
    descEl.textContent  = data.description || "";

    // Poblar select de años para Q1
    if (id === 1 && data.available_years) {
      const sel = document.getElementById("year-select");
      sel.innerHTML = data.available_years.map(y =>
        `<option value="${y}" ${y === data.year_used ? "selected" : ""}>${y}</option>`
      ).join("");

      // Bind apply button
      document.getElementById("apply-year-filter").onclick = async () => {
        const year = document.getElementById("year-select").value;
        document.getElementById("fixed-chart-container").innerHTML = "";
        await fetchAndRenderFixed(1, `?year=${year}`);
      };
    }

    // Renderizar
    const fig = JSON.parse(data.plotly_json);
    Plotly.newPlot("fixed-chart-container", fig.data, fig.layout, {
      responsive:     true,
      displayModeBar: true,
      modeBarButtonsToRemove: ["lasso2d", "select2d"],
      displaylogo: false,
    });

  } catch (err) {
    console.error("Error en pregunta fija:", err);
    errEl.textContent = `Error: ${err.message}`;
    errEl.classList.add("visible");
  }
}

// Cerrar gráfico fijo
document.addEventListener("DOMContentLoaded", () => {
  document.getElementById("close-fixed-chart").addEventListener("click", () => {
    document.getElementById("fixed-chart-area").classList.remove("visible");
    document.querySelectorAll(".q-card").forEach(c => c.classList.remove("active"));
    activeQuestionId = null;
  });
});

// ── B. Custom Query (Gemini) ────────────────────────────────────────────────

function populateCountrySelect() {
  const container = document.getElementById("countries-select");

  // "Todos los países" toggle
  const allId = "chk-all";
  let html = `
    <label class="chk-item chk-all-item">
      <input type="checkbox" id="${allId}" value="__all__" />
      <span class="chk-box"></span>
      <span class="chk-label">Todos los países</span>
    </label>
    <div class="chk-divider"></div>
  `;

  LATAM_COUNTRIES.forEach(c => {
    html += `
      <label class="chk-item">
        <input type="checkbox" class="country-chk" value="${c.code}" />
        <span class="chk-box"></span>
        <span class="chk-label">${c.name}</span>
      </label>
    `;
  });

  container.innerHTML = html;

  // Lógica: "Todos" marca/desmarca todos
  const allChk = document.getElementById(allId);
  allChk.addEventListener("change", () => {
    document.querySelectorAll(".country-chk").forEach(chk => {
      chk.checked = allChk.checked;
    });
  });

  // Si se desmarca alguno, quitar "Todos"
  container.addEventListener("change", (e) => {
    if (e.target.classList.contains("country-chk")) {
      const allChecked = [...document.querySelectorAll(".country-chk")].every(c => c.checked);
      allChk.checked = allChecked;
    }
  });
}

function getSelectedCountries() {
  const allChk = document.getElementById("chk-all");
  if (allChk && allChk.checked) return []; // vacío = todos los países en la API
  return [...document.querySelectorAll(".country-chk:checked")].map(c => c.value);
}

function bindCustomQuery() {
  const btn     = document.getElementById("run-custom-btn");
  const errEl   = document.getElementById("custom-error");
  const btnText = document.getElementById("custom-btn-text");
  const spinner = document.getElementById("custom-btn-spinner");

  btn.addEventListener("click", async () => {
    errEl.classList.remove("visible");

    // Leer parámetros
    const countries    = getSelectedCountries();
    const metric       = document.getElementById("metric-select").value;
    const yearFrom     = parseInt(document.getElementById("year-from").value);
    const yearTo       = parseInt(document.getElementById("year-to").value);

    // Validación básica
    if (yearFrom > yearTo) {
      showError(errEl, "El año de inicio debe ser menor o igual al año de fin.");
      return;
    }

    // Estado: cargando
    btn.disabled       = true;
    btnText.textContent = "Generando...";
    spinner.style.display = "inline-block";
    document.getElementById("custom-chart-area").classList.remove("visible");

    try {
      const res = await fetch(`${CONFIG.API_URL}/api/custom`, {
        method:  "POST",
        headers: { "Content-Type": "application/json" },
        body:    JSON.stringify({
          countries:  countries,
          metric:     metric,
          year_range: [yearFrom, yearTo],
        }),
      });

      if (!res.ok) {
        const errData = await res.json();
        throw new Error(errData.detail || `Error HTTP ${res.status}`);
      }

      const data = await res.json();

      // Mostrar gráfico
      const area = document.getElementById("custom-chart-area");
      area.classList.add("visible");
      area.scrollIntoView({ behavior: "smooth", block: "start" });

      const fig = JSON.parse(data.plotly_json);
      Plotly.newPlot("custom-chart-container", fig.data, fig.layout, {
        responsive:     true,
        displayModeBar: true,
        modeBarButtonsToRemove: ["lasso2d", "select2d"],
        displaylogo: false,
      });

    } catch (err) {
      console.error("Error en custom query:", err);
      showError(errEl, err.message || "No se pudo generar el gráfico. Reintentá.");
    } finally {
      btn.disabled          = false;
      btnText.textContent   = "Generar gráfico con IA →";
      spinner.style.display = "none";
    }
  });

  // Cerrar custom chart
  document.getElementById("close-custom-chart").addEventListener("click", () => {
    document.getElementById("custom-chart-area").classList.remove("visible");
  });
}

function showError(el, msg) {
  el.textContent = msg;
  el.classList.add("visible");
}

// ── Descarga CSV (Gold / Silver) ─────────────────────────────────────────────
async function downloadCSV(layer) {
  const btn   = document.getElementById(`btn-${layer}`);
  const errEl = document.getElementById("download-error");
  errEl.classList.remove("visible");

  const originalText = btn.innerHTML;
  btn.disabled  = true;
  btn.innerHTML = `<span class="spinner"></span> Generando enlace...`;

  try {
    const res  = await fetch(`${CONFIG.API_URL}/api/download/${layer}`);
    if (!res.ok) {
      const e = await res.json();
      throw new Error(e.detail || `HTTP ${res.status}`);
    }
    const data = await res.json();
    window.open(data.url, "_blank");

  } catch (err) {
    errEl.textContent = `Error: ${err.message}`;
    errEl.classList.add("visible");
  } finally {
    btn.disabled  = false;
    btn.innerHTML = originalText;
  }
}

// ── Nav sticky al scroll ──────────────────────────────────────────────────────
(function() {
  const nav = document.querySelector('nav');
  if (!nav) return;
  let sticky = false;
  const navHeight = nav.offsetHeight;

  window.addEventListener('scroll', function() {
    if (window.scrollY > navHeight && !sticky) {
      sticky = true;
      nav.classList.add('scrolled');
    } else if (window.scrollY <= navHeight && sticky) {
      sticky = false;
      nav.classList.remove('scrolled');
    }
  }, { passive: true });
})();