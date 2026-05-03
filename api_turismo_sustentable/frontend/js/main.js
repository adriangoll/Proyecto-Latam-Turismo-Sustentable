// main.js — Lógica del Home
// Funciones declaradas ANTES del listener para evitar ReferenceError con defer.

// ── Utility: enteros animados ────────────────────────────────────────────────
function animateValue(elementId, start, end, duration) {
  const el = document.getElementById(elementId);
  if (!el) return;
  const steps = Math.min(60, Math.floor(duration / 20));
  let frame = 0;
  const timer = setInterval(() => {
    frame++;
    el.textContent = Math.round(start + (end - start) * (frame / steps)).toLocaleString("es-AR");
    if (frame >= steps) { clearInterval(timer); el.textContent = end.toLocaleString("es-AR"); }
  }, duration / steps);
}

// ── Utility: decimales animados ──────────────────────────────────────────────
function animateFloat(elementId, start, end, duration, decimals) {
  decimals = decimals !== undefined ? decimals : 1;
  const el = document.getElementById(elementId);
  if (!el) return;
  if (!end || end === 0) { el.textContent = (0).toFixed(decimals); return; }
  const steps = 60;
  let frame = 0;
  const timer = setInterval(() => {
    frame++;
    const eased = 1 - Math.pow(1 - frame / steps, 3);
    el.textContent = (start + (end - start) * eased).toFixed(decimals);
    if (frame >= steps) { clearInterval(timer); el.textContent = end.toFixed(decimals); }
  }, duration / steps);
}

// ── Overview cards ───────────────────────────────────────────────────────────
async function loadOverview() {
  try {
    const res  = await fetch(CONFIG.API_URL + "/api/overview");
    if (!res.ok) throw new Error("HTTP " + res.status);
    const data = await res.json();

    console.log("Overview data:", data); // debug temporal

    animateValue("val-countries", 0, data.n_countries,      900);
    animateValue("val-years",     0, data.n_years,          900);
    animateFloat("val-co2",       0, data.total_co2_mt,     900, 1);
    animateFloat("val-arrivals",  0, data.total_arrivals_m, 900, 1);

  } catch (err) {
    console.warn("No se pudo cargar el overview:", err);
    ["val-countries", "val-years", "val-co2", "val-arrivals"].forEach(function(id) {
      var el = document.getElementById(id);
      if (el) el.textContent = "—";
    });
  }
}

// ── Top 10 chart (Q1) ────────────────────────────────────────────────────────
async function loadTop10Chart() {
  const wrapper = document.getElementById("chart-wrapper");
  const errEl   = document.getElementById("chart-error");
  wrapper.classList.add("loading");

  try {
    const res  = await fetch(CONFIG.API_URL + "/api/question/1");
    if (!res.ok) throw new Error("HTTP " + res.status);
    const data = await res.json();

    wrapper.classList.remove("loading");

    const tagEl = document.getElementById("chart-year-tag");
    if (tagEl && data.year_used) tagEl.textContent = data.year_used;

    const fig = JSON.parse(data.plotly_json);
    Plotly.newPlot("chart-container", fig.data, fig.layout, {
      responsive:     true,
      displayModeBar: false,
    });

  } catch (err) {
    console.error("Error cargando Top 10:", err);
    wrapper.classList.remove("loading");
    wrapper.style.minHeight = "60px";
    errEl.classList.add("visible");
  }
}

// ── Init ─────────────────────────────────────────────────────────────────────
document.addEventListener("DOMContentLoaded", async function() {
  await loadOverview();
  await loadTop10Chart();
  setInterval(function() {
    fetch(CONFIG.API_URL + "/health").catch(function() {});
  }, 14 * 60 * 1000);
});

// ── Descarga CSV (Gold / Silver) ─────────────────────────────────────────────
async function downloadCSV(layer) {
  const btn   = document.getElementById(`btn-${layer}`);
  const errEl = document.getElementById("download-error");
  errEl.classList.remove("visible");

  const originalText = btn.innerHTML;
  btn.disabled   = true;
  btn.innerHTML  = `<span class="spinner"></span> Generando enlace...`;

  try {
    const res  = await fetch(`${CONFIG.API_URL}/api/download/${layer}`);
    if (!res.ok) {
      const e = await res.json();
      throw new Error(e.detail || `HTTP ${res.status}`);
    }
    const data = await res.json();

    // Abrir en nueva pestaña → el browser dispara la descarga
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