function renderProducts(container, products) {
  container.innerHTML = "";
  if (!products || products.length === 0) {
    container.innerHTML = '<div class="empty">No results.</div>';
    return;
  }
  for (const p of products) {
    const div = document.createElement("div");
    div.className = "result-item";
    div.innerHTML = `
      <div>
        <strong>${p.name}</strong><br>
        <span class="meta">${p.category} &middot; $${p.price}</span>
      </div>
      <div class="meta">id ${p.id}${p.rating_average ? ` &middot; ★ ${p.rating_average}` : ""}</div>
    `;
    container.appendChild(div);
  }
}

async function runSearch(params) {
  const container = document.getElementById("search-results");
  container.innerHTML = '<div class="empty">Searching...</div>';
  const query = new URLSearchParams(params).toString();
  const resp = await fetch(`/api/search?${query}`);
  const data = await resp.json();
  renderProducts(container, data.results);
}

document.getElementById("search-form").addEventListener("submit", (e) => {
  e.preventDefault();
  const q = document.getElementById("search-q").value;
  const category = document.getElementById("search-category").value;
  runSearch({ q, category });
});

document.getElementById("geo-search-btn").addEventListener("click", () => {
  const lat = document.getElementById("search-lat").value;
  const lon = document.getElementById("search-lon").value;
  const radius_km = document.getElementById("search-radius").value;
  if (!lat || !lon || !radius_km) {
    alert("Latitude, longitude, and radius are all required for a geo search.");
    return;
  }
  runSearch({ lat, lon, radius_km });
});

// --- Live demand chart -------------------------------------------------

let demandChart = null;
let demandPollHandle = null;

function getOrCreateChart() {
  if (demandChart) return demandChart;
  const ctx = document.getElementById("demand-chart");
  demandChart = new Chart(ctx, {
    type: "line",
    data: { labels: [], datasets: [{ label: "Events / minute", data: [], borderColor: "#5b8cff", tension: 0.25 }] },
    options: {
      responsive: true,
      scales: { x: { ticks: { color: "#9aa4b8" } }, y: { beginAtZero: true, ticks: { color: "#9aa4b8" } } },
      plugins: { legend: { labels: { color: "#e6e9ef" } } },
    },
  });
  return demandChart;
}

async function refreshDemand(productId) {
  const statusEl = document.getElementById("demand-status");
  try {
    const resp = await fetch(`/api/demand/${productId}?days=1`);
    const data = await resp.json();
    const points = (data.points || []).slice().reverse();
    const chart = getOrCreateChart();
    chart.data.labels = points.map((p) => p.event_minute.slice(11, 16));
    chart.data.datasets[0].data = points.map((p) => p.event_count);
    chart.update();
    statusEl.textContent = `Last updated ${new Date().toLocaleTimeString()} (${points.length} points)`;
  } catch (err) {
    statusEl.textContent = "Error fetching demand data";
  }
}

document.getElementById("demand-watch-btn").addEventListener("click", () => {
  const productId = document.getElementById("demand-product-id").value;
  if (demandPollHandle) clearInterval(demandPollHandle);
  refreshDemand(productId);
  demandPollHandle = setInterval(() => refreshDemand(productId), 5000);
});

// --- Recommendations -----------------------------------------------------

async function lookupRecommendations(visitorId) {
  const container = document.getElementById("rec-results");
  container.innerHTML = '<div class="empty">Looking up...</div>';
  const resp = await fetch(`/api/recommendations/${visitorId}`);
  if (resp.status === 404) {
    container.innerHTML = '<div class="empty">No precomputed recommendations for this visitor id (try one with enough history, e.g. 54).</div>';
    return;
  }
  const data = await resp.json();
  container.innerHTML = "";
  for (const r of data.recommendations || []) {
    const div = document.createElement("div");
    div.className = "result-item";
    div.innerHTML = `<div>Item <strong>${r.itemid}</strong></div><div class="meta">score ${r.score.toFixed(4)}</div>`;
    container.appendChild(div);
  }
}

document.getElementById("rec-lookup-btn").addEventListener("click", () => {
  const visitorId = document.getElementById("rec-visitor-id").value;
  lookupRecommendations(visitorId);
});

// Initial load
runSearch({});
refreshDemand(document.getElementById("demand-product-id").value);
lookupRecommendations(document.getElementById("rec-visitor-id").value);
