function escapeHtml(str) {
  const div = document.createElement("div");
  div.textContent = str == null ? "" : String(str);
  return div.innerHTML;
}

function renderProducts(container, products, { append = false } = {}) {
  if (!append) container.innerHTML = "";
  if (!append && (!products || products.length === 0)) {
    container.innerHTML = '<div class="empty">No results.</div>';
    return;
  }
  for (const p of products || []) {
    const div = document.createElement("div");
    div.className = "result-item product-item";
    const image = p.images && p.images[0];
    const amazonUrl = p.asin ? `https://www.amazon.com/dp/${p.asin}` : null;
    div.innerHTML = `
      ${image ? `<img class="product-thumb" src="${escapeHtml(image)}" alt="" loading="lazy">` : ""}
      <div class="product-body">
        <strong>${escapeHtml(p.name)}</strong><br>
        <span class="meta">
          ${escapeHtml(p.brand || "")}${p.brand ? " &middot; " : ""}${escapeHtml(p.category)} &middot; $${p.price}
          ${p.rating_average ? ` &middot; ★ ${p.rating_average} (${p.rating_count || 0})` : ""}
        </span>
      </div>
      <div class="product-side meta">
        id ${p.id}<br>
        ${amazonUrl ? `<a href="${amazonUrl}" target="_blank" rel="noopener noreferrer">View on Amazon &rarr;</a>` : ""}
      </div>
    `;
    container.appendChild(div);
  }
}

const SEARCH_PAGE_SIZE = 20;
let searchState = { params: {}, offset: 0, total: 0, shown: 0 };

function updateSearchStatus() {
  const statusEl = document.getElementById("search-status");
  const moreBtn = document.getElementById("load-more-btn");
  statusEl.textContent = searchState.total
    ? `Showing ${searchState.shown} of ${searchState.total} products`
    : "";
  moreBtn.style.display = searchState.shown < searchState.total ? "inline-block" : "none";
}

async function runSearch(params, { append = false } = {}) {
  const container = document.getElementById("search-results");
  if (append) {
    searchState.offset += SEARCH_PAGE_SIZE;
  } else {
    searchState = { params, offset: 0, total: 0, shown: 0 };
    container.innerHTML = '<div class="empty">Searching...</div>';
  }
  const query = new URLSearchParams({
    ...searchState.params,
    limit: SEARCH_PAGE_SIZE,
    offset: searchState.offset,
  }).toString();
  const resp = await fetch(`/api/search?${query}`);
  const data = await resp.json();
  renderProducts(container, data.results, { append });
  searchState.total = data.total || 0;
  searchState.shown = append ? searchState.shown + (data.results || []).length : (data.results || []).length;
  updateSearchStatus();
}

document.getElementById("load-more-btn").addEventListener("click", () => {
  runSearch(null, { append: true });
});

function currentSearchParams() {
  const params = {
    q: document.getElementById("search-q").value,
    category: document.getElementById("search-category").value,
    brand: document.getElementById("search-brand").value,
    price_min: document.getElementById("search-price-min").value,
    price_max: document.getElementById("search-price-max").value,
    rating_min: document.getElementById("search-rating-min").value,
    sort: document.getElementById("search-sort").value,
  };
  return Object.fromEntries(Object.entries(params).filter(([, v]) => v !== "" && v != null));
}

document.getElementById("search-form").addEventListener("submit", (e) => {
  e.preventDefault();
  runSearch(currentSearchParams());
});

document.getElementById("geo-search-btn").addEventListener("click", () => {
  const lat = document.getElementById("search-lat").value;
  const lon = document.getElementById("search-lon").value;
  const radius_km = document.getElementById("search-radius").value;
  if (!lat || !lon || !radius_km) {
    alert("Latitude, longitude, and radius are all required for a geo search.");
    return;
  }
  runSearch({ ...currentSearchParams(), lat, lon, radius_km });
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

// --- SQL analytics charts (Postgres, via analytics-service) --------------

let topProductsChart = null;
let eventSummaryChart = null;

async function refreshTopProducts() {
  const allRows = await (await fetch("/api/top-products?limit=10")).json();

  // Resolve product names/brands for the chart labels via a batch lookup.
  const ids = allRows.map((r) => r.product_id);
  let productsById = {};
  if (ids.length) {
    const presp = await fetch("/api/products/batch", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(ids),
    });
    if (presp.ok) {
      const products = await presp.json();
      productsById = Object.fromEntries(products.map((p) => [p.id, p]));
    }
  }

  // Stats can reference a product_id that no longer exists in the catalog
  // (e.g. deleted after the stat was recorded) -- skip those rather than
  // showing a bare, unlabeled id in the chart.
  const rows = allRows.filter((r) => productsById[r.product_id]);
  const labels = rows.map((r) => {
    const name = productsById[r.product_id].name;
    return name.length > 28 ? name.slice(0, 28) + "…" : name;
  });

  const ctx = document.getElementById("top-products-chart");
  const datasets = [
    { label: "Views", data: rows.map((r) => r.views), backgroundColor: "#5b8cff" },
    { label: "Add to cart", data: rows.map((r) => r.add_to_carts), backgroundColor: "#8c5bff" },
    { label: "Purchases", data: rows.map((r) => r.purchases), backgroundColor: "#5bffb0" },
  ];
  if (topProductsChart) {
    topProductsChart.data.labels = labels;
    topProductsChart.data.datasets = datasets;
    topProductsChart.update();
    return;
  }
  topProductsChart = new Chart(ctx, {
    type: "bar",
    data: { labels, datasets },
    options: {
      responsive: true,
      scales: {
        x: { ticks: { color: "#9aa4b8" }, stacked: true },
        y: { beginAtZero: true, ticks: { color: "#9aa4b8" }, stacked: true },
      },
      plugins: { legend: { labels: { color: "#e6e9ef" } } },
    },
  });
}

async function refreshEventSummary() {
  const resp = await fetch("/api/summary");
  const rows = await resp.json();

  const days = [...new Set(rows.map((r) => r.day))].sort();
  const eventTypes = [...new Set(rows.map((r) => r.event_type))];
  const colors = { view: "#5b8cff", add_to_cart: "#8c5bff", purchase: "#5bffb0" };

  const datasets = eventTypes.map((type) => ({
    label: type,
    data: days.map((day) => {
      const row = rows.find((r) => r.day === day && r.event_type === type);
      return row ? row.count : 0;
    }),
    borderColor: colors[type] || "#e6e9ef",
    tension: 0.25,
  }));

  const ctx = document.getElementById("event-summary-chart");
  if (eventSummaryChart) {
    eventSummaryChart.data.labels = days;
    eventSummaryChart.data.datasets = datasets;
    eventSummaryChart.update();
    return;
  }
  eventSummaryChart = new Chart(ctx, {
    type: "line",
    data: { labels: days, datasets },
    options: {
      responsive: true,
      scales: { x: { ticks: { color: "#9aa4b8" } }, y: { beginAtZero: true, ticks: { color: "#9aa4b8" } } },
      plugins: { legend: { labels: { color: "#e6e9ef" } } },
    },
  });
}

// Initial load
runSearch({});
refreshDemand(document.getElementById("demand-product-id").value);
lookupRecommendations(document.getElementById("rec-visitor-id").value);
refreshTopProducts();
refreshEventSummary();
