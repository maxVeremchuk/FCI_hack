const state = {
  layer: 'transportation',
  sublayer: '',
  map: null,
  chart: null,
  drawLayer: null,
  overlays: {},
  formulas: [],
  dashboard: null,
  bbox: null,
};

async function api(path, options = {}) {
  const res = await fetch(path, options);
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
}

function fmt(v, digits = 1) {
  if (v === null || v === undefined || Number.isNaN(Number(v))) return '—';
  const n = Number(v);
  if (Math.abs(n) >= 1000) return n.toLocaleString(undefined, { maximumFractionDigits: digits });
  return n.toFixed(digits);
}

function scoreRingHtml(score, cls = '') {
  const v = Math.max(0, Math.min(100, Number(score) || 0));
  const deg = v * 3.6;
  return `<div class="score-ring ${cls}" style="--fill:${deg}deg" title="${v} out of 100"><span>${fmt(v, 0)}</span></div>`;
}

function bboxQuery() {
  return state.bbox ? `&bbox=${state.bbox.join(',')}` : '';
}

function mapQuery() {
  const sub = state.sublayer ? `&sublayer=${encodeURIComponent(state.sublayer)}` : '';
  return `/api/map?layer=${encodeURIComponent(state.layer)}${sub}${bboxQuery()}`;
}

function sourceLinksHtml(sources = []) {
  const uniq = [...new Set(sources || [])];
  if (!uniq.length) return '<span class="muted">—</span>';
  return uniq.slice(0, 4).map((u, i) => `<a href="${u}" target="_blank" rel="noopener">source ${i + 1}</a>`).join('<br/>');
}

function metricLabel(metricKey) {
  const select = document.getElementById('plotMetricSelect');
  const opt = [...select.options].find(o => o.value === metricKey);
  return opt ? opt.textContent : metricKey;
}

function setActiveLayer(layer) {
  state.layer = layer;
  document.querySelectorAll('.layer-btn').forEach(btn => btn.classList.toggle('active', btn.dataset.layer === layer));
  document.getElementById('activeLayerLabel').textContent = layer[0].toUpperCase() + layer.slice(1);
}

function setupTabs() {
  document.querySelectorAll('.tab-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
      document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
      btn.classList.add('active');
      document.querySelector(`.tab-panel[data-tab="${btn.dataset.tab}"]`).classList.add('active');
      if (btn.dataset.tab === 'plots') loadPlot();
      if (btn.dataset.tab === 'data-catalog') loadDataCatalog();
    });
  });
}

function renderLegend(items, trafficAadt) {
  let html = (items || []).map(item => `<span class="legend-item"><i style="background:${item.color}"></i>${item.label}</span>`).join('');
  if (trafficAadt && trafficAadt.length) {
    html += `<div class="legend-aadt"><span class="legend-aadt-title">Road AADT</span>${trafficAadt.map(item => `<span class="legend-item"><i style="background:${item.color}"></i>${item.label}</span>`).join('')}</div>`;
  }
  document.getElementById('mapLegend').innerHTML = html;
}

function ensureMap() {
  if (state.map) return;
  state.map = L.map('map').setView([43.45, -80.44], 11);
  L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    maxZoom: 18,
    attribution: '&copy; OpenStreetMap contributors'
  }).addTo(state.map);

  state.drawLayer = new L.FeatureGroup().addTo(state.map);
  const drawControl = new L.Control.Draw({
    draw: {
      polyline: false,
      polygon: false,
      circle: false,
      marker: false,
      circlemarker: false,
      rectangle: true,
    },
    edit: {
      featureGroup: state.drawLayer,
      edit: false,
      remove: true,
    },
  });
  state.map.addControl(drawControl);

  state.map.on(L.Draw.Event.CREATED, async (event) => {
    state.drawLayer.clearLayers();
    const layer = event.layer;
    state.drawLayer.addLayer(layer);
    const b = layer.getBounds();
    state.bbox = [b.getWest(), b.getSouth(), b.getEast(), b.getNorth()];
    await refreshEverything();
  });
  state.map.on(L.Draw.Event.DELETED, async () => {
    state.bbox = null;
    await refreshEverything();
  });
}

function clearOverlays() {
  Object.values(state.overlays).forEach(layer => {
    try { state.map.removeLayer(layer); } catch (e) {}
  });
  state.overlays = {};
}

function updateSubLayerOptions(options = ['transit'], active = '') {
  const select = document.getElementById('subLayerSelect');
  select.innerHTML = options.map(opt => `<option value="${opt}">${opt.replace(/_/g, ' ')}</option>`).join('');
  const pick = active && options.includes(active) ? active : (options[0] || 'transit');
  state.sublayer = pick;
  select.value = state.sublayer;
}

function heatPopupHtml(props) {
  if (state.layer === 'transportation') {
    return `
      <strong>${props.city}</strong><br/>
      Layer score: ${fmt(props.score)}<br/>
      Transit score: ${fmt(props.transport?.transit_score)}<br/>
      Traffic score: ${fmt(props.transport?.traffic_score)}<br/>
      Stops: ${fmt(props.transport?.stop_density, 0)}<br/>
      Routes: ${fmt(props.transport?.route_density, 0)}<br/>
      AADT: ${fmt(props.transport?.avg_aadt, 0)} vehicles/day
    `;
  }
  if (state.layer === 'healthcare') {
    return `
      <strong>${props.city}</strong><br/>
      Layer score: ${fmt(props.score)}<br/>
      Hospital distance: ${fmt(props.healthcare?.avg_hospital_distance_km)} km<br/>
      Wait time: ${fmt(props.healthcare?.avg_wait_hours)} h<br/>
      Doctors: ${fmt(props.healthcare?.doctors_count, 0)}<br/>
      Dentists: ${fmt(props.healthcare?.dentists_count, 0)}<br/>
      Clinics: ${fmt(props.healthcare?.clinics_count, 0)}
    `;
  }
  if (state.layer === 'housing') {
    return `
      <strong>${props.city}</strong><br/>
      Layer score: ${fmt(props.score)}<br/>
      Nearby offers: ${fmt(props.housing?.offer_count, 0)}<br/>
      Local rent: ${fmt(props.housing?.local_rent_cad, 0)} CAD<br/>
      Vacancy: ${fmt(props.housing?.vacancy_rate_pct)} %<br/>
      Water capacity: ${fmt(props.housing?.water_capacity_pct)} %<br/>
      Home price: ${fmt(props.housing?.home_price_cad, 0)} CAD
    `;
  }
  if (state.layer === 'employment') {
    return `
      <strong>${props.city}</strong><br/>
      Layer score: ${fmt(props.score)}<br/>
      Employment rate: ${fmt(props.employment?.employment_rate_pct)} %<br/>
      Unemployment rate: ${fmt(props.employment?.unemployment_rate_pct)} %<br/>
      Participation rate: ${fmt(props.employment?.participation_rate_pct)} %
    `;
  }
  return `
    <strong>${props.city}</strong><br/>
    Layer score: ${fmt(props.score)}<br/>
    Amenity mix: ${fmt(props.placemaking?.amenity_mix_score)} (${props.placemaking?.amenity_mix_tier || '—'} tier)<br/>
    Amenities: ${fmt(props.placemaking?.amenity_count, 0)}<br/>
    Types: ${fmt(props.placemaking?.amenity_diversity, 0)}<br/>
    Kinds: ${(props.placemaking?.amenity_types || []).join(', ') || '—'}<br/>
    Trails: ${fmt(props.placemaking?.trail_km)} km
  `;
}

async function loadMap() {
  ensureMap();
  const payload = await api(mapQuery());
  updateSubLayerOptions(payload.layer_options || ['transit'], payload.active_sublayer || '');
  const activeSub = state.sublayer || payload.active_sublayer || '';
  const showAadtLegend = state.layer === 'transportation' && activeSub === 'traffic';
  renderLegend(payload.legend || [], showAadtLegend ? (payload.traffic_aadt_legend || []) : []);
  clearOverlays();

  state.overlays.heat = L.geoJSON(payload.heat_cells, {
    style: feature => ({
      color: 'transparent',
      weight: 0,
      fillColor: feature.properties.fillColor,
      fillOpacity: 0.58,
    }),
    onEachFeature: (feature, layer) => {
      layer.bindPopup(heatPopupHtml(feature.properties));
    }
  }).addTo(state.map);

  state.overlays.cities = L.geoJSON(payload.cities, {
    style: () => ({
      color: '#e5eef9',
      weight: 2.1,
      fillOpacity: 0.02,
      fillColor: '#ffffff',
    }),
    onEachFeature: (feature, layer) => {
      const p = feature.properties;
      layer.bindPopup(`
        <strong>${p.city}</strong><br/>
        Housing: ${fmt(p.scores.Housing)}<br/>
        Transportation: ${fmt(p.scores.Transportation)}<br/>
        Healthcare: ${fmt(p.scores.Healthcare)}<br/>
        Employment: ${fmt(p.scores.Employment)}<br/>
        Placemaking: ${fmt(p.scores.Placemaking)}
      `);
    }
  }).addTo(state.map);

  if (payload.selection) {
    state.overlays.selection = L.geoJSON(payload.selection, {
      style: () => ({
        color: '#1e3a8a',
        weight: 3,
        dashArray: '10,8',
        fillColor: '#2563eb',
        fillOpacity: 0.28,
      }),
      onEachFeature: (feature, layer) => {
        layer.bindPopup(`<strong>${feature.properties.label}</strong><br/>Score: ${fmt(feature.properties.score)}`);
      }
    }).addTo(state.map);
  }

  const sub = state.sublayer || payload.active_sublayer || '';

  if (state.layer === 'transportation') {
    const aadtLineStyle = (aadt) => {
      const n = Number(aadt);
      if (!Number.isFinite(n) || n <= 0) return { color: '#94a3b8', weight: 1.2 };
      if (n < 5000) return { color: '#22c55e', weight: Math.max(1.4, Math.min(6, n / 3500)) };
      if (n < 15000) return { color: '#eab308', weight: Math.max(1.6, Math.min(6.5, n / 9000)) };
      if (n < 30000) return { color: '#f97316', weight: Math.max(1.8, Math.min(7, n / 14000)) };
      return { color: '#dc2626', weight: Math.max(2, Math.min(8, n / 12000)) };
    };
    if (sub === 'traffic') {
      state.overlays.traffic = L.geoJSON(payload.overlays.traffic_roads, {
        style: feature => {
          const s = aadtLineStyle(feature.properties.AADT);
          return { color: s.color, weight: s.weight, opacity: 0.88 };
        },
        onEachFeature: (feature, layer) => {
          layer.bindPopup(`<strong>${feature.properties.road_name || 'Road segment'}</strong><br/>AADT: ${fmt(feature.properties.AADT, 0)} vehicles/day`);
        }
      }).addTo(state.map);
    } else {
      state.overlays.lines = L.geoJSON(payload.overlays.transport_lines, {
        style: () => ({ color: '#2563eb', weight: 2.1, opacity: 0.9 })
      }).addTo(state.map);
      state.overlays.stops = L.geoJSON(payload.overlays.transport_stops, {
        pointToLayer: (f, latlng) => L.circleMarker(latlng, {
          radius: 3, color: '#1d4ed8', weight: 1, fillColor: '#60a5fa', fillOpacity: 0.88,
        }),
        onEachFeature: (feature, layer) => {
          layer.bindPopup(`<strong>${feature.properties.stop_name || 'Stop'}</strong><br/>Routes: ${(feature.properties.routes || []).join(', ')}`);
        }
      }).addTo(state.map);
    }
  } else if (state.layer === 'healthcare') {
    if (sub === 'wait_time') {
      state.overlays.hospitals = L.geoJSON(payload.overlays.hospitals, {
        pointToLayer: (f, latlng) => L.circleMarker(latlng, {
          radius: 8, color: '#7f1d1d', weight: 2, fillColor: '#fecaca', fillOpacity: 0.95,
        }),
        onEachFeature: (feature, layer) => layer.bindPopup(
          `<strong>${feature.properties.name}</strong><br/>${feature.properties.address}<br/>Wait: ${fmt(feature.properties.wait_hours, 1)} h`
        )
      }).addTo(state.map);
    } else {
      state.overlays.hospitals = L.geoJSON(payload.overlays.hospitals, {
        pointToLayer: (f, latlng) => L.circleMarker(latlng, {
          radius: 6, color: '#b91c1c', weight: 2, fillColor: '#fca5a5', fillOpacity: 0.9,
        }),
        onEachFeature: (feature, layer) => layer.bindPopup(`<strong>${feature.properties.name}</strong><br/>${feature.properties.address}`)
      }).addTo(state.map);
      state.overlays.healthcare = L.geoJSON(payload.overlays.healthcare_points, {
        pointToLayer: (f, latlng) => L.circleMarker(latlng, {
          radius: 3.5, color: '#7f1d1d', weight: 1, fillColor: '#fecaca', fillOpacity: 0.8,
        }),
        onEachFeature: (feature, layer) => layer.bindPopup(`<strong>${feature.properties.name}</strong><br/>${feature.properties.kind}`)
      }).addTo(state.map);
    }
  } else if (state.layer === 'housing') {
    if (sub === 'offers') {
      state.overlays.offers = L.geoJSON(payload.overlays.housing_offers, {
        pointToLayer: (f, latlng) => L.circleMarker(latlng, {
          radius: 3.5, color: '#14532d', weight: 1, fillColor: '#86efac', fillOpacity: 0.85,
        }),
        onEachFeature: (feature, layer) => layer.bindPopup(`<strong>${feature.properties.name}</strong><br/>Asking rent: ${fmt(feature.properties.asking_rent, 0)} CAD`)
      }).addTo(state.map);
    }
  } else if (state.layer === 'placemaking') {
    if (sub === 'trails') {
      state.overlays.trails = L.geoJSON(payload.overlays.trails, {
        style: () => ({ color: '#16a34a', weight: 2, opacity: 0.9 })
      }).addTo(state.map);
    } else if (sub === 'diversity') {
      state.overlays.trails = L.geoJSON(payload.overlays.trails, {
        style: () => ({ color: '#84cc16', weight: 1.6, opacity: 0.75, dashArray: '6,4' })
      }).addTo(state.map);
      state.overlays.amenities = L.geoJSON(payload.overlays.amenities, {
        pointToLayer: (f, latlng) => L.circleMarker(latlng, {
          radius: 2.8, color: '#c2410c', weight: 1, fillColor: '#fdba74', fillOpacity: 0.9,
        }),
        onEachFeature: (feature, layer) => layer.bindPopup(`<strong>${feature.properties.name}</strong><br/>${feature.properties.kind}`)
      }).addTo(state.map);
    } else {
      state.overlays.amenities = L.geoJSON(payload.overlays.amenities, {
        pointToLayer: (f, latlng) => L.circleMarker(latlng, {
          radius: 3.2, color: '#14532d', weight: 1, fillColor: '#86efac', fillOpacity: 0.85,
        }),
        onEachFeature: (feature, layer) => layer.bindPopup(`<strong>${feature.properties.name}</strong><br/>${feature.properties.kind}`)
      }).addTo(state.map);
    }
  }

  const boundsLayer = state.overlays.selection || state.overlays.cities || state.overlays.heat;
  if (boundsLayer && typeof boundsLayer.getBounds === 'function') {
    const bounds = boundsLayer.getBounds();
    if (bounds && bounds.isValid && bounds.isValid()) state.map.fitBounds(bounds.pad(0.08));
  }
}

function renderCityCards(cards) {
  const target = document.getElementById('cityCards');
  target.innerHTML = '';
  cards.forEach(card => {
    const el = document.createElement('div');
    el.className = 'city-card';
    el.innerHTML = `
      <div class="city-title">${card.city}</div>
      <div class="city-grid city-grid-rings">
        <div class="city-stat"><span>Housing</span>${scoreRingHtml(card.Housing)}</div>
        <div class="city-stat"><span>Transport</span>${scoreRingHtml(card.Transportation)}</div>
        <div class="city-stat"><span>Healthcare</span>${scoreRingHtml(card.Healthcare)}</div>
        <div class="city-stat"><span>Employment</span>${scoreRingHtml(card.Employment)}</div>
        <div class="city-stat"><span>Placemaking</span>${scoreRingHtml(card.Placemaking)}</div>
      </div>
    `;
    target.appendChild(el);
  });
}

function renderScoreCards(summary) {
  const target = document.getElementById('scoreCards');
  target.innerHTML = '';
  (summary.sector_details || []).forEach(sec => {
    const el = document.createElement('details');
    el.className = 'score-detail-card';
    el.innerHTML = `
      <summary>
        <span>${sec.sector}</span>
        ${scoreRingHtml(sec.score)}
      </summary>
      <div class="score-detail-list">
        ${(sec.submetrics || []).map(m => `
          <div class="score-detail-row">
            <span>${m.name}</span>
            <div class="score-detail-metric">
              <strong>${fmt(m.value)} ${m.unit || ''}</strong>
              ${scoreRingHtml(m.score, 'score-ring-tiny')}
            </div>
          </div>
        `).join('')}
      </div>
    `;
    target.appendChild(el);
  });
  const os = Number(summary.overall_score);
  const ring = document.getElementById('overallScoreRing');
  if (ring) {
    ring.style.setProperty('--fill', `${Math.max(0, Math.min(100, os)) * 3.6}deg`);
    const lab = document.getElementById('overallScore');
    if (lab) lab.textContent = fmt(summary.overall_score);
  }
  document.getElementById('heroTitle').textContent = summary.selection_label;
  document.getElementById('selectionMeta').textContent = summary.selection_label;
}

function renderRegionMetrics(metrics) {
  const body = document.getElementById('regionMetrics');
  body.innerHTML = metrics.map(m => `
    <div>
      <span>${m.name}</span>
      <strong>${fmt(m.value)} ${m.unit}</strong>
    </div>
  `).join('') || '<div class="muted">No metrics yet.</div>';

  const table = document.getElementById('metricRows');
  table.innerHTML = '';
  metrics.forEach(metric => {
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td>
        <div class="table-title">${metric.name}</div>
        <div class="muted small">${metric.sector}</div>
      </td>
      <td>${fmt(metric.value)} ${metric.unit}</td>
      <td class="td-ring">${scoreRingHtml(metric.score, 'score-ring-tiny')}</td>
      <td>${sourceLinksHtml(metric.sources)}</td>
    `;
    tr.addEventListener('click', async () => {
      document.getElementById('plotMetricSelect').value = metric.metric_key;
      document.querySelector('.tab-btn[data-tab="plots"]').click();
      await loadPlot();
    });
    table.appendChild(tr);
  });

  const plotSelect = document.getElementById('plotMetricSelect');
  const current = plotSelect.value;
  const combo = '<option value="__labour_market__">Labour market (all series)</option>';
  plotSelect.innerHTML = combo + metrics.map(m => `<option value="${m.metric_key}">${m.name}</option>`).join('');
  if (current && [...plotSelect.options].some(o => o.value === current)) plotSelect.value = current;
  else if (!current) plotSelect.value = 'unemployment_rate_pct';
}

function escHtml(t) {
  return String(t ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/"/g, '&quot;');
}

async function loadDataCatalog() {
  const el = document.getElementById('dataCatalogBody');
  if (!el) return;
  try {
    const d = await api('/api/data-catalog');
    el.classList.remove('muted');

    const freshnessList = Object.entries(d.freshness || {})
      .map(([k, v]) => `<li><code>${escHtml(k)}</code> — ${escHtml(v || '—')}</li>`).join('');

    const metaCards = `
      <div class="data-cat-meta-grid">
        <div class="data-cat-meta-card">
          <div class="meta-label">Metric export</div>
          <div class="meta-val">${escHtml(d.metrics_export_updated_at || '—')}</div>
          <div class="meta-sub">${escHtml(String(d.metric_snapshots_rows ?? '—'))} snapshot rows (from export)</div>
        </div>
        <div class="data-cat-meta-card">
          <div class="meta-label">Source registry export</div>
          <div class="meta-val">${escHtml(d.sources_export_updated_at || '—')}</div>
          <div class="meta-sub">${(d.sources || []).length} registered sources</div>
        </div>
      </div>`;

    const sectionsHtml = (d.sections || []).map((sec) => {
      const hosp = (sec.rows || []).length
        ? `<ul class="data-cat-hosp">${sec.rows.map((r) => `
            <li>
              <div class="hosp-line">
                <strong>${escHtml(r.label)}</strong>
                <span class="hosp-wait">${escHtml(r.value)}</span>
              </div>
              ${r.href ? `<a class="hosp-link" href="${escHtml(r.href)}" target="_blank" rel="noopener">ER Watch / source</a>` : ''}
              ${r.note ? `<span class="hosp-note">${escHtml(r.note)}</span>` : ''}
            </li>`).join('')}</ul>`
        : '';

      const bullets = (sec.bullets || []).length
        ? `<ul class="data-cat-kv">${sec.bullets.map((b) => `
            <li>
              <span class="kv-label">${escHtml(b.label)}</span>
              <span class="kv-val">${escHtml(b.value)}</span>
              ${b.href ? `<a class="kv-src" href="${escHtml(b.href)}" target="_blank" rel="noopener">Open source</a>` : '<span class="kv-src muted">—</span>'}
            </li>`).join('')}</ul>`
        : '';

      return `
        <section class="data-cat-section" id="catalog-${escHtml(sec.id)}">
          <h4>${escHtml(sec.title)}</h4>
          <p class="section-intro">${escHtml(sec.intro || '')}</p>
          ${hosp}
          ${bullets}
        </section>`;
    }).join('');

    const sourcesTable = `
      <div class="table-wrap data-cat-table"><table><thead><tr><th>Sector</th><th>Name</th><th>Frequency</th><th>Last checked</th><th>Link</th></tr></thead><tbody>
      ${(d.sources || []).map((s) => `<tr>
        <td>${escHtml(s.sector)}</td>
        <td>${escHtml(s.name)}</td>
        <td>${escHtml(s.update_frequency || '—')}</td>
        <td>${escHtml(s.last_checked || '—')}</td>
        <td><a href="${escHtml(s.source_url)}" target="_blank" rel="noopener">Open</a></td>
      </tr>`).join('')}
      </tbody></table></div>`;

    el.innerHTML = `
      <div class="data-cat-hero">
        <h4 class="data-cat-title">${escHtml(d.title || 'Data catalog')}</h4>
        <p class="data-cat-lead">${escHtml(d.summary || '')}</p>
        <p class="data-cat-region"><strong>Region:</strong> ${escHtml(d.region || '')}</p>
      </div>
      ${metaCards}
      <h4 class="data-cat-subhead">Cache freshness</h4>
      <p class="muted small">Last modified times for JSON files under <code>cache/</code> (updated when you run live scraping).</p>
      <ul class="data-cat-list">${freshnessList}</ul>
      ${sectionsHtml}
      <h4 class="data-cat-subhead data-cat-registry-head">Full source registry</h4>
      <p class="muted small">Rows come from <code>sources_registry.json</code> (exported with each pipeline run).</p>
      ${sourcesTable}
    `;
  } catch (e) {
    el.textContent = 'Could not load data catalog: ' + e.message;
  }
}

async function loadDashboard() {
  const payload = await api(`/api/dashboard?layer=${state.layer}${bboxQuery()}`);
  state.dashboard = payload;
  renderCityCards(payload.city_cards || []);
  renderScoreCards(payload.summary || {});
  renderRegionMetrics(payload.metrics || []);
  renderAlerts(payload.alerts || []);
}

function renderAlerts(alerts) {
  const target = document.getElementById('alertsList');
  target.innerHTML = '';
  alerts.forEach(alert => {
    const el = document.createElement('div');
    el.className = `alert-card ${alert.severity}`;
    el.innerHTML = `
      <div class="alert-head">
        <div>
          <strong>${alert.title}</strong>
          <div class="muted small">${alert.category} · ${new Date(alert.created_at).toLocaleString()}</div>
        </div>
        <span class="pill ${alert.severity}">${alert.severity}</span>
      </div>
      <div class="alert-body">${alert.message}</div>
      ${alert.source_url ? `<a href="${alert.source_url}" target="_blank" rel="noopener">Related source</a>` : ''}
    `;
    target.appendChild(el);
  });
}

async function loadFormulas() {
  const formulas = await api('/api/formulas');
  state.formulas = formulas;
  const list = document.getElementById('formulaList');
  list.innerHTML = '';
  formulas.forEach(formula => {
    const card = document.createElement('div');
    card.className = 'formula-card';
    const vars = (formula.variables || []).map(v => `<button class="token-btn token-var" draggable="true" data-token="${v.name}">${v.name}</button>`).join('');
    const ops = ['(', ')', '+', '-', '*', '/', 'clamp(', ',', '100*('].map(op => `<button class="token-btn" draggable="true" data-token="${op}">${op}</button>`).join('');
    card.innerHTML = `
      <div class="formula-head">
        <div>
          <h4>${formula.title}</h4>
          <div class="muted small">${formula.scope}</div>
        </div>
      </div>
      <div class="token-bar">${vars}${ops}</div>
      <div class="builder-wrap">
        <textarea class="formula-text" data-key="${formula.formula_key}">${formula.expression}</textarea>
      </div>
      <div class="muted formula-note">${formula.notes || ''}</div>
      <div class="formula-var-list">${(formula.variables || []).map(v => `<div><strong>${v.name}</strong>: ${v.description}</div>`).join('')}</div>
      <button class="primary small save-formula-btn" data-save="${formula.formula_key}">Save formula</button>
    `;
    list.appendChild(card);
  });

  list.querySelectorAll('.token-btn').forEach(btn => {
    btn.addEventListener('dragstart', (e) => e.dataTransfer.setData('text/plain', btn.dataset.token));
    btn.addEventListener('click', () => {
      const textarea = btn.closest('.formula-card').querySelector('.formula-text');
      insertAtCursor(textarea, btn.dataset.token);
    });
  });
  list.querySelectorAll('.formula-text').forEach(textarea => {
    textarea.addEventListener('dragover', e => e.preventDefault());
    textarea.addEventListener('drop', (e) => {
      e.preventDefault();
      insertAtCursor(textarea, e.dataTransfer.getData('text/plain'));
    });
  });
  list.querySelectorAll('.save-formula-btn').forEach(btn => {
    btn.addEventListener('click', async () => {
      const textarea = btn.closest('.formula-card').querySelector('.formula-text');
      await api(`/api/formulas/${btn.dataset.save}`, {
        method: 'PATCH', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ expression: textarea.value })
      });
      alert('Formula saved. Run live scraping again to recompute cached scores.');
    });
  });
}

function insertAtCursor(textarea, text) {
  const start = textarea.selectionStart || 0;
  const end = textarea.selectionEnd || 0;
  textarea.value = textarea.value.slice(0, start) + text + textarea.value.slice(end);
  textarea.focus();
  textarea.selectionStart = textarea.selectionEnd = start + text.length;
}

async function loadSources() {
  const sources = await api('/api/sources');
  const body = document.getElementById('sourceRows');
  body.innerHTML = '';
  sources.forEach(src => {
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td>
        <div class="table-title"><a href="${src.source_url}" target="_blank" rel="noopener">${src.name}</a></div>
        <div class="preview-badge" title="${(src.preview_text || src.notes || '').replace(/"/g, '&quot;')}">preview</div>
      </td>
      <td>${src.sector}</td>
      <td>${src.access_method}</td>
      <td>
        <select data-freq="${src.id}">
          ${['hourly','daily','weekly','monthly','quarterly','annual'].map(v => `<option ${src.update_frequency === v ? 'selected' : ''}>${v}</option>`).join('')}
        </select>
      </td>
      <td><input type="checkbox" data-llm="${src.id}" ${src.use_llm ? 'checked' : ''}></td>
      <td><input type="checkbox" data-active="${src.id}" ${src.active ? 'checked' : ''}></td>
      <td>${src.status || '—'}</td>
      <td><button class="ghost small" data-del="${src.id}">Delete</button></td>
    `;
    body.appendChild(tr);
  });
  body.querySelectorAll('[data-freq]').forEach(el => el.addEventListener('change', async (e) => {
    await api(`/api/sources/${e.target.dataset.freq}`, { method: 'PATCH', headers: {'Content-Type':'application/json'}, body: JSON.stringify({ update_frequency: e.target.value }) });
  }));
  body.querySelectorAll('[data-llm]').forEach(el => el.addEventListener('change', async (e) => {
    await api(`/api/sources/${e.target.dataset.llm}`, { method: 'PATCH', headers: {'Content-Type':'application/json'}, body: JSON.stringify({ use_llm: e.target.checked ? 1 : 0 }) });
  }));
  body.querySelectorAll('[data-active]').forEach(el => el.addEventListener('change', async (e) => {
    await api(`/api/sources/${e.target.dataset.active}`, { method: 'PATCH', headers: {'Content-Type':'application/json'}, body: JSON.stringify({ active: e.target.checked ? 1 : 0 }) });
  }));
  body.querySelectorAll('[data-del]').forEach(el => el.addEventListener('click', async (e) => {
    if (!confirm('Delete this source?')) return;
    await api(`/api/sources/${e.target.dataset.del}`, { method: 'DELETE' });
    await loadSources();
    await loadDocs();
  }));
}

async function loadLLM() {
  const rows = await api('/api/llm_extractions');
  const target = document.getElementById('llmList');
  target.innerHTML = '';
  rows.forEach(item => {
    let parsed = {};
    try { parsed = JSON.parse(item.extracted_json || '{}'); } catch (e) {}
    const metrics = (parsed.metrics || []).slice(0, 4).map(m => `<li>${m.metric_key}: ${fmt(m.value)} ${m.unit || ''}</li>`).join('');
    const el = document.createElement('div');
    el.className = 'llm-card';
    el.innerHTML = `
      <div class="table-title"><a href="${item.source_url}" target="_blank" rel="noopener">${item.source_name}</a></div>
      <div class="muted small">${item.status} · ${new Date(item.created_at).toLocaleString()}</div>
      <div class="muted">${parsed.summary || 'No summary.'}</div>
      <ul>${metrics || '<li class="muted">No extracted metrics</li>'}</ul>
    `;
    target.appendChild(el);
  });
}

async function loadDocs() {
  const docs = await api('/api/docs');
  const target = document.getElementById('docsList');
  target.innerHTML = '';
  docs.forEach(doc => {
    const el = document.createElement('div');
    el.className = 'doc-card';
    el.innerHTML = `
      <div class="doc-head">
        <div>
          <h4><a href="${doc.url}" target="_blank" rel="noopener">${doc.name}</a></h4>
          <div class="muted small">${doc.sector} · ${doc.access_method} · ${doc.frequency}</div>
        </div>
        <span class="pill ${doc.active ? 'success' : 'muted'}">${doc.active ? 'enabled' : 'disabled'}</span>
      </div>
      <div class="doc-body">${doc.notes || ''}</div>
      <details>
        <summary>Preview</summary>
        <div class="doc-preview">${doc.preview_text || 'No preview cached yet. Run live scraping to fetch one.'}</div>
      </details>
    `;
    target.appendChild(el);
  });
}

async function renderWeightEditor() {
  const payload = await api('/api/weight_meta');
  const target = document.getElementById('weightEditor');
  target.innerHTML = '';
  payload.sectors.forEach(sec => {
    const block = document.createElement('div');
    block.className = 'weight-block';
    block.innerHTML = `
      <div class="weight-heading">${sec.sector}</div>
      <label class="weight-row"><span>Sector weight</span><input type="number" step="0.1" value="${sec.weight}" data-sector="${sec.sector}" /></label>
      ${payload.metrics.filter(m => m.sector === sec.sector).map(m => `
        <label class="weight-row"><span>${m.name}</span><input type="number" step="0.1" value="${m.weight}" data-metric="${m.metric_key}" /></label>
      `).join('')}
    `;
    target.appendChild(block);
  });
  target.querySelectorAll('[data-sector]').forEach(input => input.addEventListener('change', async (e) => {
    await api(`/api/weights/sector/${encodeURIComponent(e.target.dataset.sector)}`, {
      method: 'PATCH', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ weight: Number(e.target.value) })
    });
  }));
  target.querySelectorAll('[data-metric]').forEach(input => input.addEventListener('change', async (e) => {
    await api(`/api/weights/metric/${encodeURIComponent(e.target.dataset.metric)}`, {
      method: 'PATCH', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ weight: Number(e.target.value) })
    });
  }));
}

async function loadPlot() {
  const metricKey = document.getElementById('plotMetricSelect').value || 'unemployment_rate_pct';
  const regionSel = document.getElementById('plotRegionSelect').value;
  const start = document.getElementById('plotStart').value;
  const end = document.getElementById('plotEnd').value;
  let region_scope = 'whole';
  let region_id = '';
  if (regionSel.startsWith('city:')) {
    region_scope = 'city';
    region_id = regionSel.split(':')[1];
  }
  const q = `region_scope=${region_scope}${region_id ? `&region_id=${encodeURIComponent(region_id)}` : ''}${start ? `&start=${start}` : ''}${end ? `&end=${end}` : ''}`;
  const ctx = document.getElementById('mainChart').getContext('2d');
  if (state.chart) state.chart.destroy();

  if (metricKey === '__labour_market__') {
    const keys = [
      { key: 'unemployment_rate_pct', label: 'Unemployment %', color: '#f97316' },
      { key: 'employment_rate_pct', label: 'Employment %', color: '#22c55e' },
      { key: 'labour_participation_pct', label: 'Participation %', color: '#60a5fa' },
    ];
    const results = await Promise.all(keys.map(k => api(`/api/plots?metric_key=${encodeURIComponent(k.key)}&${q}`)));
    const anyFallback = results.some(p => p.fallback_whole);
    document.getElementById('plotFallbackNote').textContent = anyFallback ? 'City-specific history was unavailable for some periods, so the chart is using whole-region history where needed.' : '';
    const lens = results.map(r => (r.points || []).length);
    const n = lens.length ? Math.min(...lens) : 0;
    const labels = n ? results[0].points.slice(0, n).map(p => new Date(p.observed_at).toLocaleDateString()) : [];
    const datasets = keys.map((k, i) => ({
      label: k.label,
      data: (results[i].points || []).slice(0, n).map(p => p.value),
      borderColor: k.color,
      backgroundColor: 'transparent',
      borderWidth: 2.4,
      tension: 0.22,
      pointRadius: 2.2,
    }));
    state.chart = new Chart(ctx, {
      type: 'line',
      data: { labels, datasets },
      options: {
        maintainAspectRatio: false,
        interaction: { mode: 'index', intersect: false },
        plugins: {
          legend: { labels: { color: '#e5eef9', font: { size: 13, weight: '600' } } }
        },
        scales: {
          x: { ticks: { color: '#c9d7ec', maxRotation: 30, minRotation: 0, font: { size: 12 } }, grid: { color: 'rgba(255,255,255,0.06)' } },
          y: { ticks: { color: '#c9d7ec', font: { size: 12 } }, grid: { color: 'rgba(255,255,255,0.06)' } },
        }
      }
    });
    return;
  }

  const payload = await api(`/api/plots?metric_key=${encodeURIComponent(metricKey)}&${q}`);
  document.getElementById('plotFallbackNote').textContent = payload.fallback_whole ? 'City-specific history was unavailable for some periods, so the chart is using whole-region history where needed.' : '';
  const labels = payload.points.map(p => new Date(p.observed_at).toLocaleDateString());
  const values = payload.points.map(p => p.value);
  state.chart = new Chart(ctx, {
    type: 'line',
    data: {
      labels,
      datasets: [{
        label: metricLabel(metricKey),
        data: values,
        borderWidth: 3,
        tension: 0.22,
        pointRadius: 2.6,
      }]
    },
    options: {
      maintainAspectRatio: false,
      plugins: {
        legend: { labels: { color: '#e5eef9', font: { size: 14, weight: '600' } } }
      },
      scales: {
        x: { ticks: { color: '#c9d7ec', maxRotation: 30, minRotation: 0, font: { size: 12 } }, grid: { color: 'rgba(255,255,255,0.06)' } },
        y: { ticks: { color: '#c9d7ec', font: { size: 12 } }, grid: { color: 'rgba(255,255,255,0.06)' } },
      }
    }
  });
}

async function pollStatus() {
  try {
    const status = await api('/api/run_status');
    document.getElementById('runStatusText').textContent = status.status || 'idle';
  } catch (e) {}
}

async function loadLogs() {
  try {
    const logs = await api('/api/logs');
    document.getElementById('logWindow').innerHTML = logs.slice(0, 50).reverse().map(log => `
      <div class="log-row ${log.level}">
        <span>${new Date(log.created_at).toLocaleTimeString()}</span>
        <strong>${log.level}</strong>
        <div>${log.message}</div>
      </div>
    `).join('');
  } catch (e) {}
}

async function refreshEverything() {
  await Promise.all([loadDashboard(), loadMap(), loadLLM(), loadDocs(), loadLogs(), pollStatus()]);
}

function wireControls() {
  document.querySelectorAll('.layer-btn').forEach(btn => {
    btn.addEventListener('click', async () => {
      state.sublayer = '';
      setActiveLayer(btn.dataset.layer);
      await loadMap();
      await loadDashboard();
    });
  });

  document.getElementById('subLayerSelect').addEventListener('change', async (e) => {
    state.sublayer = e.target.value;
    await loadMap();
  });

  document.getElementById('runBtn').addEventListener('click', async () => {
    await api('/api/run', { method: 'POST' });
    await pollStatus();
  });

  document.getElementById('clearSelectionBtn').addEventListener('click', async () => {
    state.bbox = null;
    if (state.drawLayer) state.drawLayer.clearLayers();
    await refreshEverything();
  });

  document.getElementById('plotRefreshBtn').addEventListener('click', loadPlot);

  document.getElementById('sourceForm').addEventListener('submit', async (e) => {
    e.preventDefault();
    const formData = new FormData(e.target);
    const data = Object.fromEntries(formData.entries());
    data.use_llm = formData.get('use_llm') ? 1 : 0;
    await api('/api/sources', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(data),
    });
    e.target.reset();
    await loadSources();
    await loadDocs();
  });
}

async function init() {
  setupTabs();
  wireControls();
  await renderWeightEditor();
  setActiveLayer('transportation');
  await Promise.all([loadDashboard(), loadMap(), loadFormulas(), loadSources(), loadLLM(), loadDocs(), loadLogs(), pollStatus()]);
  setInterval(async () => {
    await Promise.all([pollStatus(), loadLogs()]);
  }, 4000);
}

init().catch(err => {
  console.error(err);
  alert(`App failed to initialize: ${err.message}`);
});
