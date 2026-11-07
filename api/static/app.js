/* LifeMap E1. Consultas acotadas y de solo lectura sobre los contratos existentes. */
(() => {
  'use strict';
  const $ = id => document.getElementById(id);
  const fmt = new Intl.NumberFormat('es-CO');
  const decimal = new Intl.NumberFormat('es-CO', {maximumFractionDigits: 2});
  const num = value => Number.isFinite(value) ? fmt.format(value) : '—';
  const node = (tag, value, cls) => {
    const el = document.createElement(tag);
    if (value !== undefined) el.textContent = String(value ?? '');
    if (cls) el.className = cls;
    return el;
  };
  const message = (id, text, error = false) => {
    $(id).textContent = text; $(id).className = `message${error ? ' error' : ''}`;
  };
  function friendly(error) {
    if (error.name === 'AbortError') return 'La consulta tardó demasiado. Intenta de nuevo o reduce el radio.';
    if (error.status === 400) return 'Revisa los valores de la búsqueda e inténtalo de nuevo.';
    if (error.status === 503) return 'El servicio no está disponible temporalmente. Intenta de nuevo.';
    return 'No se pudo completar la consulta. Revisa tu conexión e inténtalo de nuevo.';
  }
  async function api(path, options = {}, parentSignal) {
    const controller = new AbortController();
    const cancel = () => controller.abort();
    if (parentSignal) {
      if (parentSignal.aborted) cancel();
      parentSignal.addEventListener('abort', cancel, {once: true});
    }
    const timer = setTimeout(cancel, 15000);
    try {
      const response = await fetch(path, {...options, signal: controller.signal, headers: {Accept: 'application/json', ...options.headers}});
      if (!response.ok) throw Object.assign(new Error('API unavailable'), {status: response.status});
      return await response.json();
    } finally {
      clearTimeout(timer);
      if (parentSignal) parentSignal.removeEventListener('abort', cancel);
    }
  }
  const state = {requestId: 0, controller: null, rows: [], markers: [], buttons: [], selected: null, statsId: 0};
  let map, markers, selection, centerMarker, radiusCircle;
  if (typeof L !== 'undefined') {
    map = L.map('map', {zoomControl: false, minZoom: 7, maxZoom: 19}).setView([39.9526, -75.1652], 13);
    L.control.zoom({position: 'topright', zoomInTitle: 'Acercar', zoomOutTitle: 'Alejar'}).addTo(map);
    const tiles = L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png', {maxZoom: 19, updateWhenIdle: true, keepBuffer: 1, attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'}).addTo(map);
    tiles.on('tileerror', () => { $('tile-message').hidden = false; });
    tiles.on('tileload', () => { $('tile-message').hidden = true; });
    markers = L.featureGroup().addTo(map); selection = L.featureGroup().addTo(map);
    map.on('click', event => { setLocation(event.latlng.lat, event.latlng.lng); $('search-form').requestSubmit(); });
    new ResizeObserver(() => map.invalidateSize({pan: false})).observe($('map'));
  } else {
    $('tile-message').textContent = 'El mapa no pudo cargarse. Puedes consultar los negocios con los filtros.';
    $('tile-message').hidden = false;
  }
  function setLocation(lat, lon) {
    $('latitude').value = Number(lat).toFixed(6); $('longitude').value = Number(lon).toFixed(6);
    $('location').value = 'custom';
  }
  function color(business) { return business.is_open === 1 ? '#428365' : '#b87957'; }
  function showBusiness(index, pan = true) {
    const business = state.rows[index]; if (!business) return;
    state.selected = index;
    state.buttons.forEach((button, i) => { button.classList.toggle('selected', i === index); button.setAttribute('aria-pressed', String(i === index)); });
    state.markers.forEach((marker, i) => marker.setStyle({radius: i === index ? 10 : 7, weight: i === index ? 3 : 2, fillOpacity: i === index ? 1 : 0.85}));
    const detail = $('business-detail'); detail.replaceChildren();
    detail.append(node('span', 'EL NEGOCIO SELECCIONADO', 'detail-eyebrow'), node('h3', business.name));
    detail.append(node('p', [business.address, business.city, business.state].filter(Boolean).join(' · '), 'address'));
    detail.append(node('span', business.is_open === 1 ? '● Abierto en la fuente' : '● Cerrado en la fuente', `status-badge${business.is_open === 1 ? '' : ' closed'}`));
    const metrics = node('div', undefined, 'detail-metrics');
    const rating = node('div'); rating.append(node('strong', Number.isFinite(business.stars) ? `${business.stars.toFixed(1)} ★` : '—'), node('small', 'Calificación Yelp'));
    const reviews = node('div'); reviews.append(node('strong', num(business.review_count_observed)), node('small', 'Reseñas analizadas'));
    metrics.append(rating, reviews); detail.append(metrics);
    const tags = node('div', undefined, 'detail-tags');
    (business.categories || []).slice(0, 3).forEach(category => tags.append(node('span', category)));
    detail.append(tags, node('p', 'Estado histórico de Yelp, no una confirmación de apertura actual.', 'historical-note'));
    if (map && state.markers[index]) {
      state.markers[index].bringToFront().openTooltip();
      if (pan) map.panTo(state.markers[index].getLatLng(), {animate: false});
    }
  }
  function renderResults(data, lat, lon, radius) {
    state.rows = data.businesses; state.buttons = []; state.markers = []; state.selected = null;
    $('result-count').textContent = num(data.count);
    $('results-note').textContent = data.count ? `${num(data.count)} mostrados · máximo ${data.limit} por búsqueda. Selecciona un negocio.` : 'Prueba otro punto, un radio mayor o una categoría diferente.';
    const list = $('results-list'); list.replaceChildren();
    $('business-detail').replaceChildren(node('p', 'Selecciona un negocio para ver su información.', 'empty-state'));
    if (markers) {
      markers.clearLayers(); selection.clearLayers();
      radiusCircle = L.circle([lat, lon], {radius, color: '#65835a', weight: 1.3, dashArray: '5 5', fillColor: '#adbf8a', fillOpacity: .07, interactive: false}).addTo(selection);
      centerMarker = L.marker([lat, lon], {icon: L.divIcon({className: 'search-center', iconSize: [14,14], iconAnchor: [7,7]}), interactive: false}).addTo(selection);
      if (!data.count) map.fitBounds(radiusCircle.getBounds(), {padding: [25,25], maxZoom: 16, animate: false});
    }
    if (!data.count) list.append(node('p', 'No encontramos negocios con esos filtros.', 'empty-state'));
    data.businesses.forEach((business, i) => {
      const button = node('button', undefined, 'business-row'); button.type = 'button'; button.setAttribute('aria-pressed', 'false');
      const copy = node('span', undefined, 'business-copy'); copy.append(node('strong', business.name), node('small', `${business.category_primary || 'Restaurants'} · ${business.is_open === 1 ? 'Abierto' : 'Cerrado'}`));
      const rating = node('span', undefined, 'business-rating'); rating.append(node('span', '★ '), node('span', Number.isFinite(business.stars) ? business.stars.toFixed(1) : '—'));
      button.append(node('span', '⌖', 'business-icon'), copy, rating); button.addEventListener('click', () => showBusiness(i)); list.append(button); state.buttons.push(button);
      if (markers) {
        const coordinates = business.location.coordinates;
        const marker = L.circleMarker([coordinates[1], coordinates[0]], {radius: 7, color: '#fff', weight: 2, fillColor: color(business), fillOpacity: .85, bubblingMouseEvents: false}).addTo(markers);
        marker.bindTooltip(node('span', business.name), {direction: 'top', offset: [0,-8]}); marker.on('click', () => showBusiness(i, false));
        const point = marker.getElement();
        if (point) {
          point.setAttribute('aria-label', business.name); point.setAttribute('role', 'button'); point.setAttribute('tabindex', '0');
          point.addEventListener('keydown', event => { if (event.key === 'Enter' || event.key === ' ') { event.preventDefault(); showBusiness(i, false); } });
        }
        state.markers.push(marker);
      }
    });
    if (data.count) {
      if (markers) map.fitBounds(markers.getBounds().extend([lat, lon]), {padding: [35,35], maxZoom: 17, animate: false});
      showBusiness(0, false);
    }
  }
  $('search-form').addEventListener('submit', async event => {
    event.preventDefault(); if (!$('search-form').reportValidity()) return;
    if (state.controller) state.controller.abort();
    const id = ++state.requestId; const controller = state.controller = new AbortController();
    const lat = Number($('latitude').value), lon = Number($('longitude').value), radius = Number($('radius').value);
    const params = new URLSearchParams({lat, lon, radius_m: radius, limit: $('limit').value});
    const category = $('category').value.trim(); if (category) params.set('category', category);
    $('search-button').disabled = true; message('search-message', 'Buscando negocios reales…'); $('results-list').setAttribute('aria-busy','true');
    try {
      const data = await api(`/businesses/near?${params}`, {}, controller.signal);
      if (id !== state.requestId) return;
      renderResults(data, lat, lon, radius);
      message('search-message', `${num(data.count)} negocios mostrados en un radio de ${decimal.format(radius / 1000)} km.`);
    } catch (error) {
      if (id !== state.requestId) return;
      message('search-message', friendly(error), true);
      $('results-note').textContent = 'No se pudo actualizar. Se conservan los últimos resultados válidos.';
    } finally {
      if (id === state.requestId) { $('search-button').disabled = false; $('results-list').setAttribute('aria-busy','false'); }
    }
  });
  $('location').addEventListener('change', () => {
    if ($('location').value === 'custom') return;
    const [lat, lon] = $('location').value.split(',').map(Number); $('latitude').value = lat; $('longitude').value = lon; $('search-form').requestSubmit();
  });
  for (const field of ['latitude','longitude']) $(field).addEventListener('input', () => { $('location').value = 'custom'; });
  $('reset-map').addEventListener('click', () => { if (map) map.setView([39.9526,-75.1652],12); });
  $('search-here').addEventListener('click', () => { if (map) { const point = map.getCenter(); setLocation(point.lat, point.lng); $('search-form').requestSubmit(); } });
  async function loadClosure(group) {
    const id = ++state.statsId;
    document.querySelectorAll('[data-group]').forEach(button => button.setAttribute('aria-pressed', String(button.dataset.group === group)));
    message('stats-message','Consultando las tasas…');
    try {
      const data = await api(`/stats/closure?group_by=${group}&min_business_days=3650&limit=6`);
      if (id !== state.statsId) return;
      const chart = $('closure-chart'); chart.replaceChildren();
      const maximum = Math.max(1, ...data.results.map(row => row.rate_pct || 0));
      data.results.forEach(row => {
        const bar = node('div', undefined, 'chart-row'); const label = node('div', undefined, 'chart-label');
        label.append(node('span', group === 'geohash' ? `Zona ${row.key}` : row.key), node('strong', row.rate_pct === null ? 'Sin exposición suficiente' : `${decimal.format(row.rate_pct)} % / año`));
        const track = node('div', undefined, 'chart-track'); const fill = node('div', undefined, 'chart-fill'); fill.style.width = `${Math.max(0, (row.rate_pct || 0) / maximum * 100)}%`; track.append(fill);
        bar.append(label, track); bar.title = `${num(row.closures)} cierres · ${num(row.businesses)} negocios · ${decimal.format(row.business_days)} días-negocio`; chart.append(bar);
      });
      message('stats-message', data.count ? '' : 'No hay grupos con exposición suficiente.');
    } catch (error) {
      if (id === state.statsId) {
        $('closure-chart').replaceChildren(node('p', 'No se pudieron cargar estas tasas. Pulsa la opción para reintentar.', 'empty-state'));
        message('stats-message', friendly(error), true);
      }
    }
  }
  document.querySelectorAll('[data-group]').forEach(button => button.addEventListener('click', () => loadClosure(button.dataset.group)));
  $('review-text').addEventListener('input', () => { $('char-count').textContent = `${fmt.format($('review-text').value.length)} / 10.000`; });
  $('predict-form').addEventListener('submit', async event => {
    event.preventDefault(); const review = $('review-text').value.trim();
    if (!review) { message('predict-message','Escribe una reseña antes de clasificar.',true); $('review-text').focus(); return; }
    $('predict-button').disabled = true; message('predict-message','Analizando el texto…');
    $('prediction-result').replaceChildren(node('p', 'El modelo está analizando la reseña…', 'prediction-placeholder'));
    try {
      const result = await api('/predict', {method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify({text: review})});
      const names = {negative: 'Negativa', neutral: 'Neutral', positive: 'Positiva'};
      const output = $('prediction-result'); output.replaceChildren();
      const heading = node('div', undefined, `prediction-heading ${result.sentiment}`); heading.append(node('strong', names[result.sentiment] || 'Sin clasificación'), node('small', 'Probabilidades del modelo')); output.append(heading);
      for (const label of ['negative','neutral','positive']) {
        const probability = Math.max(0, Math.min(1, result.probabilities[label]));
        const row = node('div', undefined, 'probability-row'); const track = node('div', undefined, 'chart-track'); const fill = node('div', undefined, 'chart-fill'); fill.style.width = `${probability*100}%`; track.append(fill);
        row.append(node('span', names[label]), track, node('span', `${decimal.format(probability*100)} %`)); output.append(row);
      }
      message('predict-message','Clasificación completada con el modelo entrenado.');
    } catch (error) {
      $('prediction-result').replaceChildren(node('p', 'No hay una clasificación para este texto.', 'prediction-placeholder'));
      message('predict-message',friendly(error),true);
    }
    finally { $('predict-button').disabled = false; }
  });
  api('/health').then(data => { $('service-status').textContent = data.status === 'ok' ? 'Servicio disponible' : 'Servicio con incidencias'; $('service-dot').className = data.status === 'ok' ? 'ready' : ''; }).catch(() => { $('service-status').textContent = 'Estado no disponible'; });
  $('search-form').requestSubmit(); loadClosure('category');
})();
