/* E2: consulta de resultados existentes; sin entrenamiento ni simulación desde la UI. */
(() => {
  'use strict';
  const $ = id => document.getElementById(id), {map, api, node} = window.LifeMap;
  const f = new Intl.NumberFormat('es-CO', {maximumFractionDigits: 2});
  const pct = x => `${f.format(100 * x)} %`;
  const layers = map ? L.featureGroup().addTo(map) : null;
  let monitorId = 0, survivalId = 0, businessId = 0;
  const label = (id, text, error = false) => { $(id).textContent = text; $(id).className = `message${error ? ' error' : ''}`; };
  async function all(path, field) {
    let rows = [], offset = 0, last;
    do {
      last = await api(`${path}&limit=500&offset=${offset}`);
      rows.push(...last[field]); offset += 500;
      if (offset >= 10000 && last.has_more) throw new Error('Reduce el radio o el período: demasiados resultados.');
    } while (last.has_more);
    return {rows, last};
  }
  async function monitor(event) {
    if (event) event.preventDefault();
    if (!$('monitor-form').reportValidity()) return;
    const id = ++monitorId;
    const params = new URLSearchParams({lat: $('latitude').value, lon: $('longitude').value,
      radius_m: $('monitor-radius').value, from: $('event-from').value, to: $('event-to').value});
    $('monitor-refresh').disabled = true; label('monitor-message', 'Consultando ventanas y alertas…');
    try {
      const [hot, alerts, health] = await Promise.all([
        all(`/live/hotspots?${params}`, 'hotspots'),
        all(`/alerts?${params}&active=true&min_score=${encodeURIComponent($('minimum-score').value)}`, 'results'),
        api('/health').catch(() => null)]);
      if (id !== monitorId) return;
      if (layers) layers.clearLayers();
      $('hotspot-list').replaceChildren(); $('alert-list').replaceChildren();
      $('hotspot-summary').textContent = `${hot.rows.length} hotspots y ${hot.last.total_windows} ventanas finalizadas en el área/período. Colores naranjas: incremento de negatividad frente a la ventana anterior.`;
      if (!hot.rows.length) $('hotspot-list').append(node('p', 'No hay hotspots observados con estos filtros. No se muestran señales sintéticas.', 'empty-state'));
      hot.rows.forEach(row => {
        const delta = row.change_percentage_points;
        const text = `${row.geohash} · ${String(row.start).slice(0,10)} · ${f.format(row.negative_percentage)} % negativas · diferencia ${Number.isFinite(delta) ? f.format(delta) + ' pp' : 'sin ventana previa'}`;
        const button = node('button', text, 'e2-row'); button.type = 'button';
        if (layers) {
          const layer = L.geoJSON(row.geometry, {style: {color: '#bc632c', weight: 2, fillOpacity: .35}}).addTo(layers);
          layer.bindTooltip(node('span', text)); button.onclick = () => map.fitBounds(layer.getBounds());
        }
        $('hotspot-list').append(button);
      });
      if (!alerts.rows.length) $('alert-list').append(node('p', 'No hay alertas activas con estos filtros.', 'empty-state'));
      alerts.rows.forEach(row => {
        const text = `${row.business_id} · score ${f.format(row.risk_score)} · ${String(row.event_date).slice(0,10)}`;
        const button = node('button', text, 'e2-row'); button.type = 'button';
        if (layers) {
          const marker = L.circleMarker([row.latitude,row.longitude], {radius: 6, color: '#8a426a', fillColor: '#ba7398', fillOpacity: .6, bubblingMouseEvents: false}).addTo(layers);
          marker.bindTooltip(node('span', text));
          button.onclick = () => { map.setView(marker.getLatLng(), 15); marker.openTooltip(); $('map').scrollIntoView({block: 'center'}); };
        }
        $('alert-list').append(button);
      });
      const stream = health?.services?.streaming;
      $('stream-status').textContent = stream ? `Kafka ${health.services.kafka.status} · retraso del consumidor: ${stream.consumer_lag_messages} mensajes · watermark: ${stream.watermark_event_time || 'sin dato'}. Las fechas son históricas, no retraso de recepción.` : 'No se pudo comprobar el estado del streaming.';
      label('monitor-message', `${alerts.rows.length} alertas activas · consulta ${new Date(hot.last.generated_at).toLocaleTimeString('es-CO')}. Actualización manual con el botón.`);
    } catch (error) { if (id === monitorId) label('monitor-message', 'No se pudieron actualizar las señales. Revisa fechas, radio y disponibilidad.', true); }
    finally { if (id === monitorId) $('monitor-refresh').disabled = false; }
  }
  window.addEventListener('lifemap:business', async event => {
    const b = event.detail, id = ++businessId;
    const target = node('p', 'Consultando riesgo observado…', 'method-note'); target.id = 'business-risk'; $('business-detail').append(target);
    try {
      const [lon,lat] = b.location.coordinates;
      const data = await api(`/risk/near?${new URLSearchParams({lat,lon,radius_m:100,business_id:b.business_id})}`);
      if (id !== businessId || !target.isConnected) return;
      const r = data.results[0];
      target.textContent = r ? `Score de riesgo: ${f.format(r.risk_score)} (no calibrado). Corte simulado: ${String(r.as_of).slice(0,10)}. Modelo: ${r.model_id || r.model_version}. Variables: ${Object.entries(r.features || {}).map(([k,v]) => `${k}=${typeof v === 'number' ? f.format(v) : v}`).join(' · ')}` : 'Este negocio no tiene aún un score producido por el streaming. No se infiere cero riesgo.';
    } catch { if (target.isConnected) target.textContent = 'No se pudo consultar el score de riesgo.'; }
  });
  const svgNode = (tag, attrs) => { const el = document.createElementNS('http://www.w3.org/2000/svg', tag); Object.entries(attrs).forEach(([k,v]) => el.setAttribute(k,v)); return el; };
  function chart(groups) {
    $('survival-point').textContent = 'Pasa el cursor o enfoca un punto para consultar el número en riesgo.';
    const svg = svgNode('svg', {viewBox: '0 0 800 310', role: 'img', 'aria-label': 'Supervivencia por edad observada en días'});
    const colors = ['#285442','#a4527a','#b87930'];
    const max = Math.max(1, ...groups.flatMap(g => (g.curve.points || []).map(p => p.time_days)));
    const x = t => 55 + 720 * t / max, y = s => 260 - 230 * s;
    [0,.25,.5,.75,1].forEach(v => {
      svg.append(svgNode('line', {x1:55,x2:775,y1:y(v),y2:y(v),stroke:'#e3e7de'}));
      const text = svgNode('text', {x: 45,y:y(v)+4,'text-anchor':'end'}); text.textContent=pct(v); svg.append(text);
    });
    for (let i=0;i<=4;i++) { const t=max*i/4, text=svgNode('text',{x:x(t),y:283,'text-anchor':'middle'}); text.textContent=f.format(t); svg.append(text); }
    const title = svgNode('text',{x:415,y:305,'text-anchor':'middle'}); title.textContent='Edad desde primera reseña (días)'; svg.append(title);
    groups.forEach((g,i) => {
      let d=`M ${x(0)} ${y(1)}`;
      (g.curve.points || []).forEach(p => { d+=` H ${x(p.time_days)} V ${y(p.survival)}`; });
      svg.append(svgNode('path',{d,fill:'none',stroke:colors[i], 'stroke-width':2}));
      (g.curve.points || []).filter(p => p.events || p.censored).forEach(p => {
        const point=svgNode('circle',{cx:x(p.time_days),cy:y(p.survival),r:3,fill:colors[i],tabindex:0});
        const text=`${g.label}: día ${f.format(p.time_days)} · S=${pct(p.survival)} · en riesgo ${p.at_risk} · eventos ${p.events} · censuras ${p.censored}${p.supported ? '' : ' · tramo con <20 en riesgo'}`;
        const tooltip=svgNode('title',{}); tooltip.textContent=text;point.append(tooltip);point.setAttribute('aria-label',text);
        point.addEventListener('mouseenter',()=>{$('survival-point').textContent=text;});point.addEventListener('focus',()=>{$('survival-point').textContent=text;});point.addEventListener('click',()=>{$('survival-point').textContent=text;});svg.append(point);
      });
    });
    $('survival-chart').replaceChildren(svg); $('survival-groups').replaceChildren();
    groups.forEach((g,i) => {
      const c=g.curve, card=node('div',undefined,'survival-group');const heading=node('h3',g.label);heading.style.color=colors[i];card.append(heading);
      card.append(node('p',`${c.businesses} negocios · ${c.events} cierres · ${c.censored} censuras · ${c.delayed_entries} entradas diferidas`, 'method-note'));
      card.append(node('p',c.median_days === null ? 'Mediana no estimable / no alcanzada.' : `Mediana: ${f.format(c.median_days)} días${c.median_supported ? '' : ' (cola con poco soporte)'}.`, 'method-note'));
      if(c.status!=='ok') card.append(node('p','Muestra insuficiente: no se traza curva.', 'method-note'));
      $('survival-groups').append(card);
    });
  }
  async function survival(event) {
    if(event) event.preventDefault(); if(!$('survival-form').reportValidity())return;
    const id=++survivalId; $('survival-refresh').disabled=true;label('survival-message','Consultando Kaplan–Meier…');
    try {
      const params=new URLSearchParams({method:$('survival-method').value});if($('survival-zone').value) params.set('zone',$('survival-zone').value);
      const data=await api(`/survival/${encodeURIComponent($('survival-category').value.trim())}?${params}`);
      if(id!==survivalId)return;
      chart([{label:data.category,curve:data.primary}, ...data.zones.map(z=>({label:z.zone_id,curve:z.curve}))]);
      label('survival-message',`Categoría ${data.category} · ${data.method} · datos retrospectivos; zonas de estudio, no barrios oficiales.`);
    } catch { if(id===survivalId){$('survival-chart').replaceChildren();$('survival-groups').replaceChildren();label('survival-message','No existe una curva para esa categoría o el servicio no está disponible.',true);} }
    finally{if(id===survivalId)$('survival-refresh').disabled=false;}
  }
  $('monitor-form').addEventListener('submit',monitor);$('survival-form').addEventListener('submit',survival);
  monitor();survival();
})();
