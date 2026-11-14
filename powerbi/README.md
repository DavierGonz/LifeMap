# LifeMap E2: API y Power BI Desktop

Trabajo exclusivamente local: `http://127.0.0.1:5100`. Power BI Desktop gratuito
está instalado (2.159.1231.0). No requiere Pro, inicio de sesión ni Azure.

## Contratos API

Todos devuelven JSON, fechas UTC ISO, errores JSON 400 para entradas inválidas,
404 para categoría inexistente y 503 para dependencias no disponibles.
Las geometrías son GeoJSON WGS84 `[longitud, latitud]` de MongoDB real.

| GET | Parámetros y respuesta |
| --- | --- |
| `/live/hotspots` | `lat,lon,radius_m`, opcionales `from,to,limit,offset`. `hotspots[]`, `windows[]`, `total,total_windows,count,window_count,has_more,generated_at`. Ventanas finalizadas que intersectan `[from,to)`; cada fila conserva su ventana completa de 30 días, geohash, geometría Polygon, reviews, negative, negative_percentage y change_percentage_points. No se inventan hotspots cuando la colección está vacía. |
| `/alerts` | Radio `lat,lon,radius_m` **o** `polygon` (GeoJSON Polygon codificado como JSON en query); `from,to` sobre event_date; `active=true/false`, `min_score,max_score,business_id,limit,offset`. `results[]`, `count,total,has_more,generated_at`. |
| `/risk/near` | `lat,lon,radius_m`; opcionales `business_id,min_score,from,to,limit,offset`. Fechas sobre as_of. `results[]` con nombre, geometry Point, features (seis variables), risk_score, model_id, as_of, processed_at, threshold. Ausencia de score no significa riesgo cero. |
| `/survival/<categoria>` | `method=observed_origin/landmark_T`, `zone=sector_oeste/sector_este`, `run_id` opcional. `primary` contiene la curva categoría existente; `zones[]` contiene la intersección **categoría y polígono** seleccionada por `$geoWithin`. Cada curva incluye businesses, events, censored, delayed_entries, median_days (null si no alcanzada), median_supported y points[{time_days,survival,at_risk,events,censored,entered,supported}]. Mínimo 50 negocios y advertencia <20 en riesgo. |

Radio 1–50.000 m, límite 1–500 (100 por defecto), offset 0–999.999. Paginar
hasta `has_more=false`; en hotspots ese indicador cubre ambas listas, cada una
con su propio total. Coordenadas/score finitos, score 0–1. Fechas `from < to`,
límite superior exclusivo. Polígono simple, cerrado, un anillo, máximo 101
posiciones y extensión 5 grados. Se rechazan parámetros desconocidos/repetidos.
`risk_score` es **no calibrado**, no una probabilidad poblacional de cierre.
Las alertas de F4 son una demostración seleccionada por score, no prevalencia.
Las curvas retrospectivas no son variables del modelo de riesgo.

`/health` conserva Mongo/Spark/Dask/API y, con `E2_ENABLED=1` en `.env`, añade
Kafka (protocolo real, tres particiones, offsets finales) y streaming (offsets
persistidos por consulta, lag en mensajes, heartbeat, watermark histórico y
versiones de modelos). Lag = máximo de los dos consumidores independientes,
no suma de copias de los mismos mensajes. Un servicio ausente produce 503.
Checkpoint montado solo lectura en API; no se crean grupos ni commits Kafka.

## Preparar el informe

El informe final **LifeMap-E2.pbix** ya está guardado en esta carpeta y se
comprobó abierto en Desktop con los cuatro visuales. El proyecto de texto
**LifeMap-E2.pbip**, Report y SemanticModel se versiona para reproducirlo sin
publicar datos ni credenciales; `.pbi/`, PBIX y cachés quedan fuera de Git.
Para usarlo basta abrir el PBIX, mantener la API local activa y pulsar Actualizar.
Para reconstruir desde fuentes, abrir el PBIP y actualizar; guardar como PBIX.
El origen autorizado por el usuario es Organizativo, con acceso anónimo local.
El usuario inició sesión para el mapa nativo y Desktop confirmó licencia gratuita.

La API E2 se activa conservando los servicios/checkpoints existentes:
`E2_ENABLED=1` en `.env` local, `docker compose build api` y
`docker compose up -d --no-deps --no-build api`. No reconstruir el resto ni
ejecutar entrenamientos/ingestas. El valor por defecto 0 conserva salud E1
para instalaciones que aún no usan los perfiles Kafka/streaming.

Las consultas `.pq` permiten reconstruir también las tablas manualmente:

1. Power BI Desktop → Obtener datos → Consulta en blanco → Editor avanzado.
   Pegar `LifeMap.pq` y llamar la consulta **LifeMap**. Deshabilitar su carga:
   devuelve un registro de tablas, no una tabla final. El código ya fue abierto
   en Desktop y su editor confirmó que no tiene errores de sintaxis.
2. Completar **manualmente** el aviso de privacidad para la API local y, si
   aparece, acceso anónimo. No desactivar privacidad global ni publicar datos.
3. Crear cinco consultas en blanco llamadas como los cinco `.pq` pequeños y
   pegar cada referencia. Cerrar y aplicar. Los tipos son explícitos incluso
   si Hotspots está vacío; no se usan CSV ni datos ficticios.
4. Crear los cuatro visuales en una página:

| Visual | Campos |
| --- | --- |
| Mapa nativo Azure Maps | Hotspots.latitude / longitude (categorías de datos Latitud/Longitud), ubicación geohash, tamaño reviews, tooltips máximos negative_percentage y change_percentage_points por celda en el período. Cero puntos es el resultado real actual. El mapa se renderiza con la cuenta gratuita ya iniciada; no se crea un recurso Azure de pago. |
| Tabla de alertas activas | Alerts.business_id, event_date, risk_score, model_id. No resumir score; título «Score no calibrado». |
| Curvas Kaplan–Meier | Eje numérico KaplanMeier.time_days ascendente, Y survival (máximo, no suma), leyenda group, tooltips at_risk, events, censored. Línea escalonada configurada. La tabla conserva supported para identificar colas con <20 en riesgo. |
| Evolución negativa | Evolution.start (fecha simple, no jerarquía), negative_percentage en Y (máximo, no suma); tooltip reviews/negative. El cálculo es `100*sum(negative)/sum(reviews)` por ventana entre celdas, no promedio de porcentajes. |

La tabla inferior Refresh incluye fecha de fuente/carga y conteos para comprobar actualización.
Guardar localmente `LifeMap-E2.pbix` (excluido de Git por contener datos).
Cambiar Area/Category al inicio de `LifeMap.pq` permite otra selección real.
Radio 50 km es selección espacial; no representa todo el dataset académico.

## Actualización y estado comprobado

Es modo **Import**: requiere **Inicio → Actualizar**; no hay refresco automático
continuo ni programado en el servicio Power BI. `Web.Contents(IsRetry=true)`
evita reutilizar la respuesta de caché HTTP al evaluar una consulta. La fuente
es siempre la API; no leer volúmenes ni secretos desde Power BI.

Se comprobaron los cuatro visuales tras dos refrescos de Desktop. Las llamadas
de `Microsoft.Data.Mashup` a la API devolvieron 200. La consulta de solo lectura
al modelo local (`verify-desktop.ps1`, ADOMD ya instalado con Power BI) confirmó
96 alertas, 0 hotspots, 2.384 puntos KM y 10 puntos de evolución. `loaded_at_utc`
cambió de `2026-11-14T13:13:25.080Z` a `2026-11-14T13:15:44.823Z`; la fecha de
la respuesta API también avanzó. No se usaron datos de prueba en el dashboard.
El negocio de la demostración real contiene en Power BI el mismo score
`0.5545494313114067` y evento `2020-10-30T19:33:53Z` actualizados por Spark/Mongo.
No quedan operaciones manuales pendientes para el informe local de esta máquina.
Otro computador debe autorizar su propia fuente e iniciar sesión gratuita para
Azure Maps; los permisos y credenciales no se versionan.

La API/interfaz sí se prueban con datos reales: `scripts/test_api_phase5.py`,
`tests/browser/lifemap-ui.cjs` (escritorio/móvil) y
`scripts/test_refresh_phase5.py` (una reseña Yelp ya existente pasa por Kafka,
Spark/modelos guardados y Mongo; observa cambio de as_of y score en la API).
Esta última conserva una selección en caché; reejecutarla reproduce el mismo
evento idempotente, no vuelve a entrenar ni carga el dataset.

Referencias oficiales: [Web.Contents](https://learn.microsoft.com/en-us/powerquery-m/web-contents)
y [origen Web de Power Query](https://learn.microsoft.com/en-us/power-query/connectors/web/web).
