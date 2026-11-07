# Trazabilidad de los 20 criterios academicos

Fuente principal: Word de LifeMap conservado en
`D:\Proyects\LifeMap\Insumos`. Las pruebas locales solo acreditan el alcance
indicado; no sustituyen la demostracion exigida en repositorio y URL publica.

| Criterio | Verificacion prevista | Estado |
|---|---|---|
| E1.1 Orquestacion y reproducibilidad | Compose + README, servicios saludables | Comprobado localmente; falta prueba en otro equipo limpio |
| E1.2 Despliegue funcional | Otro equipo, URL publica y SHA | Pendiente externo; no acreditado con localhost |
| E1.3 Integracion y despliegue continuo | Pipeline, credencial y gate | Jenkins local exitoso y fallo bloqueante; falta merge remoto/pollSCM real |
| E1.4 Ingesta y limpieza Dask | Descarga automatica y reglas contadas | Comprobado Yelp v4, ambos workers, cache y carga autorizada |
| E1.5 Datos y vida observada | GeoJSON/2dsphere, lotes, evento/censura | Comprobado 16.944 negocios / 1.320.594 resenas; aproximaciones documentadas |
| E1.6 Sentimiento | NB/LR y division por negocio | Entrenados, metricas y paridad; fuente SentimentStream no suministrada, reutilizacion literal pendiente |
| E1.7 MapReduce y tasa | reduceByKey igual a groupBy | Comprobado, 210 categorias / 390 celdas; minimo 3.650 dias |
| E1.8 Balanceo | Cuatro N, tres repeticiones y sesgo | Comprobado: 24 mediciones y filas por particion |
| E1.9 API geoespacial | Seis rutas con consultas/validacion | Comprobado local; falta URL desplegada |
| E1.10 Informe y sustentacion | Hasta seis paginas y cambio en vivo | Informe/README preparados; sustentacion y cambio remoto pendientes |
| E2.1 Integracion y despliegue | Kafka en mismo Compose y regresion de E1 | Pendiente |
| E2.2 Productor temporal | Reserva posterior a T, clave y particiones | Pendiente |
| E2.3 Inferencia streaming | Modelo E1 congelado y predicciones persistidas | Pendiente |
| E2.4 Ventanas y watermark | 30 dias, siete dias y geometria | Pendiente |
| E2.5 Supervivencia | Categoria/zona, censura, entrada y numero en riesgo | Pendiente de definicion y aprobacion |
| E2.6 Riesgo de cierre | Variables al corte y evaluacion temporal | Pendiente |
| E2.7 Alertas | foreachBatch, probabilidad, fecha y geometria | Pendiente |
| E2.8 API y Power BI | Cuatro endpoints y cuatro visualizaciones conectadas | Pendiente de aceptacion de Desktop |
| E2.9 Pruebas de flujo Jenkins | 100 mensajes, version de modelo y fallo bloqueante | Pendiente |
| E2.10 Informe y sustentacion | Maximo diez paginas y demo remota completa | Pendiente |
