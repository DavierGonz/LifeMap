# E2.6 y E2.7: riesgo retrospectivo y alertas locales

Corte T: **2019-01-01 00:00 UTC**, horizonte **2020-01-01**. Cohorte con historia
hasta T y actividad posterior: **10.974 negocios**, 709 positivos (6,46%) y
10.265 negativos. Etiqueta 1: is_open=0 y ultima reseña en (T,T+12 meses].
Etiqueta 0: abierto al snapshot o actividad despues del horizonte. Se excluyen
los negocios sin actividad despues de T y los que no tenian historia a T.
`is_open` es retrospectivo y ultima reseña no es cierre verificado.

## Variables sin estadisticas futuras

`ml/risk.py` es la definicion comun para batch e inferencia. Solo admite seis
variables, con orden versionado:

1. Media de sentimiento: media de (prediccion-1), rango [-1,1]. Se usa el modelo
   temporal congelado, no los sentimientos E1 calculados con el modelo antiguo.
2. Tendencia negativa: pendiente OLS de seis porcentajes mensuales en intervalos
   [as-of-6 meses,as-of], puntos porcentuales por mes. Meses sin reseñas no se
   inventan como 0%; con menos de dos meses observados, pendiente 0 y flag de
   soporte. En T los intervalos son julio–diciembre de 2018.
3. Estrellas promedio de las reseñas disponibles; nunca `business.stars`.
4. Cantidad acumulada de reseñas disponibles; nunca `business.review_count`.
5. Edad observable desde primera reseña conocida, no desde fundacion.
6. Densidad por km²: competidores de la misma categoria principal a 500 m,
   contados con MongoDB `$geoNear`, excluyendo al propio negocio.

La densidad es **presencia historica observada**, no numero de competidores
activos: no se filtra por el estado final `is_open` ni por ultima reseña futura.
En T solo cuenta negocios con primera reseña <=T. Durante streaming agrega
negocios que ya llegaron por Kafka y cuya fecha es <=al evento puntuado.
El catalogo de ubicaciones/categorias se congela y verifica por SHA. Yelp no
ofrece su historial de cambios: se asume estabilidad como estratos fijos del
estudio. No puede acreditarse la ubicacion/categoria historica real sin esa
suposicion, ni conocerse el cierre verdadero de cada competidor en T.

Se reutilizan los vectores TF-IDF pre-T ya guardados y el clasificador temporal
para generar agregados; no se reentrena sentimiento ni se reingesta Yelp.
`risk_baselines` guarda sumas pre-T y fechas/negatividad de los ultimos seis
meses, sin copiar textos. En vivo se agregan exclusivamente reseñas recibidas
en `stream_reviews` con fecha <=al evento. Las estrellas de mensajes antiguos
se recuperan por su propio ID/fecha/texto, sin leer reseñas aun no recibidas.
Las curvas KM y todas las colecciones retrospectivas de Fase 3 quedan fuera.

## Entrenamiento y evaluacion

Una regresion logistica MLlib: StandardScaler, regularizacion 0,05, hasta 50
iteraciones y pesos inversos de clase calculados solo en entrenamiento.
Particion determinista, estratificada por etiqueta y disjunta por negocio:

| Particion | Negativos | Positivos |
|---|---:|---:|
| Entrenamiento | 7.185 | 496 |
| Validacion | 1.540 | 106 |
| Prueba | 1.540 | 107 |

El escalado se ajusta solo en entrenamiento. Umbral **0,541868**: maximiza F2
en validacion, sujeto a precision >=2 veces la prevalencia de validacion.
F2 da mayor peso a recall; la restriccion evita alertar indiscriminadamente.
No se cambia el umbral mirando prueba o para forzar alertas en la demo.

| Conjunto | AUC-ROC | AUC-PR MLlib | Recall | Precision |
|---|---:|---:|---:|---:|
| Validacion | 0,6493 | 0,1483 | 0,4245 | 0,1372 |
| Prueba | 0,5943 | 0,1332 | 0,3925 | 0,1273 |

Matriz de prueba: TN=1.252, FP=288, FN=65, TP=42. AUC-PR se compara con una
prevalencia de prueba del 6,50%; es area bajo PR de Spark, no average precision.
El rendimiento es modesto. La salida ponderada no esta calibrada como
probabilidad poblacional: `uncalibrated_score=true` acompaña al score guardado.

Es un **benchmark retrospectivo**: las etiquetas del horizonte se observan
despues de T; no se afirma que el modelo hubiera estado disponible en 2019.
No hay fuga de esos objetivos hacia las seis variables o hacia el ajuste de
escalado/umbral. Usar el modelo con fechas posteriores es una simulacion; su
calibracion y rendimiento para otros horizontes/cohortes no se han validado.

## Inferencia real e idempotencia

`ml/train_risk.py` es batch; `streaming/consumer.py` solo carga los dos modelos
guardados. El PipelineModel MLlib de riesgo transforma un DataFrame por lote
dentro de `foreachBatch`. Sus probabilidades se contrastan con los parametros
exportados; ID y hashes JSON/pipeline deben coincidir con `config/risk.json`.
El umbral es una politica separada de la probabilidad emitida por el modelo.

Cada reseña real se puntua a su propia fecha, aun si llega tarde. Se excluyen
de sus variables mensajes con fechas posteriores, aunque ya esten en Mongo.
`risk_features` mantiene el ultimo evento puntuado por negocio sin retroceder
cuando llegan eventos antiguos. `alerts` guarda solo scores >=umbral, con
business_id, review_id, geometria Point/2dsphere, score, variables, umbral,
fechas de evento/procesamiento, ID/hash de modelo y flag retrospectivo.
Alertas anteriores se desactivan al avanzar el estado del negocio.

La clave de alerta es modelo+review_id. Se usan upserts y una marca de modelo
procesado en la reseña, escrita despues de todos los efectos. Un reintento no
duplica alertas ni cambia su primera fecha de procesamiento; el lock de riesgo
separado conserva los checkpoints y offsets de Fase 2. No se asume orden global
de Kafka. La sonda de salud distingue cada instancia del proceso para no usar
un estado activo residual despues de reiniciar.

La demo usa 100 reseñas reales de 100 negocios de **prueba**, elegidos por score
alto (sin seleccionar por etiqueta), y genera 96 alertas. No es una estimacion
de precision poblacional. Se comprueba reinicio, reenvio, offsets alcanzados y
fallo despues de escribir alertas antes de confirmar un lote, con 20 reseñas
reales en colecciones/checkpoints de prueba aislados.

## Ejecucion local

Usar el entorno host `.venv` Python 3.10; no se instalan paquetes globales.
La ruta de cache temporal de Fase 1 y los modelos guardados son prerrequisitos.

```powershell
docker compose --profile e2 --profile streaming build streaming
docker run --rm --network lifemap_backend --memory 1g --cpus 1 --mount type=volume,src=lifemap_model_store,dst=/models --mount type=volume,src=lifemap_dataset_cache,dst=/data --mount type=bind,src=D:/Proyects/LifeMap/.secrets/mongo_writer_password.txt,dst=/run/secrets/mongo_writer_password,readonly --entrypoint /opt/spark/bin/spark-submit lifemap-spark:stream-v2 --master 'local[1]' --driver-memory 512m --conf spark.driver.host=127.0.0.1 --conf spark.driver.bindAddress=127.0.0.1 /opt/lifemap/ml/train_risk.py
docker compose --profile e2 --profile streaming up -d --no-deps --no-build --wait streaming
.\.venv\Scripts\python.exe scripts/test_risk_phase4.py
.\.venv\Scripts\python.exe -m unittest discover -s tests/unit -v
```

Un entrenamiento completo se reutiliza desde cache. Los artefactos estan en
`/models/risk/closure-risk-20190101-lr-v1`, el puntero en `/models/risk-active.json`
y los resultados batch en `/data/lifemap-e2/closure-risk-20190101-lr-v1`.
Para entrenar otra version se debe cambiar el ID y revisar/actualizar sus hashes
en config antes de construir la imagen de inferencia; nunca desactivar el guard.
Los archivos binarios y muestras con texto no van a Git. La evidencia minima
esta en [e2-phase4.json](reports/e2-phase4.json).

Referencias: [LogisticRegression MLlib](https://spark.apache.org/docs/3.5.8/ml-classification-regression.html)
y [MongoDB $geoNear](https://www.mongodb.com/docs/v8.0/reference/operator/aggregation/geonear/).
Sin Azure, push, endpoints nuevos, interfaz, Power BI o cambios Jenkins.
