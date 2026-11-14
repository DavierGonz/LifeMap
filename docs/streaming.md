# Entrega 2: Kafka, reproduccion temporal y Structured Streaming local

Se conserva E1. La fase 1 incorpora Kafka/productor y la fase 2 el consumidor
Structured Streaming. No hay cambios en Azure o Jenkins. En la auditoria de
fase 1, el job local Jenkins `lifemap-main` estaba deshabilitado;
GitHub main coincide con `036c8aa`. La evidencia historica local acredita el
pollSCM y despliegue E1 en Azure; su estado actual no se verifico porque no se
enciende ni consulta la VM en esta fase.

## Corte temporal y correccion del modelo

`config/streaming.json` fija **T = 2019-01-01 00:00:00 UTC**, incluido en el
historico. El corpus E1 tiene 994.877 reseñas hasta T y 325.717 posteriores,
con observaciones hasta 2022-01-19. Hay 10.974 negocios con historia hasta T y
actividad posterior: 709 positivos y 10.265 negativos para el objetivo
retrospectivo definido por el Word a T+12 meses (2020-01-01). Estos conteos
sirven para evaluar la viabilidad; no son metricas de un modelo de riesgo.
Las fechas son actividad observada por reseñas, no apertura ni cierre real.

El entrenamiento E1 separo negocios, pero uso todo el periodo: incluyo 262.727
reseñas posteriores a T. Se conserva su modelo activo sin cambios.
`ml/train_temporal.py` corrige el mismo pipeline mediante entrenamiento batch
con **todos los pasos, IDF, clasificadores y seleccion limitados a fechas <= T**.
Conserva la particion determinista por negocio y compara NB/LR con F1 macro.
El holdout pre-T se usa para seleccion, no se presenta como evaluacion final
independiente. Guarda versiones en `/models/e2-temporal/` y un puntero separado
`/models/sentiment-temporal.json`, con SHA256, fechas maximas, hash ordenado de
IDs de entrenamiento, metricas y paridad de inferencia. No actualiza reseñas,
negocios, predicciones ni el puntero `/models/sentiment-active.json` de E1.
El productor rechaza el modelo anterior por falta de procedencia temporal.

Resultado del batch local: 797.821 reseñas de entrenamiento y 197.056 de
validacion; cero negocios compartidos; NB F1 macro 0,6719 frente a LR 0,6537;
30 comprobaciones de equivalencia Spark/exportacion. Version seleccionada:
`sentiment-until-20190101T000000Z-v1-naive_bayes`. El detalle reproducible queda
en `/data/lifemap-e2/sentiment-until-20190101T000000Z-v1/training.json` y el resumen
de pruebas en [e2-phase1.json](reports/e2-phase1.json).

La auditoria tambien encontro la imagen API local anterior a la interfaz E1.
Se reconstruyo desde el codigo existente y se alineo `DEPLOYED_COMMIT` local
con `036c8aa`; `/`, los seis endpoints y el modelo E1 quedaron comprobados.
No se modifico el codigo funcional de E1 ni el despliegue Azure.

## Ejecucion local (PowerShell desde la raiz)

Usar Docker Desktop local y las credenciales Mongo existentes fuera de Git.
No descargar de nuevo Yelp. Los scripts del host usan exclusivamente `.venv`.

```powershell
# Omitir si ya esta saludable; no recrea servicios E1.
docker compose --profile e2 up -d --no-deps --no-build --wait --wait-timeout 60 kafka
docker exec lifemap-kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic reviews --partitions 3 --replication-factor 1
docker exec lifemap-kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic reviews

# Batch temporal separado. local[1] evita otro ejecutor por la RAM del portatil.
docker cp ml/train_temporal.py lifemap-spark-master-1:/tmp/train_temporal.py
docker exec lifemap-spark-master-1 /opt/spark/bin/spark-submit --master 'local[1]' --conf spark.driver.host=127.0.0.1 --conf spark.driver.bindAddress=127.0.0.1 --conf spark.sql.files.maxPartitionBytes=16777216 /tmp/train_temporal.py --cutoff 2019-01-01T00:00:00Z

# Construccion ligera, separada de las imagenes E1.
docker build -f docker/producer/Dockerfile -t lifemap-producer:e2-v1 .

# Montajes de solo lectura; el productor solo consulta MongoDB.
$lifeMapMounts = @(
  '--mount', 'type=volume,src=lifemap_model_store,dst=/models,readonly',
  '--mount', 'type=bind,src=D:/Proyects/LifeMap/.secrets/mongo_reader_password.txt,dst=/run/secrets/mongo_reader_password,readonly'
)
docker run --rm --network lifemap_backend --memory 192m --cpus 0.5 @lifeMapMounts lifemap-producer:e2-v1 --cutoff 2019-01-01T00:00:00Z --run-id demo-local-1 --limit 100 --seconds-per-day 0.3333333333333333

# Prueba real: 100 reseñas, lectura de retorno, claves, orden y corte.
docker run --rm --network lifemap_backend --memory 192m --cpus 0.5 @lifeMapMounts --entrypoint python lifemap-producer:e2-v1 -m streaming.smoke --cutoff 2019-01-01T00:00:00Z --output /tmp/producer-smoke.json

# Repetir la auditoria de fechas y de negocios usados en E1, sin escribir datos.
docker run --rm --network lifemap_backend --memory 256m --cpus 0.5 @lifeMapMounts --mount type=volume,src=lifemap_dataset_cache,dst=/data,readonly --mount type=bind,src=D:/Proyects/LifeMap/streaming,dst=/app/streaming,readonly --entrypoint python lifemap-dask:infra-v1 -m streaming.audit --output /tmp/temporal-audit.json

.\.venv\Scripts\python.exe -m unittest discover -s tests/unit -v
.\.venv\Scripts\python.exe scripts/validate_infrastructure.py
```

`seconds-per-day = 10/30` fija como objetivo 30 dias de eventos por 10 segundos;
`0` envia sin esperas para las pruebas. El reloj se ancla a la primera reseña
publicada, despues del sort de MongoDB, y usa tiempo monotono. El transporte
y sus confirmaciones pueden limitar la velocidad efectiva.
Orden: `date`, luego `review_id` como desempate.
La clave UTF-8 es `business_id`; Kafka conserva orden **por particion**, no un
orden global de lectura entre las tres particiones. La prueba compara offsets,
fechas, timestamps de evento e IDs recibidos con los envios confirmados.
El payload excluye estrellas y sentimientos antiguos para evitar que se usen
accidentalmente como predictores. Incluye modelo esperado y ID de reproduccion.

El productor usa `acks=all`, idempotencia de cliente y un envio pendiente como
maximo. Esto no deduplica ejecuciones diferentes: repetir el comando vuelve a
publicar las reseñas; no se afirma entrega exactamente una vez entre reinicios.
El consumidor futuro debera usar `review_id` para escrituras idempotentes.

Kafka: imagen oficial 4.1.2 fijada por digest, KRaft de un solo nodo, tres
particiones, replica 1 (sin alta disponibilidad), volumen `lifemap_kafka_data`,
red `lifemap_backend` interna, ningun puerto publicado, limite 1 GiB/1 CPU,
heap 256–512 MiB. Retencion limitada a 512 MiB por particion, sin caducidad
por edad para no descartar inmediatamente eventos historicos con timestamps
de 2019. Revisar el limite si se necesita conservar el replay completo.
E1 mantiene sus limites y montajes.
La suma de limites es 7.625 GiB; no representa consumo simultaneo reservado.

Si se interrumpe el batch, conservar e inspeccionar cualquier salida parcial
antes de reintentar. El script no sobrescribe modelos parciales. Un parquet
sin `_SUCCESS` no es una cache valida. La cache completa del batch se reutiliza
sin reentrenar cuando su reporte y los hashes del modelo coinciden.

Referencias: [imagen oficial Apache Kafka](https://kafka.apache.org/41/getting-started/docker/)
y [ejemplo KRaft de Apache](https://github.com/apache/kafka/blob/4.1.1/docker/examples/docker-compose-files/single-node/plaintext/docker-compose.yml).

## Fase 2: Structured Streaming local

```powershell
docker compose --profile e2 --profile streaming build streaming
# Flujo continuo; no recrea Spark/Dask/API/Jenkins de E1.
docker compose --profile e2 --profile streaming up -d --no-deps streaming
docker compose --profile e2 --profile streaming logs --tail 20 streaming
# Prueba acotada con reseñas reales y fixtures aislados; no dejar otro consumidor
# production usando el mismo checkpoint mientras se ejecuta esta prueba.
docker compose --profile e2 --profile streaming stop streaming
.\.venv\Scripts\python.exe scripts/test_streaming_phase2.py
```

El consumidor usa Spark 3.5.8 y el conector Kafka de la misma version, incluidos
en la imagen: no descarga paquetes al arrancar. Tiene 1 CPU, limite 1 GiB y
driver de 512 MiB, `local[1]` dentro de Docker para evitar otro ejecutor.
Lee las tres particiones; retiene topic, particion y offset como procedencia.
No presupone orden global entre particiones. El join estatico con los 16.944
negocios proporciona coordenadas y geohash de precision 5. El modelo Spark se
carga una vez; ID, SHA del predictor y SHA del directorio Spark se comparan
con `config/streaming.json`. No existe entrenamiento en el consumidor.

Hay dos consultas con checkpoints independientes en `lifemap_stream_checkpoints`:

- `reviews`: clasifica todos los eventos validos, incluidos tardios, y hace
  upsert por `review_id` en `lifemap.stream_reviews`. Guarda texto, fecha
  original y fecha UTC, prediccion, tres probabilidades, modelo, procesamiento
  y GeoJSON. `$setOnInsert` conserva el mismo documento tras repetir un lote.
- `windows`: aplica `withWatermark(date, '7 days')`,
  `dropDuplicatesWithinWatermark(review_id)` y `groupBy(window(date, '30 days'),
  geohash)` nativos de Structured Streaming. Ventanas consecutivas de 30 dias
  exactos, no meses calendario, alineadas con T. En modo append solo publica
  ventanas finalizadas cuando el watermark supera su fin. El estado pendiente
  permanece en el checkpoint; avanzar el reloj de pared no cierra ventanas.

Las reseñas de Yelp tienen IDs y fechas inmutables. Repetir mensajes no aumenta
las ventanas: la deduplicacion nativa y su watermark conservan esa identidad;
un duplicado que ya quedo fuera del estado tambien queda fuera por su fecha.
Los offsets se recuperan del checkpoint; `startingOffsets=earliest` aplica
solo a su primera ejecucion. No borrar checkpoints para reiniciar. Un lock
impide reutilizarlos con otro modelo, parametros o particionado de estado.
Las dos consultas pueden avanzar a ritmos distintos; las reseñas clasificadas
no equivalen necesariamente al total de las ventanas (tardios y ventanas aun
abiertas). En modo `--available-now` se ejecutan secuencialmente para reducir RAM.

Las ventanas finales se guardan por celda e inicio en `stream_windows`, con
conteo, negativos, porcentaje y poligono GeoJSON indexado `2dsphere`.
`hotspots` contiene las celdas con incremento **>=15 puntos porcentuales** frente
a la ventana inmediatamente anterior, con **>=30 reseñas en ambas**. Es un
umbral practico para detectar cambios grandes y evitar grupos diminutos; no
es una prueba de significancia ni una estimacion de cierre. Sin ventana previa
consecutiva y suficiente, no se declara hotspot. La escritura por clave es
idempotente, no usa incrementos; revisa tambien la ventana siguiente para
resolver correctamente resultados que lleguen en distinto orden.

La prueba publica 103 reseñas reales (100 del inicio y tres de fechas posteriores
para cerrar ventanas), comprueba inferencia y geometria, y reproduce una falla
intencional despues de escribir Mongo pero antes de confirmar offsets. Reinicia
con el mismo checkpoint y verifica que no duplica ni cambia `processed_at`.
Un fixture con textos sinteticos y coordenadas de un negocio real prueba un
tardio dentro de siete dias, otro fuera, y un incremento de 20% a 70% negativo.
Los fixtures llevan `test_fixture=true`, se excluyen de produccion y usan
colecciones/checkpoints `test_*` separados. No son hallazgos sobre Yelp.
La prueba no entrena ni modifica `reviews`, `businesses` o el modelo activo E1.
Evidencia: [e2-phase2.json](reports/e2-phase2.json).

Resultado verificado: 103 reseñas reales clasificadas, 52 ventanas finales
(102 reseñas; otra permanece en una ventana abierta) y cero hotspots reales.
El fixture aislado produjo dos ventanas de 30 reseñas, 20% y 70% negativas,
y un hotspot sintetico. Se comprobo el reinicio tras una escritura parcial
de 13 documentos sin duplicarlos, la admision del tardio dentro del margen y
el descarte del tardio fuera del margen para los agregados. Las reseñas tardias
siguen clasificadas. E1 conserva sus 16.944 negocios, 1.320.594 reseñas, interfaz,
seis endpoints y modelo original. Consumidor continuo saludable, limite 1 GiB,
consumo maximo observado de 637 MiB y ningun OOM/reinicio inesperado.

La fase termina aqui: sin Kaplan-Meier, riesgo, alertas de cierre, Power BI,
endpoints nuevos, cambios Jenkins ni despliegue Azure.
Referencias: [Structured Streaming 3.5.8](https://spark.apache.org/docs/3.5.8/structured-streaming-programming-guide.html)
y [conector Kafka 3.5.8](https://spark.apache.org/docs/3.5.8/structured-streaming-kafka-integration.html).
