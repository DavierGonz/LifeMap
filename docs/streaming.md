# Entrega 2, fase 1: Kafka y reproduccion temporal

Se conserva E1. No hay consumidor Structured Streaming en esta fase, ni cambios
en Azure o en Jenkins. El job local Jenkins `lifemap-main` esta deshabilitado;
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
