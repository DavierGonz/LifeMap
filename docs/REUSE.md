# Reutilizacion comprobada

Se consultaron sin modificar Geoespacial (`D:\Proyects\ParcialBigData`) y
Discos (`D:\Proyects\Python\BigData\Discos`). No se copiaron secretos, datos,
entornos, volumenes o estado Jenkins. La poblacion sigue siendo Restaurants
dentro del rectangulo aprobado para desarrollo; los tags de categoria principal
no amplian esa seleccion.

- Geoespacial: `ingestion/download.py`, `etl.py`, `run.py`: cache, hashes,
  redireccion sin Authorization, dos particiones en vuelo y lotes MongoDB.
- Geoespacial: `api/validation.py` y `repository.py`: validadores numericos,
  poligonos, consultas acotadas $near/$geoWithin/$geoNear y errores seguros.
- Geoespacial: `ci/pipeline.sh`, `scripts/jenkins_local.py`: cliente Jenkins,
  checkout local real, pytest bloqueante, imagen candidata y promocion posterior.
- Discos: `src/common.py` y `batch.py`: formula AFR adaptada a dias-negocio,
  RDD reduceByKey, oraculo groupBy, repeticion y distribucion por particion.
- Sentimiento: se implemento el flujo exigido por el Word. No se encontro
  codigo fuente de SentimentStream entre las referencias; no se atribuye una
  copia o reutilizacion de codigo que no se ha podido comprobar.

Hashing del predictor: algoritmo MurmurHash3 x86_32, semilla 42, contrastado
con HashingTF Spark 3.5.8 y con probabilidades nativas en 30 textos reales.
Referencia Apache Spark (licencia Apache 2.0):
https://github.com/apache/spark/blob/v3.5.8/common/unsafe/src/main/java/org/apache/spark/unsafe/hash/Murmur3_x86_32.java

La categoria principal es el primer tag alfabetico distinto de Restaurants,
Food y Nightlife; Yelp no proporciona una bandera de categoria principal.
Es una regla reproducible, no una jerarquia oficial o una nueva seleccion.
