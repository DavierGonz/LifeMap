# Infraestructura Docker local de LifeMap

Estado: los ocho servicios estan construidos, iniciados y saludables. Se
probaron tareas sinteticas Dask/Spark, carga del conector MongoDB, salud
integrada de Flask y acceso Jenkins/Docker CLI. Yelp v4 ya esta preparado con
Dask; la carga definitiva espera seleccion. No hay pipelines Jenkins.
Ver docs/reports/infrastructure-phase3.md para consumo y limites de evidencia.
Presupuesto: $0 COP. No requiere servicios cloud ni registro de imagenes de pago.

## Servicios y limites

| Servicio | Version base | Limite RAM | CPU maxima | Puerto al host |
|---|---|---:|---:|---|
| mongodb | MongoDB 8.0.16 | 768 MiB | 1 | Ninguno |
| spark-master | Spark 3.5.8, Java 17, Scala 2.12 | 1024 MiB | 1 | 127.0.0.1:8082 hacia 8080 |
| spark-worker | Misma imagen Spark | 1536 MiB | 1 | Ninguno |
| dask-scheduler | Dask/distributed 2025.5.1 | 384 MiB | 0,5 | 127.0.0.1:8788 hacia 8787 |
| dask-worker-1 | Misma imagen Dask | 1024 MiB | 1 | Ninguno |
| dask-worker-2 | Misma imagen Dask | 1024 MiB | 1 | Ninguno |
| api | Python 3.10.20, Flask 3.1.3, Gunicorn 23.0.0 | 256 MiB | 0,5 | 127.0.0.1:5100 hacia 5000 |
| jenkins | Jenkins 2.568.3, JDK 21, Docker CLI 29.5.3 | 768 MiB | 1 | 127.0.0.1:8083 hacia 8080 |

Suma de limites: 6784 MiB, equivalente a 6,625 GiB, y 7 CPU.
No son reservas ni una medicion de uso. Docker/WSL, builds, procesos efimeros
y herramientas del host necesitan margen adicional.

MongoDB usa cache WiredTiger de 0,25 GiB. Dask tiene un proceso y un hilo
por worker, limite de proceso de 768 MiB, derrame al 70 %, pausa al 80 %
y reinicio al 95 %. Spark declara un core y 1 GiB de capacidad de ejecutor
en el worker, con ejecutor de 768 MiB y driver de 512 MiB. El contenedor
del master contempla alojar ese driver en una fase posterior. Jenkins tiene
un ejecutor y heap maximo de 384 MiB; no se ejecutan builds concurrentes.

La holgura inicial se reserva para evolucionar hacia Kafka, inferencia y
streaming, pero no demuestra que puedan sumarse sin redistribuir memoria.
Entrenamiento, ingesta Dask y benchmark pesado seran secuenciales. El primer
arranque debe medir memoria y verificar limites efectivos de WSL/Docker.
No se modifica `.wslconfig`, Docker Desktop ni otros servicios automaticamente.

## Aislamiento

Proyecto Compose: `lifemap`. Imagenes propias:
`lifemap-mongo:infra-v1`, `lifemap-spark:infra-v1`,
`lifemap-dask:infra-v1`, `lifemap-api:infra-v1` y `lifemap-jenkins:infra-v1`.
No se usan `container_name`, recursos externos o nombres de volumenes compartidos.

Redes:

- `lifemap_backend`: interna; MongoDB solo pertenece a esta red.
- `lifemap_frontend`: API y consolas administrativas publicadas en loopback.
- `lifemap_ci`: salida de Jenkins para futuras consultas SCM y builds.

RPC Spark 7077, driver 7078, block manager 7079 y Dask 8786 permanecen
sin publicar al host. Los puertos internos repetidos no colisionan entre
proyectos con redes independientes. Los puertos elegidos estan libres en
la inspeccion; hay que comprobarlos otra vez antes del arranque.

Volumenes propios, creados durante los arranques autorizados:

- `lifemap_mongo_data` y `lifemap_mongo_config`.
- `lifemap_dataset_cache` y `lifemap_model_store`.
- `lifemap_spark_master_scratch` y `lifemap_spark_worker_scratch`.
- `lifemap_dask_worker_1_scratch` y `lifemap_dask_worker_2_scratch`.
- `lifemap_jenkins_home`.

El dataset se monta tambien en Spark worker para futuras lecturas distribuidas;
en ese worker es de solo lectura. El almacen de modelos es compartido por
master y worker. Dask y Spark conservan el patron de permisos por grupo 0
y directorios de datos con setgid del proyecto fuente. Las pruebas actuales
no acreditan todavia lectura/escritura distribuida de datasets o modelos.

El unico bind mount del host es el socket Docker en Jenkins. Ese acceso da
capacidad administrativa sobre el motor y no impide tecnicamente operar
otros proyectos. Se mantiene para reutilizar la integracion estable, con
Jenkins solo en loopback, asistente/autenticacion conservados y sin jobs
creados. Los futuros pipelines deben validar proyecto y recursos LifeMap;
no se permite limpieza global ni ejecucion de PR no confiables con secretos.
Una separacion fuerte del motor requeriria otro daemon/equipo autorizado.

## Secretos y MongoDB

En Fase 2 no se crearon contrasenas; en Fase 3 se prepararon `.env` y tres
archivos locales protegidos. `.env.example` contiene solamente sus rutas:

- `mongo_root_password.txt`: administrador `lifemap_admin`, solo MongoDB.
- `mongo_writer_password.txt`: usuario `lifemap_writer`, readWrite en lifemap.
- `mongo_reader_password.txt`: usuario `lifemap_reader`, read en lifemap.

Solo API recibe el secreto de lectura. Dask recibe unicamente la credencial
writer para los lotes de ingesta autorizados y sus fixtures. Spark no recibe
todavia credenciales MongoDB. Ningun secreto se copia a imagenes.
Compose monta los archivos en `/run/secrets`; no es una boveda cifrada.
Los archivos del host requeriran permisos de acceso adecuados.

El init MongoDB define usuarios, no colecciones ni indices de dominio.
Requiere tres valores distintos de al menos 24 caracteres. El script solo
se ejecuta al inicializar un volumen vacio; cambiar `.env` o un archivo
no rota usuarios de una base ya inicializada. Una inicializacion fallida
debe diagnosticarse sin eliminar volumenes automaticamente.

## Salud e inferencia de estado

- MongoDB: autenticacion como lector y ping; sin escrituras.
- Spark: estado de master y registro del worker; sin jobs.
- Dask: RPC y registro de cada worker; sin enviar tareas de computo.
- API `/live`: vida del proceso. `/health`: ping autenticado MongoDB,
  master Spark con al menos un worker ALIVE y Dask con al menos dos workers.
  Devuelve 503 si alguna dependencia falla y no expone detalles de errores.
- Jenkins: respuesta de la pagina de acceso; no ejecuta pipeline.

La API actual es un soporte de infraestructura, sin endpoints de negocios.
`DEPLOYED_COMMIT=unreleased` produce `commit_configured=false`. Incluso un
SHA configurado debe verificarse frente a la imagen/SCM en una fase posterior;
no es una certificacion de despliegue. El contrato actual es local y aun no
acredita el criterio de despliegue publico.

## Reutilizacion y dependencias

Se reutilizaron por lectura las versiones/digests y patrones de Compose,
healthchecks, permisos, logs, limites y persistencia de
`D:\Proyects\ParcialBigData`. Se copiaron exclusivamente locks de paquetes
y lista de plugins, sin secretos, datos, entornos o estado Jenkins.

Spark incorpora MongoDB Spark Connector 10.7.0 para Scala 2.12 y drivers Java
MongoDB 5.1.4. La documentacion declara Spark 3.2 a 3.5 compatible con ese
conector: https://www.mongodb.com/docs/spark-connector/v10.x/.
Los JAR quedaron incorporados en el build autorizado; no se descargan por job.

Locks y plugins conservan las versiones de la referencia y los builds de
las cinco imagenes propias terminaron correctamente, reutilizando cache
disponible. Spark usa el alias exclusivo backend spark-master-internal para
evitar escuchar en la interfaz frontend al resolver su propio nombre.
No hay minimo obligatorio de 4 GiB antes de builds: se mide el consumo y se
detiene ante OOM, agotamiento critico, reinicios inesperados o inestabilidad.

No se traslado logica US Accidents, valores de muestreo, coleccion accidents,
scripts de pipelines, endpoints del proyecto anterior o configuracion SCM.
Discos solo se consulta como referencia; no hay Kafka incorporado aun.

## Entrega 2 y pendientes

El mismo Compose se ampliara con Kafka KRaft, topic reviews con al menos tres
particiones, productor, jobs Structured Streaming, volumen de checkpoints,
conector Kafka compatible con Spark 3.5.8, modelos versionados y metricas de
retraso. Se definiran secretos/redes por necesidad y se revisara el presupuesto
antes de aumentar limites. No se han reservado puertos, creado topics o
instalado esas dependencias ahora.

Tambien quedan pendientes los seis endpoints academicos completos, indices
y colecciones Yelp, asistente/usuario Jenkins y pipeline con pollSCM,
credenciales SCM, pruebas de negocio y despliegue. El servidor distinto, la URL y
su disponibilidad, Power BI Desktop y area/categorias siguen sin resolver.
No bloquean la preparacion local autorizada.
