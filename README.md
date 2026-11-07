# LifeMap - Entrega 1

Proyecto universitario: Yelp -> Dask -> MongoDB -> Spark MLlib/MapReduce -> Flask.
Presupuesto $0 COP. Entrega 2 fuera de alcance. Referencias Geoespacial y Discos
consultadas sin modificar sus recursos. Codigo adaptado y limites: [REUSE](docs/REUSE.md).

## Resultado y alcance

Yelp `yelp-dataset/yelp-dataset`, version **4**: 150.346 negocios y 6.990.280
resenas inspeccionadas. Seleccion autorizada por el usuario: Restaurants,
rectangulo Filadelfia/entorno `[-76.1,39.3,-74.4,40.6]`: **16.944 negocios y
1.320.594 resenas** cargados. No es un limite metropolitano oficial. Falta
notificacion/exclusividad ante el profesor; no se afirma aprobacion docente.

Entrenamiento por negocio (13.555 train / 3.389 test, interseccion cero), Naive
Bayes y regresion logistica. NB seleccionado por macro F1 (0,6775 frente a
0,6540); modelo Spark guardado e inferencia Flask equivalente comprobada.
RDD y DataFrame coinciden; benchmark real 2/8/32/200 particiones, dos repartos,
tres repeticiones. Evidencias finales: `docs/reports/e1-evidence.json`,
`docs/reports/e1-jenkins.json` e informe `docs/reports/LifeMap-Entrega1.pdf`.

**Despliegue externo ejecutado:** la API responde en
https://lifemap-e1-davier-2026.westus2.cloudapp.azure.com con ocho servicios
saludables y seis endpoints comprobados desde Internet. Evidencia en
deploy/azure/deployment-evidence.json. Faltan
primer push autorizado y demostracion GitHub -> Jenkins -> URL. El repositorio
origin conserva la visibilidad publica elegida por el usuario, sin push.
La sustentacion y la reutilizacion verificable de SentimentStream tambien
requieren atencion: su codigo no esta en las referencias suministradas.

Servidor Azure comunicado: Ubuntu 24.04 x64, 20.230.240.88; preparacion sin
operaciones remotas en [deploy/azure](deploy/azure/README.md).

## Arranque desde cero (Windows / Docker Desktop + WSL2)

Requisitos: Git, Python 3.10 y Docker Desktop Linux ya instalados; unos 30 GB
libres para cache, Parquet, modelos e imagenes. En Linux pueden emplearse los
mismos archivos Compose; los lanzadores host estan preparados para Windows.
No usar recursos ni credenciales de otros proyectos.

Obtener primero el codigo: mientras origin siga vacio, transferir un bundle
creado con `git bundle create lifemap.bundle main` y ejecutar
`git clone lifemap.bundle D:\Proyects\LifeMap`. El bundle contiene solo lo
versionado, sin secretos/datos. Tras el primer push autorizado, se podra usar
`git clone https://github.com/DavierGonz/LifeMap.git D:\Proyects\LifeMap`.

```powershell
Set-Location D:\Proyects\LifeMap
# Solo si .venv todavia no existe:
py -3.10 -m venv .venv
# Desde este punto toda ejecucion Python Windows usa este interprete.
.\.venv\Scripts\python.exe scripts\check_environment.py
```

Crear `.secrets` **solo en un equipo nuevo** y protegerla antes de generar
credenciales. No repetir sobre la instalacion actual ni sobrescribir secretos:

```powershell
New-Item -ItemType Directory -Path .secrets
$lifeMapIdentity = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
icacls .secrets /inheritance:r /grant:r "${lifeMapIdentity}:(OI)(CI)F" "SYSTEM:(OI)(CI)F"
.\.venv\Scripts\python.exe scripts\prepare_local_env.py
```

El generador crea contrasenas exclusivas y `.env`; se niega a sobrescribirlas.
Guardar un token Kaggle personal en `.secrets/kaggle-token.txt` o el formato
legacy en `.secrets/kaggle.json`. Aceptar los terminos de Yelp/Kaggle cuando
corresponda; no pegar secretos en terminal/chat. `.env`, `.secrets`, `.venv`,
datasets, modelos y volumenes estan excluidos de Git y del contexto de build.
No instalar dependencias Python globales: las aplicaciones se instalan en Docker.

```powershell
# En un checkout con commit local; usa el SHA real en build y /health:
.\.venv\Scripts\python.exe scripts\set_revision.py
docker --context desktop-linux compose --project-name lifemap config --quiet
# Construir secuencialmente (Spark y Dask comparten imagen con sus workers):
docker --context desktop-linux compose --project-name lifemap build mongodb
docker --context desktop-linux compose --project-name lifemap build dask-scheduler
docker --context desktop-linux compose --project-name lifemap build spark-master
docker --context desktop-linux compose --project-name lifemap build api
docker --context desktop-linux compose --project-name lifemap build jenkins
docker --context desktop-linux compose --project-name lifemap up -d --no-build --wait
```

Los ocho servicios se comunican por nombres Compose. Mongo no publica puerto;
red backend interna y volumenes `lifemap_*` independientes. Host **solo loopback**:
API 5100, Spark UI 8082, Dask 8788, Jenkins 8083. Mongo 8.0.16, Spark 3.5.8,
conector Mongo Spark 10.7.0; dependencias exactas/digests en Dockerfiles/locks.
RAM maxima por contenedor: Mongo 768 MiB; master Spark 1.024; worker 1.536;
scheduler Dask 384; cada worker Dask 1.024; API 256; Jenkins 768.
Total de limites 6.625 GiB, CPU sumada 7; no es consumo reservado ni capacidad
para ejecutar trabajos pesados simultaneamente. Equipo 16 GB, WSL comparte RAM
con Windows: no sumar WSL otra vez. Medir RAM/CPU y ejecutar trabajos por turnos;
los lanzadores abortan su propio job ante presion critica sostenida/OOM.
No cerrar aplicaciones ajenas, hacer prune o cambiar Docker/WSL globalmente.

## Datos, entrenamiento y resultados reproducibles

```powershell
.\.venv\Scripts\python.exe scripts\yelp.py download
.\.venv\Scripts\python.exe scripts\yelp.py analyze
.\.venv\Scripts\python.exe scripts\yelp.py load --approved-scope config\yelp_selection.json
# Para trabajos grandes en 16 GB, detener unicamente Dask y API temporalmente:
docker --context desktop-linux compose --project-name lifemap stop api dask-worker-1 dask-worker-2 dask-scheduler
.\.venv\Scripts\python.exe scripts\spark_e1.py train
.\.venv\Scripts\python.exe scripts\spark_e1.py analytics
docker --context desktop-linux compose --project-name lifemap up -d --no-build --wait
```

Descarga fijada a v4; cache validada SHA256 en volumen `lifemap_dataset_cache`,
reutilizada sin redescarga. Dask usa ambos workers, particiones 8 MiB y lotes
Mongo de 500 upserts; los conteos no dependen del `review_count` declarado.
Una carga completa verificada no sobrescribe enriquecimientos al repetirse.
No usar `down -v` ni borrar cache/modelos. Reejecuciones Spark completas reutilizan
sus manifiestos; una ejecucion incompleta conserva sus archivos para diagnostico.

Vida observada = (ultima - primera resena)/86.400 segundos, cero para una sola
fecha; evento `is_open=0`, censura `is_open=1`. No son fechas reales de apertura
ni cierre; no extender artificialmente hasta el corte. Etiquetas por estrellas
solo como objetivo, nunca como predictor. IDF solo train; comparacion de dos
modelos en un unico test no equivale a una estimacion final independiente.
Tasa anualizada = cierres/(dias-negocio/365)*100; minimo 3.650 dias agregados,
heuristica de exposicion y no intervalo de confianza. Categoria principal:
primer tag alfabetico distinto de Restaurants/Food/Nightlife, fallback Restaurants;
Yelp no aporta una categoria principal oficial. Geohash precision 5.

## API y pruebas

```powershell
Invoke-RestMethod http://127.0.0.1:5100/health
Invoke-RestMethod 'http://127.0.0.1:5100/businesses/near?lat=39.9526&lon=-75.1652&radius_m=5000&limit=5'
Invoke-RestMethod 'http://127.0.0.1:5100/zones/summary?lat=39.9526&lon=-75.1652&radius_m=5000'
Invoke-RestMethod 'http://127.0.0.1:5100/stats/closure?group_by=geohash&min_business_days=3650&limit=5'
Invoke-RestMethod -Method Post -Uri http://127.0.0.1:5100/predict -ContentType application/json -Body '{"text":"Excellent food and friendly service"}'
# /businesses/within: POST {"geometry":{"type":"Polygon","coordinates":[[[-75.3,39.85],[-75,39.85],[-75,40.1],[-75.3,40.1],[-75.3,39.85]]]},"limit":5}
.\.venv\Scripts\python.exe -m unittest discover -s tests/unit -v
docker --context desktop-linux build --target test -f docker/api/Dockerfile -t lifemap-api-test:e1 .
docker --context desktop-linux run --rm --network none --memory 256m lifemap-api-test:e1 python -m pytest -q /app/tests/e1/test_core.py
docker --context desktop-linux exec lifemap-api-1 python /app/tests/e1/smoke_api.py
docker --context desktop-linux exec lifemap-spark-master-1 python3 /opt/lifemap/analytics/verify.py
```

`near` usa `$near`, `within` `$geoWithin` y `zones/summary` `$geoNear`.
Coordenadas finitas, radio maximo 50 km, limites 1-100; poligonos cerrados sin
cruces/agujeros, maximo 101 posiciones y extension 5 grados. Entradas invalidas
400/413/415; dependencias ausentes 503, errores internos sin secretos. API usa
usuario lector; `/predict` carga pesos/IDF reales con SHA256 y no inicia Spark.
`/health` informa API, Mongo, Spark, Dask y SHA desplegado.

## Jenkins, Git y demostracion

Jenkins es local y autenticado. Su socket Docker otorga control del motor:
no exponerlo a internet ni ejecutar Jenkinsfiles de terceros.

```powershell
.\.venv\Scripts\python.exe scripts\jenkins_e1.py configure
.\.venv\Scripts\python.exe scripts\jenkins_e1.py prepare
.\.venv\Scripts\python.exe scripts\jenkins_e1.py run
.\.venv\Scripts\python.exe scripts\jenkins_e1.py status
# Demostracion aislada del gate: no modificar main ni produccion para fallar.
.\.venv\Scripts\python.exe scripts\jenkins_e1.py run --fault
```

`prepare` requiere un commit en main; transporta un bundle de Git a Jenkins sin
push. Pipeline: checkout real -> imagenes secuenciales -> pytest/JUnit -> servicios
-> Kaggle/cache/carga aprobada -> modelos/resultados -> API candidata real -> promocion
local. Si falla pytest, no alcanza la promocion. Los artifacts conservan SHA,
resultados y estado; las credenciales se registran en Jenkins, nunca en Git.
`lifemap-main` apunta a origin/main con pollSCM cada dos minutos y queda deshabilitado
hasta primer push autorizado; el historial local no prueba aun un merge remoto.
Despues de autorizar push/servidor, seguir [DEPLOYMENT](docs/DEPLOYMENT.md) para
GitHub -> Jenkins -> aplicacion y la demostracion solicitada en vivo. No inventar
contribuciones de integrantes ni dar por realizada la sustentacion.

## Informe y evidencia

El informe final tiene seis paginas, renderizadas y revisadas. Su generador
es `scripts/report_e1.py`, ejecutado en la imagen auxiliar de
`docker/report/Dockerfile` (ReportLab/pypdf/PyMuPDF, locks fijados), sin agregar
un servicio Compose. Recibe `docs/reports` en `/evidence` de solo lectura y
un directorio de salida en `/out`. El JSON conserva conteos por cada regla,
metricas, 24 tiempos, filas por particion, recursos e historial del gate.
Las capturas corresponden a sus ejecuciones originales, no al tiempo presente.
Jenkins #4 SUCCESS y #5 FAILURE son la demostracion final del gate; #1 conserva
el error inicial de configuracion y #2/#3 las primeras pruebas. #5 conserva
la misma imagen API y no archiva artifacts de despliegues anteriores.
