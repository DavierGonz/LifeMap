# LifeMap ? Entrega 1

Proyecto universitario: Yelp ? Dask ? MongoDB ? Spark MLlib/MapReduce ? Flask.
Presupuesto $0 COP. Entrega 2 fuera de alcance. Referencias Geoespacial y Discos
consultadas sin modificar sus recursos. C?digo adaptado y l?mites: [REUSE](docs/REUSE.md).

## Resultado y alcance

Yelp `yelp-dataset/yelp-dataset`, versi?n **4**: 150.346 negocios y 6.990.280
rese?as inspeccionadas. Selecci?n autorizada por el usuario: Restaurants,
rect?ngulo Filadelfia/entorno `[-76.1,39.3,-74.4,40.6]`: **16.944 negocios y
1.320.594 rese?as** cargados. No es un l?mite metropolitano oficial. Falta
notificaci?n/exclusividad ante el profesor; no se afirma aprobaci?n docente.

Entrenamiento por negocio (13.555 train / 3.389 test, intersecci?n cero), Naive
Bayes y regresi?n log?stica. NB seleccionado por macro F1 (0,6775 frente a
0,6540); modelo Spark guardado e inferencia Flask equivalente comprobada.
RDD y DataFrame coinciden; benchmark real 2/8/32/200 particiones, dos repartos,
tres repeticiones. Evidencias finales: `docs/reports/e1-evidence.json`,
`docs/reports/e1-jenkins.json` e informe `docs/reports/LifeMap-Entrega1.pdf`.

**No se cumple a?n el despliegue externo:** faltan otro computador, URL p?blica,
primer push autorizado y demostraci?n GitHub ? Jenkins ? URL. El repositorio
origin conserva la visibilidad p?blica elegida por el usuario, sin push.
La sustentaci?n y la reutilizaci?n verificable de SentimentStream tambi?n
requieren atenci?n: su c?digo no est? en las referencias suministradas.

## Arranque desde cero (Windows / Docker Desktop + WSL2)

Requisitos: Git, Python 3.10 y Docker Desktop Linux ya instalados; unos 30 GB
libres para cach?, Parquet, modelos e im?genes. En Linux pueden emplearse los
mismos archivos Compose; los lanzadores host est?n preparados para Windows.
No usar recursos ni credenciales de otros proyectos.

```powershell
Set-Location D:\Proyects\LifeMap
# Solo si .venv todav?a no existe:
py -3.10 -m venv .venv
# Desde este punto toda ejecuci?n Python Windows usa este int?rprete.
.\.venv\Scripts\python.exe scripts\check_environment.py
```

Crear `.secrets` **solo en un equipo nuevo** y protegerla antes de generar
credenciales. No repetir sobre la instalaci?n actual ni sobrescribir secretos:

```powershell
New-Item -ItemType Directory -Path .secrets
$lifeMapIdentity = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
icacls .secrets /inheritance:r /grant:r "${lifeMapIdentity}:(OI)(CI)F" "SYSTEM:(OI)(CI)F"
.\.venv\Scripts\python.exe scripts\prepare_local_env.py
```

El generador crea contrase?as exclusivas y `.env`; se niega a sobrescribirlas.
Guardar un token Kaggle personal en `.secrets/kaggle-token.txt` o el formato
legacy en `.secrets/kaggle.json`. Aceptar los t?rminos de Yelp/Kaggle cuando
corresponda; no pegar secretos en terminal/chat. `.env`, `.secrets`, `.venv`,
datasets, modelos y vol?menes est?n excluidos de Git y del contexto de build.
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
red backend interna y vol?menes `lifemap_*` independientes. Host **solo loopback**:
API 5100, Spark UI 8082, Dask 8788, Jenkins 8083. Mongo 8.0.16, Spark 3.5.8,
conector Mongo Spark 10.7.0; dependencias exactas/digests en Dockerfiles/locks.
RAM m?xima por contenedor: Mongo 768 MiB; master Spark 1.024; worker 1.536;
scheduler Dask 384; cada worker Dask 1.024; API 256; Jenkins 768.
Total de l?mites 6.625 GiB, CPU sumada 7; no es consumo reservado ni capacidad
para ejecutar trabajos pesados simult?neamente. Equipo 16 GB, WSL comparte RAM
con Windows: no sumar WSL otra vez. Medir RAM/CPU y ejecutar trabajos por turnos;
los lanzadores abortan su propio job ante presi?n cr?tica sostenida/OOM.
No cerrar aplicaciones ajenas, hacer prune o cambiar Docker/WSL globalmente.

## Datos, entrenamiento y resultados reproducibles

```powershell
.\.venv\Scripts\python.exe scripts\yelp.py download
.\.venv\Scripts\python.exe scripts\yelp.py analyze
.\.venv\Scripts\python.exe scripts\yelp.py load --approved-scope config\yelp_selection.json
# Para trabajos grandes en 16 GB, detener ?nicamente Dask y API temporalmente:
docker --context desktop-linux compose --project-name lifemap stop api dask-worker-1 dask-worker-2 dask-scheduler
.\.venv\Scripts\python.exe scripts\spark_e1.py train
.\.venv\Scripts\python.exe scripts\spark_e1.py analytics
docker --context desktop-linux compose --project-name lifemap up -d --no-build --wait
```

Descarga fijada a v4; cach? validada SHA256 en volumen `lifemap_dataset_cache`,
reutilizada sin redescarga. Dask usa ambos workers, particiones 8 MiB y lotes
Mongo de 500 upserts; los conteos no dependen del `review_count` declarado.
Una carga completa verificada no sobrescribe enriquecimientos al repetirse.
No usar `down -v` ni borrar cach?/modelos. Reejecuciones Spark completas reutilizan
sus manifiestos; una ejecuci?n incompleta conserva sus archivos para diagn?stico.

Vida observada = (?ltima ? primera rese?a)/86.400 segundos, cero para una sola
fecha; evento `is_open=0`, censura `is_open=1`. No son fechas reales de apertura
ni cierre; no extender artificialmente hasta el corte. Etiquetas por estrellas
solo como objetivo, nunca como predictor. IDF solo train; comparaci?n de dos
modelos en un ?nico test no equivale a una estimaci?n final independiente.
Tasa anualizada = cierres/(d?as-negocio/365)*100; m?nimo 3.650 d?as agregados,
heur?stica de exposici?n y no intervalo de confianza. Categor?a principal:
primer tag alfab?tico distinto de Restaurants/Food/Nightlife, fallback Restaurants;
Yelp no aporta una categor?a principal oficial. Geohash precisi?n 5.

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
Coordenadas finitas, radio m?ximo 50 km, l?mites 1?100; pol?gonos cerrados sin
cruces/agujeros, m?ximo 101 posiciones y extensi?n 5 grados. Entradas inv?lidas
400/413/415; dependencias ausentes 503, errores internos sin secretos. API usa
usuario lector; `/predict` carga pesos/IDF reales con SHA256 y no inicia Spark.
`/health` informa API, Mongo, Spark, Dask y SHA desplegado.

## Jenkins, Git y demostraci?n

Jenkins es local y autenticado. Su socket Docker otorga control del motor:
no exponerlo a internet ni ejecutar Jenkinsfiles de terceros.

```powershell
.\.venv\Scripts\python.exe scripts\jenkins_e1.py configure
.\.venv\Scripts\python.exe scripts\jenkins_e1.py prepare
.\.venv\Scripts\python.exe scripts\jenkins_e1.py run
.\.venv\Scripts\python.exe scripts\jenkins_e1.py status
# Demostraci?n aislada del gate: no modificar main ni producci?n para fallar.
.\.venv\Scripts\python.exe scripts\jenkins_e1.py run --fault
```

`prepare` requiere un commit en main; transporta un bundle de Git a Jenkins sin
push. Pipeline: checkout real ? im?genes secuenciales ? pytest/JUnit ? servicios
? Kaggle/cach?/carga aprobada ? modelos/resultados ? API candidata real ? promoci?n
local. Si falla pytest, no alcanza la promoci?n. Los artifacts conservan SHA,
resultados y estado; las credenciales se registran en Jenkins, nunca en Git.
`lifemap-main` apunta a origin/main con pollSCM cada dos minutos y queda deshabilitado
hasta primer push autorizado; el historial local no prueba a?n un merge remoto.
Despu?s de autorizar push/servidor, seguir [DEPLOYMENT](docs/DEPLOYMENT.md) para
GitHub ? Jenkins ? aplicaci?n y la demostraci?n solicitada en vivo. No inventar
contribuciones de integrantes ni dar por realizada la sustentaci?n.
