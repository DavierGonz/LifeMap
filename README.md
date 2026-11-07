# LifeMap - Entrega 1

Validacion E1.3: actualizacion de documentacion detectada por pollSCM.

Proyecto universitario: Yelp -> Dask -> MongoDB -> Spark MLlib/MapReduce -> Flask.
La ruta `/` abre el explorador visual con Leaflet/OpenStreetMap, filtros,
negocios reales, tasas de cierre y clasificador de resenas. `/health` conserva
el estado JSON de la API. No requiere Node ni un servicio frontend adicional.
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
deploy/azure/deployment-evidence.json. Repositorio publico:
https://github.com/DavierGonz/LifeMap. El flujo de actualizacion es
GitHub main -> pollSCM cada dos minutos -> Jenkins -> API candidata -> Azure.
La sustentacion y la reutilizacion verificable de SentimentStream tambien
requieren atencion: su codigo no esta en las referencias suministradas.

Servidor Azure verificado: Ubuntu 24.04 x64, 20.230.240.88. La documentacion
de [deploy/azure](deploy/azure/README.md) conserva el procedimiento de migracion;
no es necesario repetirlo para actualizar la API con Jenkins.

## Arranque desde cero (Windows / Docker Desktop + WSL2)

Requisitos: Git, Python 3.10 y Docker Desktop Linux ya instalados; unos 30 GB
libres para cache, Parquet, modelos e imagenes. En Linux pueden emplearse los
mismos archivos Compose; los lanzadores host estan preparados para Windows.
No usar recursos ni credenciales de otros proyectos.

Obtener el codigo con
`git clone --branch main https://github.com/DavierGonz/LifeMap.git D:\Proyects\LifeMap`.
El repositorio no incluye secretos, datasets, dumps ni modelos entrenados.
Para una instalacion nueva, ejecutar las etapas de datos descritas abajo o
restaurar una copia privada autorizada con el procedimiento de deploy/azure.

```powershell
Set-Location D:\Proyects\LifeMap
# Solo si .venv todavia no existe:
py -3.10 -m venv .venv
@'
[global]
require-virtualenv = true
disable-pip-version-check = true
'@ | Set-Content -LiteralPath .venv\pip.ini -Encoding ascii
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

Jenkins Azure es privado en localhost:8083. Administracion por tunel SSH,
credenciales nuevas fuera de Git y cliente `.venv/Scripts/python.exe
scripts/jenkins_azure.py`. Instrucciones: [Jenkins Azure](deploy/azure/JENKINS.md).
No ejecutar los antiguos lanzadores de CI Windows contra el servidor.

Pipeline Azure: checkout Git -> build API secuencial -> 18 pytest -> validar
infraestructura/resultados existentes -> credencial Kaggle (solo metadata)
-> candidata privada -> promocion publica condicionada a todos los gates.
No repite ingesta, entrenamiento ni benchmark. La promocion usa Caddy y una
candidata temporal para servir mientras se actualiza la API principal.

Preflight Azure #1 SUCCESS; #2/#3 FAILURE intencional, promocion bloqueada e
imagen publica intacta. Credencial Kaggle registrada y enmascarada por Jenkins.
GitHub es publico: lectura SCM sin PAT adicional. `lifemap-main` apunta a main
con pollSCM cada dos minutos. Su activacion requiere publicar primero main y
establecer el checkout inicial; los merges posteriores se detectan por sondeo.
La prueba automatica se distingue del preflight: debe registrar SCMTrigger,
el commit de GitHub, pytest/JUnit y el mismo SHA en /health publico.
Cambio minimo de demostracion: deploy/azure/pollscm-demo.patch.

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
