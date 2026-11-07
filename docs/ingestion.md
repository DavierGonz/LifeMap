# Ingesta Yelp con Dask

Fuente: [Yelp Dataset en Kaggle](https://www.kaggle.com/datasets/yelp-dataset/yelp-dataset),
version 4, actualizada el 17 de marzo de 2022. Uso academico y atribucion a Yelp;
el ZIP original conserva Dataset_User_Agreement.pdf. Se usa la API REST, sin
instalar Kaggle ni dependencias en Windows. Descarga/cache y tareas acotadas
adaptan `ParcialBigData/ingestion/download.py`, `etl.py` y `run.py`, consultados
sin modificar el proyecto original. No se traslado logica US Accidents.

## Ejecucion reproducible

Desde D:\Proyects\LifeMap, construir la imagen Dask cuando cambie el codigo y
levantar solamente los servicios existentes necesarios:

```powershell
docker --context desktop-linux compose --project-name lifemap --env-file .env build dask-scheduler
docker --context desktop-linux compose --project-name lifemap --env-file .env up -d --no-build mongodb dask-scheduler dask-worker-1 dask-worker-2
.\.venv\Scripts\python.exe scripts\yelp.py download
.\.venv\Scripts\python.exe scripts\yelp.py test
.\.venv\Scripts\python.exe scripts\yelp.py analyze
```

El lanzador requiere .venv Python 3.10; toda lectura/limpieza real se ejecuta
en Docker. La limpieza se reparte entre los dos workers, con bloques de 8 MiB
y como maximo dos particiones en vuelo. El coordinador recibe rutas y conteos.
Los modulos son invocables desde el futuro pipeline Jenkins; no se ha creado
todavia ese pipeline ni se acredita su integracion por ejecutar el lanzador.

La credencial se guarda en `.secrets/kaggle-token.txt` o `.secrets/kaggle.json`.
El lanzador la transmite por stdin, no como argumento, variable Docker, imagen
o archivo versionado. La API publica tambien puede admitir acceso anonimo.
La cabecera Authorization se retira al redirigir a almacenamiento.

La cache persiste en `lifemap_dataset_cache:/data/yelp-v4`, compartida por
scheduler y workers. Conserva ZIP completo, los JSONL de negocios/resenas,
manifiesto con version/tamanos/SHA-256, esquema, particiones preparadas y
analisis. Una cache alterada falla sin sobrescribirse o descargarse de nuevo.
Las transferencias interrumpidas conservan `.part` y pueden reanudarse si el
servidor acepta el rango. Recrear contenedores no debe retirar este volumen.

## Reglas y conteos

El analisis guarda por regla antes, descartados y despues. Se aplican en orden
y con descartes mutuamente excluyentes: JSON invalido; coordenadas ausentes,
no numericas/no finitas o fuera de rango; IDs esenciales ausentes; is_open
invalido; categorias con tipo inesperado. (0,0) y limites geograficos son validos.
Categorias nulas se conservan como lista vacia, sin asignarles una familia.

En resenas: IDs ausentes, texto vacio, fecha invalida, estrellas fuera de 1..5,
negocio inexistente o retirado por limpieza. No se confunden estos dos ultimos
motivos. Se conservan textos originales salvo espacios exteriores: la limpieza
linguistica de sentimiento pertenece a otra tarea. No se infieren zonas horarias.

La deduplicacion es global por business_id/review_id, no solo por particion.
Gana la primera fila valida en orden fisico del archivo. Un indice SQLite
temporal en disco del coordinador evita guardar millones de IDs en RAM; no es
un servicio adicional. Las resenas unicas quedan en Parquet por particion.
El cruce por business_id y las reglas se ejecutan en los workers; la reduccion
global y los contadores se consolidan en el coordinador.

Las cantidades por familia suman resenas reales limpias/unicas relacionadas,
no el review_count declarado en business.json. Una union de categorias cuenta
cada negocio una vez; las familias superpuestas se comparan por separado.
Las areas candidatas son rectangulos exploratorios definidos en config,
no limites administrativos oficiales ni seleccion docente aprobada.

## MongoDB y vida observada

`businesses`: `_id=business_id`, location Point [longitud,latitud], indice
2dsphere, categorias y primera/ultima resena, conteo observado, dias-negocio,
evento, censura, corte y huella de fuente. `reviews`: `_id=review_id`,
business_id y fecha indexados, texto, estrellas y huella. ReplaceOne/upsert,
lotes de 500 y claves estables permiten repetir cargas parciales sin duplicar.
Solo se monta la credencial writer de LifeMap en Dask, nunca la de administrador.

Los dias-negocio son (ultima-primera).total_seconds()/86400: cero para una
sola fecha y null si no hay resenas observables. No se agrega artificialmente
un dia. is_open=0 determina evento; is_open=1 determina censura. El corte es
la maxima fecha interpretable del archivo completo. Las fechas civiles de
Yelp no traen zona; BSON Date se usa como representacion de esos valores,
sin afirmar una zona horaria local real. No se prolonga la vida hasta el corte.
Apertura, cierre exacto y edad previa a primera resena son desconocidos;
entry_age_days queda null. Esto no resuelve aun la entrada tardia de Entrega 2.

Los fixtures usan solamente `__yelp_ingestion_test_*`. No escriben Yelp real
en colecciones definitivas. El comando load exige un archivo de seleccion con
estado approved_by_user, misma version/huella y al menos 1.000.000 de resenas
verificadas. `config/yelp_selection.example.json` permanece sin aprobar.
No crear una seleccion aprobada automaticamente ni cargar antes de decidirla.

## Estado vigente de la selecci?n

El usuario autoriz? config/yelp_selection.json: Filadelfia/entorno, Restaurants.
Se cargaron 16.944 negocios y 1.320.594 rese?as. Comunicaci?n/exclusividad
ante el profesor siguen pendientes. Una carga completa con conteos verificados
reutiliza su manifiesto y conserva los enriquecimientos de sentimiento/anal?tica.
