# E2.5 — supervivencia descriptiva local

Se conservan los 16.944 negocios, sus reseñas y todos los campos E1.
`analytics/survival.py` calcula Kaplan-Meier sin nuevas dependencias.
`analytics/survival_job.py` solo lee los negocios y cuatro pares de limites de
reseñas para validar el origen. No reprocesa las 1.320.594 reseñas ni usa Spark
para una tabla pequeña. Solo escribe en `lifemap.survival_results` y en salidas
propias del volumen de datos. No crea endpoints ni modifica streaming.

## Tiempo, censura y entrada

El origen es la **primera reseña**, no la apertura. Para `is_open=0`, el evento
se aproxima por la ultima reseña. Para `is_open=1`, se usa la fecha de corte
administrativa almacenada en E1: **2022-01-19 19:48:45 UTC**, conforme al Word.
La ultima reseña y `business_days=last-first` siguen siendo los limites de
actividad registrada; no se modifican ni se confunden con el seguimiento
administrativo de un negocio marcado abierto al corte. Ningun abierto se
elimina o trata como cierre. Los resultados no describen el estado en 2026.

La auditoria encontro `entry_age_days=null` en los 16.944 negocios. No se puede
estimar exactamente la edad desde fundacion ni corregir su truncamiento sin
inventar historia. Se guardan dos analisis claramente separados:

- `observed_origin`: toda la muestra, reloj desde primera reseña, entrada cero
  en ese reloj observable. No afirma edad cero desde fundacion.
- `landmark_T`: seguimiento a partir de T=2019-01-01. Entrada conocida en el
  reloj observable `max(0,T-first_review)`, con salida `end-first_review`.
  Excluye cierres anteriores o iguales a T porque no estaban en riesgo despues
  de T; conserva todos los abiertos. Negocios observados por primera vez
  despues de T entran a edad observable cero. No inventa edad anterior a la
  primera reseña. Este analisis condicionado tiene otra poblacion en riesgo:
  sus medianas no son una comparacion causal con las curvas completas.

Se usa el producto `S(t)=producto(1-eventos/en_riesgo)`. Los intervalos con
entrada positiva son `(entrada,salida]`: quien entra al mismo instante que
un evento ajeno entra despues de ese evento. Los sujetos de la cohorte inicial
estan presentes en t=0. Eventos y censuras empatados comparten el conjunto
previo a las salidas. Las observaciones de duracion cero no se alteran con
fechas artificiales. La mediana es el primer tiempo con S<=0,5; si no ocurre,
se devuelve `null`, sin extrapolar ni llamarla vida infinita.

Minimo predefinido: **50 negocios por grupo**. Es una barrera practica contra
grupos diminutos, no una garantia de precision. Por debajo se guardan conteos,
pero no una curva o mediana interpretables. Cada punto incluye el numero en
riesgo; las colas con menos de 20 se marcan sin soporte suficiente para
interpretacion. La interpretacion KM supone censura/truncamiento no informativos,
algo que Yelp no permite verificar: cobertura, primera reseña y ultimo registro
pueden depender del negocio. Ni siquiera el analisis landmark recupera vida
desde apertura, y las diferencias entre zonas no establecen causalidad.

## Grupos y resultados reutilizables

La categoria principal es la ya fijada por E1, no una nueva reclasificacion.
Hay 210 categorias, 45 con tamaño suficiente. La comparacion destacada usa
American (New) y American (Traditional). Dos rectangulos WGS84 validos, definidos
en `config/survival.json`, seleccionan negocios mediante `$geoWithin`:

- Sector oeste: lon [-75,24; -75,17], lat [39,90; 40,02].
- Sector este: lon [-75,16; -75,09], lat [39,90; 40,02].

Son sectores de estudio, no barrios oficiales. No hay negocios compartidos
entre ambos. Se guardan 424 documentos por run/grupo/metodo, incluidos estados
de muestra insuficiente. Cada documento lleva conteos, mediana cuando existe,
pasos de curva con entradas/eventos/censuras/en riesgo y definiciones del reloj.
Los CSV `groups.csv` y `points.csv` sirven al futuro endpoint y Power BI; aun
no se implementa ninguno. El grafico HTML offline compara cuatro curvas.

**Son descriptivas retrospectivas y usan observaciones posteriores a T.**
Todos los documentos llevan `allowed_as_risk_predictor=false`. No deben entrar
en las variables del futuro modelo de riesgo: ese pipeline debera construirlas
solo con datos hasta T. Tampoco se usan estrellas del snapshot como predictor.

## Reproduccion sin nuevos servicios

Desde PowerShell, en LifeMap, con Docker local y secreto Mongo existente:

```powershell
$lifeMapMounts = @(
  '--mount', 'type=volume,src=lifemap_dataset_cache,dst=/data',
  '--mount', 'type=bind,src=D:/Proyects/LifeMap/analytics,dst=/app/analytics,readonly',
  '--mount', 'type=bind,src=D:/Proyects/LifeMap/config,dst=/app/config,readonly'
)
docker run --rm --user 185:0 --network lifemap_backend --memory 256m --cpus 0.5 @lifeMapMounts --mount type=bind,src=D:/Proyects/LifeMap/.secrets/mongo_writer_password.txt,dst=/run/secrets/mongo_writer_password,readonly --entrypoint python lifemap-dask:infra-v1 -m analytics.survival_job
# Usar la ruta curves.json que devuelve el job; no repetir el calculo para dibujar.
docker run --rm --user 185:0 --memory 384m --cpus 0.5 @lifeMapMounts --entrypoint python lifemap-dask:infra-v1 -m analytics.plot_survival /data/lifemap-e2/survival/b0f6c7f694b8bf5e/curves.json
.\.venv\Scripts\python.exe -m unittest discover -s tests/unit -p test_survival.py -v
```

UID 185 permite escribir solo en la salida del volumen creada por Spark en E2;
no se cambian permisos globales. El fingerprint identifica datos y configuracion;
la escritura por clave es idempotente. La evidencia resumida esta en
[e2-phase3.json](reports/e2-phase3.json); curvas completas y CSV permanecen fuera
de Git, en el volumen y `artifacts/survival-phase3`.

Se reutiliza el caso conocido de censura del ejercicio Discos y su criterio de
no inferir edad de fabrica/fundacion; se agrega entrada tardia explicita. Discos
solo se consulto. Referencias metodologicas:
[Surv: intervalos (inicio,fin]](https://www.stat.ethz.ch/R-manual/R-devel/library/survival/html/Surv.html),
[entrada tardia en Kaplan-Meier](https://lifelines.readthedocs.io/en/latest/Survival%20analysis%20with%20lifelines.html).
