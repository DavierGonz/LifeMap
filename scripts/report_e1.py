"""Informe E1: seis páginas desde evidencia JSON real; ejecutar en imagen report."""
import json
from pathlib import Path
from html import escape
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_LEFT
from reportlab.lib.pagesizes import A4
from reportlab.graphics.shapes import Drawing, Rect, String, Line
from pypdf import PdfReader
import fitz

E=Path('/evidence');OUT=Path('/out');OUT.mkdir(parents=True,exist_ok=True)
data=json.loads((E/'e1-evidence.json').read_text())
jenkins=json.loads((E/'e1-jenkins.json').read_text()) if (E/'e1-jenkins.json').exists() else {'builds':[]}
sent=data['sentiment'];a=data['analytics'];analysis=data['source_analysis']
blue=colors.HexColor('#15384C');teal=colors.HexColor('#168582');grey=colors.HexColor('#EDF2F5')
styles=getSampleStyleSheet()
styles.add(ParagraphStyle(name='TitleLM',fontName='Helvetica-Bold',fontSize=25,leading=29,textColor=blue,spaceAfter=15))
styles.add(ParagraphStyle(name='SubLM',fontName='Helvetica-Bold',fontSize=14,leading=18,textColor=teal,spaceBefore=11,spaceAfter=8))
styles.add(ParagraphStyle(name='BodyLM',fontName='Helvetica',fontSize=10,leading=14,spaceAfter=8))
styles.add(ParagraphStyle(name='SmallLM',fontName='Helvetica',fontSize=8,leading=10,spaceAfter=5))
styles.add(ParagraphStyle(name='CellLM',fontName='Helvetica',fontSize=8.5,leading=11))
story=[]
def p(s,style='BodyLM'):return Paragraph(s,styles[style])
def para(s):story.append(p(s))
def heading(n,title):
    if n>1:story.append(PageBreak())
    story.append(p(f'0{n} / {title}','TitleLM'))
def sub(s):story.append(p(s,'SubLM'))
def table(rows,widths=None):
    rows=[[p(escape(str(v)),'CellLM') for v in row] for row in rows]
    obj=Table(rows,colWidths=widths,repeatRows=1,hAlign='LEFT')
    obj.setStyle(TableStyle([('BACKGROUND',(0,0),(-1,0),grey),('TEXTCOLOR',(0,0),(-1,0),blue),('VALIGN',(0,0),(-1,-1),'TOP'),('BOTTOMPADDING',(0,0),(-1,-1),5),('TOPPADDING',(0,0),(-1,-1),5),('LINEBELOW',(0,0),(-1,0),.8,teal),('LINEBELOW',(0,1),(-1,-1),.25,colors.HexColor('#D6E0E5'))]))
    story.append(obj);story.append(Spacer(1,7))
def fmt(v):return f'{v:,.0f}'.replace(',','.')

heading(1,'LifeMap | Entrega 1')
para('<b>Informe técnico académico - 7 de noviembre de 2026</b><br/>Presupuesto: $0 COP. Alcance: ingesta, sentimiento, tasas de cierre y API local. No se implementa Entrega 2.')
table([['Población cargada','Reseñas clasificadas','Estado de entrega'],['16.944 negocios','1.320.594','Funcional local; despliegue externo pendiente']],[155,155,205])
sub('Arquitectura ejecutada')
d=Drawing(515,145)
boxes=[(0,84,'Kaggle / Yelp v4'),(177,84,'Dask: 2 workers'),(354,84,'MongoDB 8.0.16'),(0,9,'Jenkins / Git'),(177,9,'Spark 3.5.8'),(354,9,'Flask / modelo real')]
for x,y,label in boxes:
    d.add(Rect(x,y,160,48,fillColor=grey,strokeColor=teal,strokeWidth=.7,rx=5))
    d.add(String(x+80,y+20,label,textAnchor='middle',fontName='Helvetica-Bold',fontSize=10,fillColor=blue))
for x in [160,337]:d.add(Line(x,108,x+16,108,strokeColor=teal))
d.add(Line(434,84,257,57,strokeColor=teal));d.add(Line(337,33,353,33,strokeColor=teal));d.add(Line(80,57,80,82,strokeColor=teal))
story.append(d)
para('Compose propio <b>lifemap</b>: ocho contenedores, redes independientes y nueve volúmenes persistentes. MongoDB autenticado sin puerto host; Dask y Spark procesan dentro de Docker. API usa lector y el export del modelo; Jenkins realiza checkout, pruebas y promoción local. Conector MongoDB Spark 10.7.0.')
sub('Reproducibilidad y aislamiento')
para('Imagen base por digest y dependencias fijadas. Python del host: .venv 3.10.11, sin paquetes globales. Descarga v4 con manifiesto SHA256; Parquet, modelos y resultados reutilizables. Fuente, configuración y pipeline en Git local; credenciales, datasets y volúmenes excluidos. Geoespacial y Discos se consultaron sin alterar sus recursos.')
para('El README documenta arranque desde cero, secretos nuevos, builds secuenciales y comandos de procesamiento. Esta instalación local sí se probó; un arranque en otro equipo limpio todavía no se demostró. Puertos administrativos y API ligados a 127.0.0.1; no hay publicación externa.')
sub('Selección con autorización del usuario')
para('Restaurants, Filadelfia y entorno: rectángulo [-76.1, 39.3, -74.4, 40.6]. Supera el mínimo de un millón de reseñas únicas. Es un recorte exploratorio, no una delimitación metropolitana oficial. Sigue pendiente comunicarlo al profesor y comprobar exclusividad con otros equipos.')

heading(2,'Datos y vida observada')
para('Fuente: <b>yelp-dataset/yelp-dataset, versión 4</b>, actualización 17-03-2022. JSONL: negocio con 14 campos y reseña con 9. Claves: business_id y review_id; ubicación decimal, categorías, is_open, texto, estrellas y fecha. Corte máximo de reseñas: 19-01-2022 19:48:45; mínimo: 16-02-2005 03:23:22.')
sub('Limpieza cuantificada sobre la fuente completa')
business_rules=['JSON invalido','Sin coordenadas','Coordenadas no numericas','Coordenadas fuera de rango','Sin business_id','is_open invalido','Tipo categorias invalido','Duplicados']
review_rules=['JSON invalido','Sin review_id','Sin business_id','Texto vacio','Fecha invalida','Estrellas invalidas','business_id huerfano','Negocio descartado','Duplicados']
rows=[['Regla negocios','Antes / despues (ret.)','Regla resenas','Antes / despues (ret.)']]
for i in range(9):
    rows.append([business_rules[i] if i<8 else '-', '150.346 / 150.346 (0)' if i<8 else '-', review_rules[i], '6.990.280 / 6.990.280 (0)'])
table(rows,[145,110,145,115])
para('Filtro autorizado Restaurants + rectangulo: 150.346 negocios limpios a 16.944; 6.990.280 resenas limpias a 1.320.594 (5.669.686 fuera de alcance, no errores).')
para('Cada regla individual conserva los mismos conteos antes/después: no hubo descartes en esta fuente bajo las validaciones aplicadas. Esto no prueba ausencia de otros errores semánticos. El JSON de evidencia conserva todas las reglas, esquemas y huellas; fixtures introducen errores y duplicados para comprobar que sí se detectan.')
para('Coordenadas finitas y válidas evitan geometrías inválidas; IDs permiten joins estables; texto/fecha/estrellas válidos permiten NLP y vida observada. Deduplicación global por ID: primera fila válida en orden físico. Join por business_id: <b>0 huérfanas</b>. Dask lee bloques de 8 MiB (14 particiones de negocios, 636 de reseñas), usando ambos workers; caché compartida persistente y deduplicación SQLite en disco del coordinador.')
sub('Modelo MongoDB y aproximaciones explícitas')
para('businesses: _id=business_id, Point [longitud, latitud], índice 2dsphere; reviews: _id=review_id y referencia business_id indexada. Upserts ReplaceOne en lotes de 500; una carga completa verificada se reutiliza sin borrar el enriquecimiento. La idempotencia se comprobó con fixtures y reutilización de la carga real.')
para('<b>Días-negocio = (última reseña - primera reseña)/86.400 segundos.</b> Una fecha única aporta cero días. Evento = 1 si is_open=0; censura = 1 si is_open=1. No se prolonga la observación hasta el corte. Fechas civiles de Yelp sin zona conocida; apertura/cierre real y edad antes de la primera reseña son desconocidos. Las reseñas aproximan la vida, con sesgos de selección y actividad: no son historia completa de supervivencia.')

heading(3,'Sentimiento real con Spark')
para('Texto → minúsculas/limpieza [a-z] → tokenización → stopwords inglesas → máximo 256 tokens → HashingTF (8.192) → IDF → clasificador. IDF ajustado solo en train. Estrellas: 1-2 negativo, 3 neutral, 4-5 positivo; se eliminan de las características. División determinista por hash de business_id: <b>13.555 negocios train, 3.389 test; intersección cero</b>.')
dist={(r['split'],int(r['label'])):r['count'] for r in sent['distribution']}
table([['Clase','Train','Test','Total (%)']]+[[label,fmt(dist['train',i]),fmt(dist['test',i]),f"{fmt(dist['train',i]+dist['test',i])} ({100*(dist['train',i]+dist['test',i])/1320594:.1f}%)"] for i,label in enumerate(['Negativo','Neutral','Positivo'])],[140,100,100,175])
rows=[['Modelo / clase','Precision','Recall','F1']]
for name,label in [('naive_bayes','NB'),('logistic_regression','LR')]:
    model=sent['models'][name]
    for i,metrics in enumerate(model['per_class']):
        rows.append([label+' / '+['negativo','neutral','positivo'][i],f"{metrics['precision']:.4f}",f"{metrics['recall']:.4f}",f"{metrics['f1']:.4f}"])
    rows.append([label+' / macro F1','-', '-', f"{model['macro_f1']:.4f}"])
table(rows,[225,95,95,100])
sub('Matrices de confusión (filas reales / columnas predichas)')
matrix_rows=[['Modelo / real','Negativo','Neutral','Positivo']]
for name,label in [('naive_bayes','NB'),('logistic_regression','LR')]:
    matrix_rows.extend([[label+' / '+['neg.','neutral','pos.'][i]]+[fmt(n) for n in row] for i,row in enumerate(sent['models'][name]['confusion_matrix'])])
table(matrix_rows,[225,95,95,100])
para('Mayoría positiva (66,1%): accuracy favorece esa clase. NB: accuracy 0,7699, macro F1 0,6775; LR: 0,8287 y 0,6540. Se selecciona <b>Naive Bayes</b> por macro F1; neutral sigue siendo difícil. NB smoothing=1; LR 12 iteraciones, reg=0,1; sin búsqueda extensa. La comparación usa el mismo test, no una evaluación final independiente.',)
para('PipelineModel Spark y pesos/IDF guardados. Paridad Spark/API: 30 textos reales, mismas clases y probabilidades con tolerancia 10^-6. Las 1.320.594 reseñas tienen predicción persistida. La procedencia literal de SentimentStream queda pendiente: su fuente no fue suministrada; se implementó el flujo exigido, sin atribuir una reutilización inexistente.')

heading(4,'MapReduce y exposición')
para('Adaptación de Discos: emitir <b>(clave, (evento, días-negocio))</b> para categoría principal y geohash; sumar con reduceByKey. Una implementación independiente groupBy/sum de DataFrame verifica todas las claves, cierres y exposición (tolerancias 10^-10 relativas / 10^-6 absolutas para días).')
table([['Resultado verificado','Valor'],['Negocios / cierres observados',f"16.944 / {fmt(a['total_closures'])}"],['Exposición acumulada',f"{a['total_business_days']:,.3f} días-negocio"],['Agrupaciones','210 categorías derivadas / 390 celdas geohash'],['RDD frente a DataFrame','Coinciden ambas agregaciones'],['Tasa global descriptiva',f"{a['total_closures']/(a['total_business_days']/365)*100:.3f} cierres por 100 negocios-año"]],[270,245])
sub('Fórmula y umbral')
para('<b>Tasa (%) = cierres / (días-negocio / 365) × 100.</b> No es el porcentaje simple de negocios cerrados. Se exigen 3.650 días agregados (10 negocios-año) para comparar grupos, evitando dividir por exposición casi nula. Es una heurística operativa, no un intervalo de confianza; la tasa puede superar 100 y no se interpreta como probabilidad individual.')
sub('Grupos reales de ejemplo, ordenados por cierres')
rows=[['Categoría derivada','Negocios','Cierres','Días','Tasa anual']]
for r in data['closure_examples']['results']:
    rows.append([r['key'],fmt(r.get('businesses',0)),fmt(r['closures']),fmt(r['business_days']),f"{r['rate_pct']:.3f}"])
table(rows,[165,75,75,110,90])
para('Categoría principal reproducible: primer tag alfabético excluyendo Restaurants, Food y Nightlife, o Restaurants como fallback. Yelp no declara una categoría principal oficial; esta decisión cambia los grupos, no la población de restaurantes. Geohash de precisión 5, con polígono de celda guardado.')
sub('Persistencia y reputación geográfica')
para('Colección closure_stats: clave, cierres, días, negocios, tasa, run_id y, por celda, geometría y porcentaje de reseñas negativas. businesses conserva resumen de sentimiento y geohash; reviews conserva clase, probabilidades y modelo. Una zona combina estas estadísticas con $geoNear y exposición real. El porcentaje negativo usa predicciones del modelo seleccionado, no se confunde con los días-negocio.')
para('Limitaciones: is_open es un estado del dataset y no una fecha de cierre; primera/última reseña dependen de actividad. Agrupaciones con exposición pequeña o categoría derivada requieren cautela. No se hicieron inferencias causales, Kaplan-Meier ni predicción de cierre (E2).')

heading(5,'Particiones y recursos')
para('24 mediciones reales: 2, 8, 32 y 200 particiones; reparto por ciudad frente a round-robin sin clave; tres repeticiones por caso. Una pareja de reducciones sin medir calienta cada configuración. Se materializan los 16.944 negocios antes del cronómetro; se incluyen repartición, dos reduceByKey, collect y comprobación DataFrame. Spark 3.5.8, un worker y un núcleo.')
rows=[['N / reparto','T1 / T2 / T3 (s)','Media','Mediana','DE muestral']]
for row in a['benchmark_summary']:
    times=[r['seconds'] for r in a['benchmark_times'] if r['partitions']==row['partitions'] and r['mode']==row['mode']]
    rows.append([str(row['partitions'])+' / '+('ciudad' if row['mode']=='city' else 'sin clave'),' / '.join(f'{t:.2f}' for t in times),f"{row['mean_seconds']:.2f}",f"{row['median_seconds']:.2f}",f"{row['stdev_seconds']:.3f}"])
table(rows,[110,190,70,70,75])
sub('Sesgo observado con spark_partition_id()')
table([['N','Ciudad: mín./máx. filas','Vacías','Sin clave: mín./máx.']]+[[n,f"{c['min_partition_rows']} / {c['max_partition_rows']}",c['empty_partitions'],f"{r['min_partition_rows']} / {r['max_partition_rows']}"] for n in [2,8,32,200] for c in a['benchmark_summary'] if c['partitions']==n and c['mode']=='city' for r in a['benchmark_summary'] if r['partitions']==n and r['mode']=='round_robin'],[65,190,70,190])
para('Con 200 particiones, ciudad concentra hasta 5.854 filas y deja 19 vacías; sin clave distribuye 84-85 filas. Un reparto equilibrado no garantiza menos tiempo: en este equipo pequeño el costo de tareas domina. Dos particiones son más rápidas; no se extrapola escalabilidad a millones de negocios ni a varios nodos físicos.')
sub('Memoria medida y recuperación')
successful=[r for r in data['resources']['spark_jobs'] if r['returncode']==0]
para('La primera publicación se detuvo de forma segura a 0,468 GiB libres en Windows (96% de carga), sin OOM ni borrar datos. El usuario liberó memoria manualmente y se reanudó desde Parquet/modelo, sin reentrenar. Publicación final: mínimo 1,651 GiB libres, pico CPU Windows 33,7%; analytics: mínimo 1,714 GiB, pico 24,9%. Son muestras del host, no CPU exclusiva del job.')
para('Límites por contenedor suman 6.625 GiB; Windows/WSL comparten memoria y no se contabilizan dos veces. Procesos intensivos se ejecutan secuencialmente; no se cambiaron ajustes globales. Consumo al cierre y muestras por contenedor se conservan en e1-evidence.json. La caché original tuvo que descargarse de nuevo una vez tras detectar un montaje faltante; fue corregido y la reutilización posterior sí está verificada.')

heading(6,'API, CI y aceptación')
table([['Endpoint','Comprobación real'],['GET /health','API, Mongo, Spark, Dask y commit real'],['GET /businesses/near','$near, categoría/radio/límite variables'],['POST /businesses/within','$geoWithin, polígono validado'],['GET /zones/summary','$geoNear, sentimiento y tasa por zona'],['GET /stats/closure','Categoría/geohash y exposición mínima'],['POST /predict','Modelo NB real; probabilidades de texto']],[230,285])
para('Seis endpoints probados contra la población real; entradas inválidas devuelven 400. Límite 100 filas, radio 50 km y consultas Mongo con timeout; errores de dependencia 503 sin secretos. API usa solo lector; modelo exportado validado SHA256 y equivalente al de Spark.')
sub('Evidencia de pruebas y Jenkins')
builds=jenkins.get('builds',[])
summary=', '.join(f"#{r['number']} {r.get('result') or 'en curso'}" for r in sorted(builds,key=lambda r:r['number'])) or 'Pendiente de ejecución; no acreditado'
para('Pruebas: 21 unittest locales, 13 pytest Docker, fixtures distribuidos en ambos workers con GeoJSON/idempotencia, paridad de 30 textos, igualdad RDD/DataFrame y 24 benchmarks. Historial local Jenkins: '+escape(summary)+'. Detalle de parámetros, JUnit y artifacts en e1-jenkins.json.')
gate=data.get('jenkins_gate')
gate_text=(f"Build #{gate['successful_build']} exitoso; #{gate['failed_build']} falla intencionalmente en pytest, omite despliegue y conserva exactamente la imagen anterior de la API." if gate else 'La prueba de bloqueo del despliegue sigue pendiente.')
para('Jenkins usa checkout de Git bundle local, builds secuenciales, pytest, infraestructura, caché/ingesta, resultados académicos, API candidata y promoción local. '+gate_text+' Credencial Kaggle registrada en Jenkins. GitHub origin conserva visibilidad pública, sin push; pollSCM cada dos minutos preparado y job remoto deshabilitado hasta autorización.')
sub('Aceptación: no confundir local con público')
para('<b>E1.1:</b> Compose/README y salud local; falta equipo limpio externo. <b>E1.2:</b> pendiente servidor distinto y URL pública. <b>E1.3:</b> pipeline/gate locales; falta merge remoto y disparo por main. <b>E1.4-E1.5:</b> ingesta/datos/vida comprobados. <b>E1.6:</b> modelos y métricas reales; falta verificar reutilización literal de SentimentStream. <b>E1.7-E1.8:</b> agregación y benchmark completos. <b>E1.9:</b> seis rutas locales, falta URL desplegada. <b>E1.10:</b> informe/README; sustentación y cambio remoto en vivo pendientes.')
para('Pendientes externos: segundo equipo gratuito autorizado, disponibilidad pública, primer push autorizado y profesor (exclusividad de selección). Procedimiento sin costo en DEPLOYMENT.md; ningún servicio contratado ni administrativo expuesto. No se inventan autores/aportes. Power BI queda para E2; Kafka, streaming, supervivencia y riesgo no implementados.')
story.append(p('Fuentes: Word oficial LifeMap (Insumos), §§4, 6 y 7, rúbrica E1.1-E1.10; fecha de entrega actualizada por el usuario. Yelp/Kaggle: https://www.kaggle.com/datasets/yelp-dataset/yelp-dataset (v4). Adaptaciones: docs/REUSE.md; Spark HashingTF/MurmurHash3, código Apache Spark 3.5.8. Evidencia reproducible: docs/reports/e1-evidence.json y e1-jenkins.json.','SmallLM'))

def footer(canvas,doc):
    canvas.saveState();canvas.setStrokeColor(teal);canvas.line(40,39,555,39)
    canvas.setFont('Helvetica',8);canvas.setFillColor(blue)
    canvas.drawString(40,26,'LIFEMAP  /  E1  /  RESULTADOS LOCALES VERIFICADOS')
    canvas.drawRightString(555,26,f'{doc.page} / 6');canvas.restoreState()

target=OUT/'LifeMap-Entrega1.pdf'
doc=SimpleDocTemplate(str(target),pagesize=A4,rightMargin=40,leftMargin=40,topMargin=40,bottomMargin=50,title='LifeMap - Entrega 1',author='LifeMap - evidencia técnica local')
doc.build(story,onFirstPage=footer,onLaterPages=footer)
reader=PdfReader(str(target));assert len(reader.pages)==6,f'Expected 6 pages, got {len(reader.pages)}'
pdf=fitz.open(target)
for i,page in enumerate(pdf):page.get_pixmap(matrix=fitz.Matrix(1.25,1.25)).save(OUT/f'page-{i+1}.png')
print(json.dumps({'pdf':str(target),'pages':len(reader.pages),'rendered_pages':len(pdf)}))
