# Dependencias de LifeMap

El host no necesita paquetes de aplicacion en las fases de preparacion y
validacion estatica de infraestructura. `host.in` y
`host.lock.txt` representan deliberadamente un conjunto vacio. No se han
instalado paquetes adicionales ni actualizado las herramientas incluidas
por Python.

`bootstrap.snapshot.txt` registra las versiones de pip y setuptools incluidas
por `py -3.10 -m venv .venv` con CPython 3.10.11. Es un inventario del
bootstrap, separado del lock de aplicacion. Otra distribucion de Python 3.10
podria incluir versiones distintas; debe registrarse y revisarse esa diferencia.

## Uso local

Todos los comandos se ejecutan desde `D:\Proyects\LifeMap` mediante
`D:\Proyects\LifeMap\.venv\Scripts\python.exe`. No utilizar pip global.

Cuando se autorice agregar dependencias, se fijaran las versiones directas
y transitivas en un lock, con hashes y validacion bajo Windows/Python 3.10.
No se generara el lock del host copiando un entorno de otro proyecto.

Los locks `api.lock.txt`, `mongo.lock.txt` y `dask.lock.txt` conservan las
versiones fijadas del proyecto Geoespacial, leido sin modificarlo. En API se
adapto la inclusion a `mongo.lock.txt`. `spark.lock.txt` incluye solamente el
cliente MongoDB; PySpark y MLlib vienen en la imagen Spark fijada.

No se ejecuto pip-compile ni una nueva resolucion de paquetes. Los encabezados
de los locks copiados describen su generacion en el proyecto fuente. La
construccion futura debe verificar que todas esas versiones siguen disponibles
y funcionan en las imagenes; la validacion estatica no lo demuestra.

No se instalaron estos locks en el host ni se descargaron paquetes. No se
agrego pytest a imagenes runtime. Las pruebas pequenas usan unittest.
Los locks de supervivencia y otras dependencias nuevas quedan pendientes.

El instalador del conector MongoDB descarga cuatro JAR de versiones fijas y
verifica el SHA-1 publicado por Maven durante un futuro build. Hereda ese
control del proyecto fuente; no equivale a un lock SHA-256 independiente.
No se ha ejecutado el instalador. Las imagenes base se fijan por version y digest.

El archivo local `.venv\pip.ini` exige un entorno virtual y desactiva la
comprobacion automatica de actualizaciones de pip. Este archivo se pierde
al borrar `.venv`; al reconstruir el entorno se debe restaurar su contenido
segun el README principal. No modifica la configuracion global de pip.
