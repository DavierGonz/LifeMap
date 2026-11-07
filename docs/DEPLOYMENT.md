# Despliegue y demostracion de Entrega 1

Estado externo: no se ha identificado otro computador autorizado ni URL publica.
El servicio local no acredita E1.2. No se ha hecho push, merge remoto o
despliegue publico. El repositorio https://github.com/DavierGonz/LifeMap esta
publico por decision del usuario; todavia no contiene estos commits locales.

## Segundo equipo sin gasto

Usar un equipo propio/prestado/de laboratorio con permiso de su administrador,
Docker/Compose disponibles y capacidad medida para las cargas secuenciales.
No se propone contratar hosting, activar facturacion o publicar MongoDB,
Jenkins o las consolas Spark/Dask. La disponibilidad de red publica y su
permiso son requisitos pendientes, no infraestructura que ya exista.

Tras autorizacion expresa de publicacion, transferir el commit exacto mediante
el repositorio o un bundle Git. Crear secretos exclusivos para ese servidor,
proporcionar Kaggle por archivo privado o Jenkins Credentials y ejecutar el
README. No copiar los secretos ni volumenes de Geoespacial/Discos. Se puede
reutilizar la cache Yelp validada por sus hashes para evitar otra descarga.

Exponer exclusivamente Flask mediante el proxy HTTPS institucional existente
o una regla de red autorizada para ese servidor. La configuracion actual liga
todos los puertos al loopback; no se cambiara automaticamente. El equipo
administrador debe definir URL, TLS/red y ventana de disponibilidad antes de
habilitar la regla correspondiente. No hay afirmacion de que ese acceso exista.

## GitHub a Jenkins y aplicacion

`lifemap-local-e1` comprueba un commit local real mediante bundle, ejecuta
pruebas, usa datos/modelos reales y promueve la API solo despues de los gates.
El job es una demostracion local; no es un merge o webhook remoto.

`lifemap-main` queda configurado con GitHub, rama main, Jenkinsfile y pollSCM
cada dos minutos. Permanece deshabilitado hasta el primer push autorizado;
el remoto vacio no permite demostrar checkout o disparo por merge. Al disponer
del codigo remoto, habilitarlo y comprobar su checkout antes de la sustentacion.
Jenkins administrativo puede permanecer privado: pollSCM no requiere webhook
publico. No se usan credenciales de otros proyectos.

Para la modificacion solicitada en vivo, realizarla en una rama propia, probar,
crear commit con autoria real y, tras autorizacion, integrar a main/publicar.
Observar en Jenkins el mismo SHA, el resultado de pytest, las pruebas contra
la API candidata y la promocion. Confirmar /health y etiqueta OCI de imagen
con ese SHA desde la URL del segundo equipo. Repetir con un fallo intencional
para mostrar que la API anterior y su SHA permanecen intactos.

El pipeline suministrado despliega localmente. En el segundo equipo debe
ejecutarse Jenkins con su motor Docker local y los secretos de ese equipo,
siguiendo el mismo procedimiento. No se habilita SSH remoto o exposicion de
administracion sin conocer y autorizar el servidor. La demostracion remota y
la sustentacion de cada integrante siguen pendientes; no se inventan aportes.
