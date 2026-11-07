# Preparacion local de LifeMap

Estado: autorizada por el usuario y preparada localmente.

El presupuesto es $0 COP. Se utiliza exclusivamente el entorno
`D:\Proyects\LifeMap\.venv` con Python 3.10 para ejecucion Python local.
La creacion autorizada mediante `py -3.10 -m venv .venv` es el bootstrap;
todos los comandos posteriores usan el interprete del entorno.

No se requieren dependencias de aplicacion en esta fase. Se conservan las
herramientas incluidas por venv sin actualizar ni descargar paquetes.
Los locks de imagenes Docker se prepararan por componente cuando se autorice
su implementacion; no se copian los entornos de proyectos anteriores.

Se preparo estructura sin codigo de dominio, servicios ni simulaciones de
funcionalidad terminada. No se inicializo Git ni se realizaron commits.

Los pendientes de servidor, URL, Power BI y estudio permanecen visibles.
La falta de servidor no bloquea trabajo local independiente autorizado.
Publicar y demostrar el sistema remoto sigue siendo requisito obligatorio.

La configuracion del editor selecciona el entorno; la tarea de verificacion
usa su ruta explicita para evitar depender del interprete predeterminado global.
La comprobacion no escribe resultados ni consulta los proyectos protegidos.
