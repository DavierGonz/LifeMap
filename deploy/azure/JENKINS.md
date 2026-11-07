# E1.3 - GitHub y Jenkins Azure

Jenkins: localhost:8083 en la VM. No abrir su NSG ni crear webhook publico.
Administracion mediante tunel SSH a 127.0.0.1:18083 del equipo local:

```powershell
ssh -N -o ExitOnForwardFailure=yes -i C:\Users\User\.ssh\vm-lifemap-e1_key.pem -L 127.0.0.1:18083:127.0.0.1:8083 azureuser@20.230.240.88
```

La llave solo se utiliza como identidad; no se copia a Jenkins. Cliente local:
`.venv\Scripts\python.exe scripts\jenkins_azure.py configure|prepare|run|status`.
Admin API token en .secrets/jenkins-azure-admin.json, excluido de Git.
Kaggle se registra como Secret Text lifemap-kaggle (cifrado por Jenkins),
se inyecta solo en su etapa y no se guarda en artifacts ni en el workspace.
GitHub es publico: checkout/poll de lectura no requiere PAT ni llave adicional.

`lifemap-main`: SCM https://github.com/DavierGonz/LifeMap.git, rama main,
Jenkinsfile, pollSCM `H/2 * * * *`. Permanece deshabilitado mientras origin
este vacio y no haya autorizacion del primer push.

`lifemap-azure-preflight`: checkout real mediante bundle Git, sin publicar;
DEPLOY_PUBLIC=false. Comprueba build secuencial de las dos variantes API,
17 pytest, salud de los ocho servicios, resultados persistidos, credencial
Kaggle con consulta pequena de metadata (cero bytes de dataset descargados),
y seis endpoints contra candidata privada en localhost:5101. FORCE_TEST_FAILURE
agrega una prueba intencional; el pipeline debe omitir todos los stages siguientes.
No se ingiere, entrena, ejecuta Spark benchmark ni recrea infraestructura.

La promocion publica futura usa Caddy: valida candidata, cambia upstream a
5101, actualiza la API principal mientras la candidata sirve, vuelve a 5100
saludable y guarda el SHA en .env del servidor. Caddy recarga por su API
localhost:2019 desde un helper Docker efimero; no se expone la administracion
ni se entrega una llave SSH a Jenkins. Se conservan imagen anterior y rollback.
Si rollback no devuelve el proxy a 5100, cleanup conserva la candidata activa.
Solo un executor Jenkins y disableConcurrentBuilds evitan promociones simultaneas.
El socket Docker es acceso privilegiado: ejecutar exclusivamente main confiable;
no habilitar builds automaticos de forks/PR de terceros.

## Demostracion despues de autorizar push

Preflight Azure ya ejecutado: build #1 SUCCESS (17 pytest y candidata real),
build #3 FAILURE deliberado con DEPLOY_PUBLIC=true (17 pass / 1 fail).
El #2 tambien fallo en preflight. La promocion fue bloqueada y
la imagen/commit de produccion no cambiaron. Evidencia en jenkins-evidence.json.
pollscm-demo.patch contiene el cambio minimo exacto propuesto para una rama
de demostracion; no esta aplicado ni publicado.

1. Publicar la rama main auditada. No se ejecuta sin autorizacion del usuario.
2. Habilitar lifemap-main e iniciar una primera ejecucion para establecer
   el checkout y baseline SCM. Los cambios posteriores los detecta pollSCM
   cada dos minutos; no exige acceso publico a Jenkins.
3. Crear un cambio minimo en rama, probar, y solicitar autorizacion del merge/
   publicacion si no la incluye la aprobacion. Observar disparo automatico,
   SHA GitHub = source-commit.txt = imagen OCI = /health publico.
4. Ejecutar FORCE_TEST_FAILURE=true y conservar FAILURE/JUnit en historial:
   comprobar que no alcanzo promocion y que /health/imagen publica no cambiaron.
5. Conservar artifacts de ambos builds y disponibilidad HTTPS. No afirmar
   demostracion automatica GitHub antes de que esos pasos se ejecuten realmente.
