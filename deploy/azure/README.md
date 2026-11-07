# Preparacion de despliegue Azure - Entrega 1

**Preparado localmente; ninguna operacion remota ejecutada.** Destino indicado
por el usuario: `azureuser@20.230.240.88`, Ubuntu 24.04 x64, 4 vCPU, 16 GiB,
128 GiB, Docker/Compose instalados. Estos datos no se han comprobado por SSH.
Creditos educativos autorizados; no crear recursos Azure adicionales ni activar
facturacion de bolsillo. VM, IP, disco y trafico consumen el credito existente:
comprobar su saldo/limite antes de transferir, sin habilitar upgrades.

## Auditoria y compatibilidad

- Git inicial limpio, main `1c8ee682253a409a95f9d434fd93c56ade8a8d5f`,
  origin https://github.com/DavierGonz/LifeMap.git. No push ni consulta remota Git
  en esta preparacion. No se supone que origin ya contenga el codigo.
- Compose valida sin motor. Los ocho servicios son contenedores Linux;
  manifiestos de los cinco FROM fijados por digest contienen linux/amd64.
  Mongo, Spark master/worker, Dask scheduler/dos workers, Flask y Jenkins
  son compatibles por plataforma con Ubuntu x64. Falta prueba real en la VM.
- Limites actuales suman 6.625 GiB y 7 CPU de cuotas, no reservas; 4 vCPU
  comparten esas cuotas. Conservar limites y procesar/builds secuencialmente.
- El motor Docker local no responde en esta auditoria. No se inicio ni reparo.
  Exportacion de datos/imagenes y medicion de volumenes pendientes de motor.
- Windows usa contexto desktop-linux; Ubuntu usa su motor local (normalmente
  default). Los lanzadores host de scripts/yelp.py, spark_e1.py y jenkins_e1.py
  validan .venv 3.10 y/o asumen contexto Windows: NO ejecutarlos en Ubuntu.
  Usar Compose, comandos dentro de contenedores y Jenkinsfile/ci/pipeline.sh.
- Compose monta secretos desde rutas relativas: en Linux sus archivos deben
  ser legibles por los UID 185 y 10001. prepare-secrets.sh crea directorio
  host 0700 y archivos 0644, evitando el fallo de permisos de bind mounts.

## Que trasladar

| Objeto | Destino / finalidad |
|---|---|
| Git bundle main + esta carpeta deploy/azure | Codigo, Dockerfiles, locks, Compose, pruebas, evidencia, configuracion HTTPS |
| Cinco imagenes propias, en images.tar | Reutilizar builds linux/amd64 existentes sin descargar/reconstruir todo |
| Dump logico comprimido SOLO lifemap | Negocios, resenas enriquecidas, indices, ingestion_runs, model_runs, closure_stats, analytics_runs/state |
| model_store completo | sentiment-active.json, predictor.json, PipelineModel Spark y SHA del modelo |
| dataset_cache completo | Yelp v4, manifiestos, SQLite/Parquet preparados y /data/lifemap-e1; permite CI cache-hit sin reprocesar |

La opcion recomendada conserva ambos volumenes de archivos completos para
evitar referencias rotas: el cargador comprueba los Parquet y stage.sqlite
ANTES de decidir cache-hit. Copiar solo Mongo/modelo basta para servir API,
pero NO para reproducir el pipeline Jenkins actual sin descargar/procesar otra vez.
Si el paquete completo no cabe, medir y planear una reduccion explicita antes
de omitir archivos. No hay medidas actuales de volumen: Docker esta detenido.

**Excluir:** .venv Windows, .env, .secrets, llaves SSH, Kaggle, tokens Jenkins,
AppData Docker, VHDX, mongo_data/mongo_config fisicos, jenkins_home (contiene
credenciales), scratches/checkpoints temporales y todos los recursos de
Geoespacial/Discos. No transferir otros proyectos ni exportar todas las bases.
Recrear usuarios/secretos/Jenkins en destino; conservar el historial academico
sanitizado en Git. Los archives de Mongo/Yelp/modelos son privados, fuera de Git.

## Empaquetado local (futuro, con Docker funcionando)

Sin iniciar jobs ni pipelines durante el dump. No es snapshot transaccional
de un replica set: confirmar ausencia de escrituras propias durante exportacion.
Ejecutar en Windows PowerShell usando Docker CLI, no redireccion binaria `>`:

```powershell
Set-Location D:\Proyects\LifeMap
$lifeMapPackage = 'artifacts\azure-e1'
New-Item -ItemType Directory -Path $lifeMapPackage
git bundle create artifacts/azure-e1/lifemap.bundle main
# Hasta versionar esta preparacion, adjuntarla como overlay separado:
tar -czf artifacts/azure-e1/deploy-config.tgz deploy/azure
docker --context desktop-linux system df
docker --context desktop-linux exec lifemap-spark-master-1 du -sh /models /data
docker --context desktop-linux cp deploy/azure/mongo-transfer.sh lifemap-mongodb-1:/tmp/lifemap-mongo-transfer.sh
docker --context desktop-linux exec lifemap-mongodb-1 sh /tmp/lifemap-mongo-transfer.sh dump
docker --context desktop-linux cp lifemap-mongodb-1:/tmp/lifemap-migration.archive.gz artifacts/azure-e1/mongo.archive.gz
docker --context desktop-linux exec lifemap-spark-master-1 tar -czf /scratch/lifemap-models.tgz -C /models .
docker --context desktop-linux exec lifemap-spark-master-1 tar -czf /scratch/lifemap-data.tgz -C /data .
docker --context desktop-linux cp lifemap-spark-master-1:/scratch/lifemap-models.tgz artifacts/azure-e1/models.tgz
docker --context desktop-linux cp lifemap-spark-master-1:/scratch/lifemap-data.tgz artifacts/azure-e1/data.tgz
docker --context desktop-linux image save --output artifacts/azure-e1/images.tar lifemap-mongo:infra-v1 lifemap-spark:infra-v1 lifemap-dask:infra-v1 lifemap-api:infra-v1 lifemap-jenkins:infra-v1
Get-ChildItem -File $lifeMapPackage | Get-FileHash -Algorithm SHA256 | Select-Object Hash,Path
```

Antes de crear archives, reservar espacio en host y scratches para copias
comprimidas. images.tar no se comprime; medir paquete, RAM y disco, no asumir
que 128 GiB bastan. Mantener margen para extraccion, Mongo e indices. Guardar
SHA256SUMS con nombres relativos, hash del commit y SHA256 de cada archive.
Conservar fuentes/backups; no usar prune ni down -v. Los scripts se niegan a
sobrescribir el dump: si existe, revisarlo y preservarlo antes de otra copia.

## Transferencia privada (requiere autorizacion remota)

Usar el SSH Agent ya configurado por el usuario; estos comandos no leen,
copian ni modifican archivos de llaves. No usar -i, ssh-keygen ni agent-add.
Si no hay agente configurado, el usuario inicia la sesion con su cliente;
no inspeccionar la llave. Verificar huella del host con Azure por canal fiable;
no deshabilitar StrictHostKeyChecking. Mantener SSH restringido a su IP.

```powershell
ssh azureuser@20.230.240.88 'umask 077; mkdir -p ~/lifemap-transfer; chmod 700 ~/lifemap-transfer'
scp artifacts/azure-e1/* azureuser@20.230.240.88:lifemap-transfer/
```

Son instrucciones pendientes, NO ejecutadas. Para transferencias interrumpidas
usar SFTP con reanudacion del cliente elegido; validar hashes al terminar,
no repetir descargas de Kaggle. Nunca servir esta carpeta con Caddy.

## Instalacion Ubuntu (futura, tras autorizar acceso)

1. Consultar `uname -m`, `docker version`, `docker compose version`, `free -h`,
   `df -h`, `ss -lnt`, `docker ps -a` y `docker volume ls`; verificar host x86_64,
   espacio real y ausencia de conflictos/recursos LifeMap ajenos. No borrarlos.
   Confirmar plugin Compose y compatibilidad API del motor/CLI Jenkins fijado.
2. Verificar SHA256, `git clone ~/lifemap-transfer/lifemap.bundle ~/LifeMap`,
   comprobar `git rev-parse HEAD`; extraer deploy-config.tgz dentro del checkout.
   Mantener paquete privado con chmod 700 en ~/lifemap-transfer.
3. `cd ~/LifeMap; bash deploy/azure/prepare-secrets.sh`. No copiar .env Windows.
   `docker load -i ~/lifemap-transfer/images.tar` y comprobar arquitectura linux/amd64
   e imagen OCI de API coincidente con SHA. Si se construye en destino, construir
   las cinco imagenes de una en una; NO lanzar ingesta ni entrenamiento.
4. `docker compose --project-name lifemap config --quiet`;
   `docker compose --project-name lifemap up -d --no-build --wait mongodb`.
   Copiar archive de Mongo al path fijo del script:

   ```bash
   docker cp ~/lifemap-transfer/mongo.archive.gz lifemap-mongodb-1:/tmp/lifemap-migration.archive.gz
   docker compose exec -T mongodb sh -s -- restore < deploy/azure/mongo-transfer.sh
   ```

   Restore solo lifemap, sin --drop, sin usuarios/roles heredados, rechaza destino
   con colecciones de aplicacion. Si falla, detenerse: no repetir sobre una carga
   parcial ni eliminar volumenes automaticamente. Usuario/roles nuevos del init
   permanecen; se restauran indices desde el dump.
5. `docker compose create --no-build spark-master` crea sus volumenes sin job.
   Con contenedor detenido, copiar los archivos; inspeccionar `tar -tzf` para
   rechazar paths absolutos, ../ o enlaces inesperados antes de extraer:

   ```bash
   docker cp ~/lifemap-transfer/models.tgz lifemap-spark-master-1:/scratch/models.tgz
   docker cp ~/lifemap-transfer/data.tgz lifemap-spark-master-1:/scratch/data.tgz
   docker compose run --rm --no-deps --user 0 --entrypoint sh spark-master -c 'tar -tzf /scratch/models.tgz; tar -tzf /scratch/data.tgz'
   # SOLO despues de revisar listas y confirmar volumenes /models y /data vacios:
   docker compose run --rm --no-deps --user 0 --entrypoint sh spark-master -ec 'test -z "$(ls -A /models)"; test -z "$(ls -A /data)"; tar -xzf /scratch/models.tgz -C /models; tar -xzf /scratch/data.tgz -C /data; chown -R 185:0 /models /data; chmod -R g+rwX /data; chmod -R a+rX /models; chmod 2775 /data /models'
   ```

   Solo archivos de datos en estos volumenes, nunca secretos. Dask UID10001
   comparte grupo 0, Spark UID185 y API lee modelos sin escribir. No alterar
   /data/yelp-v4 ni rutas absolutas registradas (/data y /models).
6. Arrancar gradualmente Spark master/worker, Dask scheduler/dos workers, API
   y Jenkins con `docker compose up -d --no-build --wait <servicios>`.
   No usar lanzadores Windows ni scripts que impongan desktop-linux en Ubuntu.
   Jenkins sera nuevo/autenticado: registrarlo/configurarlo separadamente con
   credenciales nuevas por su UI privada; no copiar jenkins_home ni ejecutar jobs
   automaticamente. Primero validar datos/modelo. Mantener Kaggle fuera de Git;
   provisionar Jenkins Credentials antes de la futura demostracion CI completa.
7. Ejecutar `docker compose exec spark-master python3 /opt/lifemap/analytics/verify.py`
   y `docker compose exec api python /app/tests/e1/smoke_api.py --expected-commit "$(git rev-parse HEAD)"`.
   Confirmar 16.944/1.320.594, indice 2dsphere, predicciones, modelos, los seis
   endpoints y ocho healthchecks. Historial de 24 benchmarks sigue siendo local:
   copiar resultados no los convierte en mediciones de Azure.

## HTTPS publico: solo Flask

Propuesta: **Caddy en el host**, proxy hacia 127.0.0.1:5100; mantiene los ocho
contenedores existentes y evita un balanceador/App Gateway/servicio Azure pago.
Instalar Caddy desde su repositorio oficial solo tras autorizar operaciones remotas.
Su configuracion preparada es Caddyfile.example; no inicia servicios ahora.

Solicitar un DNS name label en el recurso de IP publica YA existente, sin zona
Azure DNS ni compra de dominio. Usar el FQDN exacto que muestre Azure (puede
incluir hash); falta conocer region y etiqueta disponible. Ejemplo orientativo:
`https://<etiqueta>.<region>.cloudapp.azure.com`. No se ha creado ni probado.
Comprobar que DNS resuelva a 20.230.240.88 antes de solicitar certificado ACME.
Si ese recurso no permite etiqueta, usar un dominio/subdominio ya disponible
con permiso; no comprarlo ni crear una zona de pago.

NSG: conservar TCP22 solo IP del usuario; permitir TCP80 y TCP443 desde Internet
para ACME/HTTPS y celular con datos. No autorizar 27017/7077/8082/8788/8083/5100.
Revisar tambien reglas por defecto de AzureLoadBalancer/VirtualNetwork para que
no alcancen administracion fuera del alcance previsto. No cambiar el NSG ahora.
Firewall Ubuntu debe conservar SSH permitido antes de cualquier activacion;
Docker puede eludir UFW en puertos publicados: la defensa es NSG + no publicar
servicios privados + binds 127.0.0.1, no confiar solo en UFW.

Sustituir REEMPLAZAR_FQDN, instalar Caddyfile en /etc/caddy/Caddyfile, ejecutar
`sudo caddy validate --config /etc/caddy/Caddyfile` antes de reload/start. ACME
obtiene/renueva certificado automaticamente; verificar cadena valida sin -k.
No publicar el puerto administrativo Caddy (2019), ni directorios de transferencias.

Administracion Jenkins opcional por tunel SSH local, nunca NSG publico:
`ssh -N -L 8083:127.0.0.1:8083 azureuser@20.230.240.88`.
Validar HTTPS /health con SHA desde una red externa y con Wi-Fi del celular
apagado, mas POST /predict y consultas variables. La API es JSON, no un dashboard.
No declarar E1.2 ni E1.9 remoto cumplidos hasta verificar URL, certificados,
salud, SHA y disponibilidad durante evaluacion. GitHub/pollSCM requieren push
autorizado posterior; este despliegue por bundle no demuestra merge remoto.

## Referencias oficiales

- https://learn.microsoft.com/en-us/azure/virtual-network/ip-services/public-ip-addresses
- https://caddyserver.com/docs/quick-starts/reverse-proxy
- https://caddyserver.com/docs/install#debian-ubuntu-raspbian
- https://docs.docker.com/engine/network/packet-filtering-firewalls/
- https://www.mongodb.com/docs/database-tools/mongodump/
- https://www.mongodb.com/docs/database-tools/mongorestore/
