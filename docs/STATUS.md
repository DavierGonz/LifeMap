# Estado vigente -> Entrega 1

Implementacion local: ingesta definitiva Restaurants (16.944 negocios,
1.320.594 resenas), dos modelos Spark entrenados por negocio, NB guardado,
MapReduce RDD/DataFrame coincidente, 24 benchmarks y seis endpoints Flask.
Ver README y evidencia final; los informes locales anteriores son historicos.

Despliegue Azure ejecutado y verificado: Ubuntu 24.04 x64, 20.230.240.88.
URL: https://lifemap-e1-davier-2026.westus2.cloudapp.azure.com
Ocho contenedores saludables; seis endpoints HTTP 200 desde Internet, TLS
valido, cantidades/modelo/indices identicos al origen. Evidencia real en
deploy/azure/deployment-evidence.json. No se reentreno ni descargo Yelp.
Pendientes externos: autorizacion de primer push,
activacion GitHub/pollSCM y demostracion tras merge; comunicacion de area/
familia y exclusividad al profesor; sustentacion. SentimentStream no fue entregado
como codigo fuente, por lo que su reutilizacion literal no esta verificada.
E1.2/E1.9 ya tienen verificacion remota puntual; falta mantener disponibilidad
durante evaluacion. E1.3 remoto y sustentacion E1.10 siguen pendientes.
Power BI Desktop requiere decision posterior para E2; no bloquea E1.
Presupuesto $0 COP; referencias protegidas; sin implementacion E2.

Jenkins Azure configurado: Kaggle protegido, preflight #1 SUCCESS (17 tests),
#2 fallo deliberado que bloqueo promocion. API publica sin cambios.
Ver deploy/azure/jenkins-evidence.json y JENKINS.md.
