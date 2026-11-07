#!/usr/bin/env bash
# SOLO en Ubuntu, dentro de un checkout nuevo. Nunca copiar secretos de Windows.
set -Eeuo pipefail
cd "$(dirname "$0")/../.."
command -v openssl >/dev/null
test ! -e .env && test ! -e .secrets || { echo 'Configuracion existente: no se sobrescribe'; exit 2; }
umask 077
mkdir -m 700 .secrets
for name in mongo_root_password mongo_writer_password mongo_reader_password; do
    openssl rand -hex 48 > ".secrets/$name.txt"
    # Compose monta archivos: los UID no-root de API/Dask/Spark deben leerlos.
    # El directorio host 0700 impide acceso a otros usuarios del host.
    chmod 644 ".secrets/$name.txt"
done
cp .env.example .env
revision="$(git rev-parse HEAD)"
sed -i "s/^DEPLOYED_COMMIT=.*/DEPLOYED_COMMIT=$revision/" .env
gid="$(stat -c %g /var/run/docker.sock)"
sed -i "s/^DOCKER_SOCKET_GID=.*/DOCKER_SOCKET_GID=$gid/" .env
chmod 600 .env
echo 'Secretos exclusivos y .env creados; valores no mostrados.'
