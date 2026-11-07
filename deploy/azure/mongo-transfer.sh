#!/bin/sh
# Ejecutar DENTRO del contenedor MongoDB LifeMap. Sin secretos en argumentos.
set -eu
action="${1:-}"
archive=/tmp/lifemap-migration.archive.gz
case "$action" in dump|restore) ;; *) echo 'Usar dump o restore' >&2; exit 2;; esac
umask 077
config="$(mktemp /tmp/lifemap-tools.XXXXXX)"
trap 'rm -f "$config"' EXIT HUP INT TERM
export LIFEMAP_TOOL_CONFIG="$config"
mongosh --nodb --quiet --eval '
const fs = require("fs");
const password = fs.readFileSync("/run/secrets/mongo_root_password", "utf8").trim();
fs.writeFileSync(process.env.LIFEMAP_TOOL_CONFIG, "password: " + JSON.stringify(password) + "\n", {mode: 0o600});
' >/dev/null
if [ "$action" = dump ]; then
    test ! -e "$archive" || { echo 'Archive existente: conservarlo y revisar antes de repetir' >&2; exit 2; }
    # No --dumpDbUsersAndRoles: solo base de aplicacion, no usuarios ni admin.
    mongodump --config="$config" --host=127.0.0.1 --username=lifemap_admin --authenticationDatabase=admin --db=lifemap --archive="$archive" --gzip --numParallelCollections=1
else
    test -s "$archive"
    # Se requiere un volumen nuevo con usuarios nuevos; no restaurar encima de datos.
    mongosh --quiet --eval '
const fs=require("fs");
const a=db.getSiblingDB("admin");
if(!a.auth("lifemap_admin",fs.readFileSync("/run/secrets/mongo_root_password","utf8").trim())) throw new Error("Auth failed");
const names=db.getSiblingDB("lifemap").getCollectionNames().filter(n=>!n.startsWith("system."));
if(names.length) throw new Error("Destination already has application collections: refusing restore");
' >/dev/null
    mongorestore --config="$config" --host=127.0.0.1 --username=lifemap_admin --authenticationDatabase=admin --archive="$archive" --gzip --nsInclude='lifemap.*' --nsExclude='lifemap.system.*' --numParallelCollections=1 --stopOnError
fi
echo 'Operacion terminada; archive conservado en /tmp/lifemap-migration.archive.gz' >&2
