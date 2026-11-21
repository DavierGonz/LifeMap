#!/bin/sh
# DENTRO de lifemap-mongodb-1, después de detener todos los escritores E2.
set -eu
action=${1:-}; directory=${2:-}
case "$directory" in /tmp/lifemap-e2-*) ;; *) echo 'Use unique /tmp/lifemap-e2-* directory' >&2; exit 2;; esac
case "$action" in dump|restore|backup) ;; *) exit 2;; esac
umask 077
config=$(mktemp /tmp/lifemap-e2-tools.XXXXXX)
trap 'rm -f "$config"' EXIT HUP INT TERM
export LIFEMAP_TOOL_CONFIG="$config" LIFEMAP_E2_ACTION="$action"
mongosh --nodb --quiet --eval '
const fs=require("fs");
fs.writeFileSync(process.env.LIFEMAP_TOOL_CONFIG,"password: "+JSON.stringify(fs.readFileSync("/run/secrets/mongo_root_password","utf8").trim())+"\n",{mode:0o600});
' >/dev/null
collections='risk_catalog risk_baselines risk_metadata risk_features alerts survival_results stream_reviews stream_windows hotspots'
if [ "$action" = backup ]; then
 test ! -e "$directory"
 mongodump --config="$config" --host=127.0.0.1 --username=lifemap_admin --authenticationDatabase=admin --db=lifemap --gzip --out="$directory" --numParallelCollections=1
 echo 'Full application backup with indexes; users/secrets excluded.'
 exit 0
fi
if [ "$action" = dump ]; then
 test ! -e "$directory"; mkdir "$directory"
 for collection in $collections; do
  mongodump --config="$config" --host=127.0.0.1 --username=lifemap_admin --authenticationDatabase=admin --db=lifemap --collection="$collection" --gzip --out="$directory" --numParallelCollections=1
 done
else
 test -d "$directory/lifemap"
 # Azure E1 already populated: only absent E2 collections may be restored.
 mongosh --quiet --eval '
const fs=require("fs");const a=db.getSiblingDB("admin");
if(!a.auth("lifemap_admin",fs.readFileSync("/run/secrets/mongo_root_password","utf8").trim()))throw new Error("Auth failed");
const allowed="risk_catalog risk_baselines risk_metadata risk_features alerts survival_results stream_reviews stream_windows hotspots".split(" ");
const existing=db.getSiblingDB("lifemap").getCollectionNames();
if(allowed.some(n=>existing.includes(n)))throw new Error("E2 destination exists: preserve it; no overwrite permitted");
' >/dev/null
 for collection in $collections; do
  test -f "$directory/lifemap/$collection.bson.gz"
  mongorestore --config="$config" --host=127.0.0.1 --username=lifemap_admin --authenticationDatabase=admin --gzip --nsInclude="lifemap.$collection" --dir="$directory" --stopOnError --numParallelCollections=1
 done
fi
echo 'E2 application collections preserved with indexes. E1 and users untouched.'
