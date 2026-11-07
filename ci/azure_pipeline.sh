#!/usr/bin/env bash
# E1.3 sobre Azure poblado: sin descarga, ingesta, entrenamiento o recreacion de datos.
set -Eeuo pipefail
: "${BUILD_NUMBER:?Jenkins required}"; : "${JOB_BASE_NAME:?Jenkins required}"
RUN_ID="${JOB_BASE_NAME}-${BUILD_NUMBER}"
[[ "$RUN_ID" =~ ^lifemap-[a-z0-9-]+$ ]] || exit 2
TEST="$RUN_ID-tests"; CANDIDATE="$RUN_ID-candidate"
TEST_IMAGE="lifemap-api-test:$RUN_ID"; IMAGE="lifemap-api:$RUN_ID"
compose() { docker compose --project-name lifemap --env-file .env.ci "$@"; }
reader_source() { docker inspect --format '{{range .Mounts}}{{if eq .Destination "/run/secrets/mongo_reader_password"}}{{.Source}}{{end}}{{end}}' lifemap-mongodb-1; }
proxy() { docker run --rm --network host --memory 128m --user 0 --mount type=bind,src=/etc/caddy,dst=/host-caddy --entrypoint python "$IMAGE" /app/ci/azure_promote.py switch "$2" --port "$1"; }
set_revision() { docker run --rm --network none --memory 128m --user 0 --mount "type=bind,src=$(cat .ci/host-project)/.env,dst=/host-env" --entrypoint python "$IMAGE" /app/ci/azure_promote.py revision "$1"; }
case "${1:-}" in
 prepare)
  umask 077; mkdir -p reports .ci
  rm -f reports/*.json reports/*.xml reports/*.txt reports/*.log .ci/tests.passed .ci/candidate.passed
  git rev-parse HEAD > reports/source-commit.txt
  printf 'DEPLOYED_COMMIT=%s\n' "$(git rev-parse HEAD)" > .env.ci
  for role in root writer reader; do
    path=$(docker inspect --format "{{range .Mounts}}{{if eq .Destination \"/run/secrets/mongo_${role}_password\"}}{{.Source}}{{end}}{{end}}" lifemap-mongodb-1)
    test -n "$path"
    printf 'MONGO_%s_PASSWORD_FILE=%s\n' "${role^^}" "$path" >> .env.ci
  done
  compose config --quiet
  docker inspect --format '{{.Image}}' lifemap-api-1 > reports/api-image-before.txt
  docker inspect --format '{{range .Config.Env}}{{println .}}{{end}}' lifemap-api-1 | sed -n 's/^DEPLOYED_COMMIT=//p' > .ci/previous-commit
  docker inspect --format '{{index .Config.Labels "com.docker.compose.project.working_dir"}}' lifemap-mongodb-1 > .ci/host-project
  test "$(cat .ci/host-project)" = /home/azureuser/LifeMap
  ;;
 build)
  revision=$(git rev-parse HEAD)
  docker build --build-arg "VCS_REF=$revision" --target test -f docker/api/Dockerfile -t "$TEST_IMAGE" .
  docker build --build-arg "VCS_REF=$revision" --target runtime -f docker/api/Dockerfile -t "$IMAGE" .
  ;;
 test)
  paths=(/app/tests/e1/test_core.py /app/tests/e1/test_azure_ci.py)
  if [[ "${FORCE_TEST_FAILURE:-false}" == true ]]; then paths+=(/app/ci/faults/test_forced_failure.py); fi
  docker create --name "$TEST" --label "lifemap.ci_run=$RUN_ID" --network none --memory 256m --cpus 1 "$TEST_IMAGE" python -m pytest -q -p no:cacheprovider --junitxml=/tmp/pytest.xml "${paths[@]}"
  status=0; docker start -a "$TEST" || status=$?
  docker cp "$TEST:/tmp/pytest.xml" reports/pytest.xml
  if [[ "$status" == 0 ]]; then touch .ci/tests.passed; fi
  exit "$status"
  ;;
 verify)
  test -f .ci/tests.passed
  for name in mongodb spark-master spark-worker dask-scheduler dask-worker-1 dask-worker-2 api jenkins; do
    test "$(docker inspect --format '{{.State.Health.Status}}' "lifemap-$name-1")" = healthy
  done
  docker exec lifemap-spark-master-1 python3 /opt/lifemap/analytics/verify.py > reports/academic-results.json
  ;;
 kaggle)
  set +x
  printf '%s' "$KAGGLE_CREDENTIAL" | docker exec -i lifemap-dask-scheduler-1 python -c 'import sys,json; from ingestion.download import fetch,API,DATASET; credential=sys.stdin.read().strip(); response=fetch(API+"/view/"+DATASET,credential); metadata=json.load(response); response.close(); print(json.dumps({"credential_configured":bool(credential),"metadata_http":200,"dataset":DATASET,"current_version":metadata.get("currentVersionNumber"),"downloaded_bytes":0}))' > reports/kaggle-auth.json
  ;;
 candidate)
  test -f .ci/tests.passed
  docker create --name "$CANDIDATE" --label "lifemap.ci_run=$RUN_ID" --network lifemap_backend --memory 256m --cpus 0.5 -p 127.0.0.1:5101:5000 --mount type=volume,src=lifemap_model_store,dst=/models,readonly --mount "type=bind,src=$(reader_source),dst=/run/secrets/mongo_reader_password,readonly" -e MONGO_HOST=mongodb -e MONGO_READER_PASSWORD_FILE=/run/secrets/mongo_reader_password -e SPARK_STATUS_URL=http://spark-master:8080/json/ -e DASK_STATUS_URL=http://dask-scheduler:8787/json/counts.json "$IMAGE"
  docker start "$CANDIDATE"
  for attempt in $(seq 1 30); do
    if docker exec "$CANDIDATE" python /app/tests/e1/smoke_api.py --expected-commit "$(git rev-parse HEAD)" > reports/candidate-api.json 2>reports/candidate-api-error.txt; then touch .ci/candidate.passed; break; fi
    sleep 2
  done
  test -f .ci/candidate.passed
  ;;
 deploy)
  test -f .ci/tests.passed; test -f .ci/candidate.passed
  revision=$(git rev-parse HEAD); previous=$(cat .ci/previous-commit)
  rollback() {
    proxy 5101 "$revision"
    docker tag "$(cat reports/api-image-before.txt)" lifemap-api:infra-v1
    DEPLOYED_COMMIT="$previous" compose up -d --no-deps --no-build --wait --wait-timeout 90 api
    proxy 5100 "$previous"
    set_revision "$previous"
    printf 'Previous healthy API restored.\n' > reports/rollback.txt
  }
  trap 'status=$?; trap - ERR; rollback; exit "$status"' ERR
  proxy 5101 "$revision"
  docker tag "$IMAGE" lifemap-api:infra-v1
  compose up -d --no-deps --no-build --wait --wait-timeout 90 api
  docker exec lifemap-api-1 python /app/tests/e1/smoke_api.py --expected-commit "$revision" > reports/deployed-api.json
  proxy 5100 "$revision"
  set_revision "$revision"
  docker run --rm --network host --memory 128m --cpus 0.5 --entrypoint python "$IMAGE" /app/tests/e1/smoke_api.py https://lifemap-e1-davier-2026.westus2.cloudapp.azure.com --expected-commit "$revision" > reports/public-api.json
  printf 'Azure promotion passed all gates.\n' > reports/deployment.txt
  ;;
 cleanup)
  for name in "$TEST" "$CANDIDATE"; do
    if docker inspect "$name" >/dev/null 2>&1; then
      test "$(docker inspect --format '{{index .Config.Labels "lifemap.ci_run"}}' "$name")" = "$RUN_ID"
      if [[ "$name" == "$CANDIDATE" ]]; then
        serving=$(docker run --rm --network none --memory 64m --user 0 --mount type=bind,src=/etc/caddy,dst=/host-caddy,readonly --entrypoint python "$IMAGE" -c 'from pathlib import Path; print("5101" if "reverse_proxy 127.0.0.1:5101" in Path("/host-caddy/Caddyfile").read_text() else "5100")')
        if [[ "$serving" != 5100 ]]; then echo 'Candidate preserved: Caddy still serves port 5101; investigate rollback'; exit 2; fi
      fi
      docker rm -f "$name"
    fi
  done
  ;;
 *) exit 2;;
esac
