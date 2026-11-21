#!/usr/bin/env bash
# E2: verifica infraestructura poblada, sin descargar, ingerir ni entrenar.
set -Eeuo pipefail
: "${BUILD_NUMBER:?Jenkins required}"; : "${JOB_BASE_NAME:?Jenkins required}"
RUN_ID="${JOB_BASE_NAME}-${BUILD_NUMBER}"
[[ "$RUN_ID" =~ ^lifemap-[a-z0-9-]+$ ]] || exit 2
TEST="$RUN_ID-tests"; CANDIDATE="$RUN_ID-candidate"
TEST_IMAGE="lifemap-api-test:$RUN_ID"; IMAGE="lifemap-api:$RUN_ID"
STREAM_IMAGE="lifemap-spark:$RUN_ID"
compose() { docker compose --project-name lifemap --profile e2 --profile streaming --env-file .env.ci "$@"; }
secret_source() { docker inspect --format "{{range .Mounts}}{{if eq .Destination \"/run/secrets/mongo_${1}_password\"}}{{.Source}}{{end}}{{end}}" lifemap-mongodb-1; }
helper() {
 docker run --rm --name "$RUN_ID-helper" --label "lifemap.ci_run=$RUN_ID" --user 185:0 --network lifemap_backend --memory 192m --cpus 0.5 \
  --mount type=volume,src=lifemap_model_store,dst=/models,readonly \
  --mount type=volume,src=lifemap_stream_checkpoints,dst=/checkpoints,readonly \
  --mount type=volume,src=lifemap_dataset_cache,dst=/data \
  --mount "type=bind,src=$(secret_source writer),dst=/run/secrets/mongo_writer_password,readonly" \
  --entrypoint python "$IMAGE" /app/ci/e2_smoke.py "$@"
}
case "${1:-}" in
 prepare)
  umask 077; mkdir -p reports .ci
  rm -f reports/*.json reports/*.xml reports/*.txt reports/*.log .ci/*.passed
  revision=$(git rev-parse HEAD); printf '%s\n' "$revision" > reports/source-commit.txt
  printf 'DEPLOYED_COMMIT=%s\nE2_ENABLED=1\n' "$revision" > .env.ci
  for role in root writer reader; do
    path=$(secret_source "$role"); test -n "$path"
    printf 'MONGO_%s_PASSWORD_FILE=%s\n' "${role^^}" "$path" >> .env.ci
  done
  compose config --quiet
  for name in mongodb spark-master spark-worker dask-scheduler dask-worker-1 dask-worker-2 api jenkins kafka streaming; do
    test "$(docker inspect --format '{{.State.Health.Status}}' "lifemap-$name-1")" = healthy
    test "$(docker inspect --format '{{.Platform}}' "lifemap-$name-1")" = linux
    test "$(docker exec "lifemap-$name-1" uname -m)" = x86_64
  done
  docker inspect --format '{{.Image}}' lifemap-api-1 > reports/api-image-before.txt
  docker inspect --format '{{.Image}}' lifemap-streaming-1 > reports/stream-image-before.txt
  docker inspect --format '{{range .Config.Env}}{{println .}}{{end}}' lifemap-api-1 | sed -n 's/^DEPLOYED_COMMIT=//p' > .ci/previous-commit
  docker inspect --format '{{index .Config.Labels "com.docker.compose.project.working_dir"}}' lifemap-mongodb-1 > .ci/host-project
  ;;
 build)
  # Solo capas modificadas; Kafka y demas imagenes existentes se validaron en prepare.
  docker build -f docker/spark/Dockerfile -t "$STREAM_IMAGE" .
  revision=$(git rev-parse HEAD)
  docker build --build-arg "VCS_REF=$revision" --target test -f docker/api/Dockerfile -t "$TEST_IMAGE" .
  docker build --build-arg "VCS_REF=$revision" --target runtime -f docker/api/Dockerfile -t "$IMAGE" .
  for image in "$TEST_IMAGE" "$IMAGE" "$STREAM_IMAGE"; do
   test "$(docker image inspect --format '{{.Os}}/{{.Architecture}}' "$image")" = linux/amd64
  done
  ;;
 test)
  paths=(/app/tests/e1/test_core.py /app/tests/e1/test_azure_ci.py /app/tests/e1/test_ci_e2.py /app/tests/unit/test_e2_producer.py /app/tests/unit/test_e2_streaming.py /app/tests/unit/test_survival.py /app/tests/unit/test_risk.py /app/tests/unit/test_e2_health.py)
  if [[ "${FORCE_TEST_FAILURE:-false}" == true ]]; then paths+=(/app/ci/faults/test_forced_failure.py); fi
  docker create --name "$TEST" --label "lifemap.ci_run=$RUN_ID" --network none --memory 256m --cpus 1 "$TEST_IMAGE" python -m pytest -q -p no:cacheprovider --junitxml=/tmp/pytest.xml "${paths[@]}"
  status=0; docker start -a "$TEST" || status=$?
  docker cp "$TEST:/tmp/pytest.xml" reports/pytest.xml
  if [[ "$status" == 0 ]]; then touch .ci/unit.passed; fi
  exit "$status"
  ;;
 models) test -f .ci/unit.passed; helper models > reports/models.json; touch .ci/models.passed;;
 smoke)
  test -f .ci/models.passed
  helper smoke > reports/stream-100.json
  scope="test_ci_${RUN_ID//-/_}"
  helper publish-test --scope "$scope" > reports/candidate-stream-publish.json
  offsets=$(docker run --rm -i --network none --memory 64m --entrypoint python "$IMAGE" -c 'import json,sys; print(json.dumps(json.load(sys.stdin)["starting_offsets"]))' < reports/candidate-stream-publish.json)
  printf 'services:\n  streaming:\n    image: %s\n' "$STREAM_IMAGE" > .ci/stream-candidate.yml
  # Test the image just built, not only the previous production consumer.
  compose -f docker-compose.yml -f .ci/stream-candidate.yml run --rm --no-deps --name "$RUN_ID-stream-test" --label "lifemap.ci_run=$RUN_ID" streaming /opt/lifemap/streaming/consumer.py --scope "$scope" --starting-offsets "$offsets" --available-now > reports/candidate-stream.log 2>&1
  helper verify-test --scope "$scope" > reports/candidate-stream.json
  touch .ci/smoke.passed
  ;;
 candidate)
  test -f .ci/smoke.passed
  docker create --name "$CANDIDATE" --label "lifemap.ci_run=$RUN_ID" --network lifemap_backend --memory 256m --cpus 0.5 -p 127.0.0.1:5101:5000 \
   --mount type=volume,src=lifemap_model_store,dst=/models,readonly --mount type=volume,src=lifemap_stream_checkpoints,dst=/checkpoints,readonly \
   --mount "type=bind,src=$(secret_source reader),dst=/run/secrets/mongo_reader_password,readonly" \
   -e E2_ENABLED=1 -e STREAM_STATUS_FILE=/checkpoints/production/status.json -e MONGO_HOST=mongodb \
   -e MONGO_READER_PASSWORD_FILE=/run/secrets/mongo_reader_password -e SPARK_STATUS_URL=http://spark-master:8080/json/ \
   -e DASK_STATUS_URL=http://dask-scheduler:8787/json/counts.json "$IMAGE"
  docker network connect lifemap_frontend "$CANDIDATE"; docker start "$CANDIDATE"
  for attempt in $(seq 1 30); do
   if docker exec "$CANDIDATE" python /app/ci/smoke_e2_api.py --expected-commit "$(git rev-parse HEAD)" > reports/candidate-api.json 2>reports/candidate-error.txt; then touch .ci/api.passed; break; fi
   sleep 2
  done
  test -f .ci/api.passed
  ;;
 promote)
  for gate in unit models smoke api; do test -f ".ci/$gate.passed"; done
  case "${DEPLOY_TARGET:-validate-only}" in
   local)
    # Candidate remains available during local replacement; no public listener here.
    previous=$(cat .ci/previous-commit)
    rollback() { docker tag "$(cat reports/api-image-before.txt)" lifemap-api:infra-v1; docker tag "$(cat reports/stream-image-before.txt)" lifemap-spark:stream-v2; DEPLOYED_COMMIT="$previous" compose up -d --no-deps --no-build --wait --wait-timeout 180 streaming api; }
    trap 'status=$?; trap - ERR; rollback; exit "$status"' ERR
    docker tag "$IMAGE" lifemap-api:infra-v1
    docker tag "$STREAM_IMAGE" lifemap-spark:stream-v2
    compose up -d --no-deps --no-build --wait --wait-timeout 180 streaming
    compose up -d --no-deps --no-build --wait --wait-timeout 90 api
    docker exec lifemap-api-1 python /app/ci/smoke_e2_api.py --expected-commit "$(git rev-parse HEAD)" > reports/deployed-api.json
    echo 'LOCAL_PROMOTION_PASSED' > reports/promotion.txt
    ;;
   azure)
    test "$(cat .ci/host-project)" = /home/azureuser/LifeMap
    # Use the existing Caddy transaction/rollback after the E2 gates above.
    touch .ci/tests.passed .ci/candidate.passed
    export E2_STREAM_CANDIDATE="$STREAM_IMAGE"
    bash ci/azure_pipeline.sh deploy
    ;;
   *) echo 'Promotion disabled: validation-only' >&2; exit 2;;
  esac
  ;;
 cleanup)
  for name in "$TEST" "$CANDIDATE" "$RUN_ID-helper" "$RUN_ID-stream-test"; do
   if docker inspect "$name" >/dev/null 2>&1; then
    test "$(docker inspect --format '{{index .Config.Labels "lifemap.ci_run"}}' "$name")" = "$RUN_ID"
    # Never remove a candidate still selected by Caddy after an Azure rollback problem.
    if [[ "$name" == "$CANDIDATE" && "${DEPLOY_TARGET:-}" == azure ]]; then
     docker run --rm --network none --memory 64m --user 0 --mount type=bind,src=/etc/caddy,dst=/host-caddy,readonly --entrypoint python "$IMAGE" -c 'from pathlib import Path; assert "reverse_proxy 127.0.0.1:5101" not in Path("/host-caddy/Caddyfile").read_text()'
    fi
    docker rm -f "$name"
   fi
  done
  ;;
 *) exit 2;;
esac
