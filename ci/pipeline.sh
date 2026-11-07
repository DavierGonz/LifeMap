#!/usr/bin/env bash
# Adaptado de Geoespacial: gates, imagen candidata y promocion posterior a pruebas.
set -Eeuo pipefail
: "${BUILD_NUMBER:?Jenkins build required}"
: "${JOB_BASE_NAME:?Jenkins job required}"
RUN_ID="${JOB_BASE_NAME}-${BUILD_NUMBER}"
[[ "$RUN_ID" =~ ^lifemap-[a-z0-9-]+$ ]] || { echo 'Invalid LifeMap CI name'; exit 2; }
CANDIDATE="${RUN_ID}-candidate"; TEST="${RUN_ID}-tests"
CANDIDATE_IMAGE="lifemap-api:${RUN_ID}"; TEST_IMAGE="lifemap-api-test:${RUN_ID}"
compose() { docker compose --project-name lifemap --env-file .env.ci "$@"; }
server_path() {
  local value="$1"
  if [[ "$value" =~ ^([A-Za-z]): ]]; then
    local drive="${BASH_REMATCH[1],,}"; value="${value:3}"; value="${value//\\//}"
    printf '/run/desktop/mnt/host/%s/%s' "$drive" "$value"
  else printf '%s' "$value"; fi
}
secret_source() {
  local name="$1"
  docker inspect --format "{{range .Mounts}}{{if eq .Destination \"/run/secrets/${name}\"}}{{.Source}}{{end}}{{end}}" lifemap-mongodb-1
}
case "${1:-}" in
 prepare)
  umask 077; mkdir -p reports .ci
  rm -f .ci/tests.passed .ci/candidate.passed
  git rev-parse HEAD > reports/source-commit.txt
  export DEPLOYED_COMMIT="$(git rev-parse HEAD)"
  printf 'DEPLOYED_COMMIT=%s\n' "$DEPLOYED_COMMIT" > .env.ci
  printf 'MONGO_ROOT_PASSWORD_FILE=%s\n' "$(server_path "$(secret_source mongo_root_password)")" >> .env.ci
  printf 'MONGO_WRITER_PASSWORD_FILE=%s\n' "$(server_path "$(secret_source mongo_writer_password)")" >> .env.ci
  printf 'MONGO_READER_PASSWORD_FILE=%s\n' "$(server_path "$(secret_source mongo_reader_password)")" >> .env.ci
  compose config --quiet
  docker inspect --format '{{.Image}}' lifemap-api-1 > reports/api-image-before.txt
  docker inspect --format '{{range .Config.Env}}{{println .}}{{end}}' lifemap-api-1 | sed -n 's/^DEPLOYED_COMMIT=//p' > .ci/previous-commit
  ;;
 build)
  revision="$(git rev-parse HEAD)"
  compose build mongodb
  compose build spark-master
  compose build dask-scheduler
  docker build --build-arg "VCS_REF=$revision" --target test -f docker/api/Dockerfile -t "$TEST_IMAGE" .
  docker build --build-arg "VCS_REF=$revision" --target runtime -f docker/api/Dockerfile -t "$CANDIDATE_IMAGE" .
  ;;
 test)
  paths=(/app/tests/e1/test_core.py)
  if [[ "${FORCE_TEST_FAILURE:-false}" == true ]]; then paths+=(/app/ci/faults/test_forced_failure.py); fi
  docker create --name "$TEST" --label "lifemap.ci_run=$RUN_ID" --network none --memory 256m --cpus 1 "$TEST_IMAGE" python -m pytest -q --junitxml=/tmp/pytest.xml "${paths[@]}"
  status=0; docker start -a "$TEST" || status=$?
  docker cp "$TEST:/tmp/pytest.xml" reports/pytest.xml
  if [[ "$status" == 0 ]]; then touch .ci/tests.passed; fi
  exit "$status"
  ;;
 infrastructure)
  test -f .ci/tests.passed
  compose up -d --no-build --wait --wait-timeout 180 mongodb spark-master spark-worker dask-scheduler dask-worker-1 dask-worker-2
  ;;
 ingestion)
  set +x
  printf '%s' "$KAGGLE_CREDENTIAL" | docker exec -i lifemap-dask-scheduler-1 python -m ingestion.download --credential-raw-stdin > reports/kaggle-cache.json
  docker cp config/yelp_selection.json lifemap-dask-scheduler-1:/scratch/yelp-approved-scope.json
  docker exec lifemap-dask-scheduler-1 python -m ingestion.run --approved-scope /scratch/yelp-approved-scope.json > reports/ingestion.json
  ;;
 results)
  docker exec lifemap-spark-master-1 /opt/spark/bin/spark-submit /opt/lifemap/ml/train.py > reports/sentiment-job.log 2>&1
  docker exec lifemap-spark-master-1 /opt/spark/bin/spark-submit /opt/lifemap/analytics/job.py > reports/analytics-job.log 2>&1
  docker exec lifemap-spark-master-1 python3 /opt/lifemap/analytics/verify.py > reports/academic-results.json
  ;;
 candidate)
  test -f .ci/tests.passed
  reader="$(secret_source mongo_reader_password)"
  docker create --name "$CANDIDATE" --label "lifemap.ci_run=$RUN_ID" --network lifemap_backend --memory 256m --cpus 0.5 \
    --mount type=volume,src=lifemap_model_store,dst=/models,readonly \
    --mount "type=bind,src=$reader,dst=/run/secrets/mongo_reader_password,readonly" \
    -e MONGO_HOST=mongodb -e MONGO_READER_PASSWORD_FILE=/run/secrets/mongo_reader_password \
    -e SPARK_STATUS_URL=http://spark-master:8080/json/ -e DASK_STATUS_URL=http://dask-scheduler:8787/json/counts.json "$CANDIDATE_IMAGE"
  docker start "$CANDIDATE"
  for attempt in $(seq 1 30); do
    if docker exec "$CANDIDATE" python /app/tests/e1/smoke_api.py --expected-commit "$(git rev-parse HEAD)" > reports/candidate-api.json 2>reports/candidate-api-error.txt; then touch .ci/candidate.passed; break; fi
    sleep 2
  done
  test -f .ci/candidate.passed
  ;;
 deploy)
  test -f .ci/tests.passed; test -f .ci/candidate.passed
  rollback() {
    previous="$(cat reports/api-image-before.txt)"
    docker tag "$previous" lifemap-api:infra-v1
    DEPLOYED_COMMIT="$(cat .ci/previous-commit)" compose up -d --no-deps --no-build --wait --wait-timeout 90 api
    printf 'Previous local API restored after deployment failure.\n' > reports/rollback.txt
  }
  trap 'status=$?; trap - ERR; rollback; exit "$status"' ERR
  docker tag "$CANDIDATE_IMAGE" lifemap-api:infra-v1
  compose up -d --no-deps --no-build --wait --wait-timeout 90 api
  docker exec lifemap-api-1 python /app/tests/e1/smoke_api.py --expected-commit "$(git rev-parse HEAD)" > reports/deployed-api.json
  docker inspect --format '{{.Image}}' lifemap-api-1 > reports/api-image-after.txt
  printf 'Local deployment passed all gates; no public deployment performed.\n' > reports/deployment.txt
  ;;
 cleanup)
  for name in "$TEST" "$CANDIDATE"; do
    if docker inspect "$name" >/dev/null 2>&1; then
      owned="$(docker inspect --format '{{index .Config.Labels "lifemap.ci_run"}}' "$name")"
      [[ "$owned" == "$RUN_ID" ]] || { echo 'Refusing cleanup of unowned container'; exit 2; }
      docker rm -f "$name"
    fi
  done
  ;;
 *) echo 'Unknown stage' >&2; exit 2;;
esac
