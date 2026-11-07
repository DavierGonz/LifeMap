FROM python:3.10.20-slim-bookworm@sha256:019e31cc1f52e4139d338bca44f27b71339a9da261738fd80fab9cf4595152fc
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PIP_DISABLE_PIP_VERSION_CHECK=1
ENV OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1
WORKDIR /app
COPY requirements/dask.lock.txt /tmp/requirements.txt
RUN python -m pip install --no-cache-dir -r /tmp/requirements.txt \
    && useradd --uid 10001 --create-home app \
    && mkdir -p /data /scratch \
    && chown app:root /data /scratch && chmod 2775 /data
COPY scripts/check_dask.py /opt/lifemap/check_dask.py
COPY ingestion /app/ingestion
COPY storage /app/storage
COPY config/yelp_scope_candidates.json /app/config/yelp_scope_candidates.json
COPY tests/unit/test_yelp_ingestion.py /app/tests/test_yelp_ingestion.py
ENV PYTHONPATH=/app
USER app:0
