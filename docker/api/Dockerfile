FROM python:3.10.20-slim-bookworm@sha256:019e31cc1f52e4139d338bca44f27b71339a9da261738fd80fab9cf4595152fc AS base
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /app
COPY requirements/api.lock.txt requirements/mongo.lock.txt /tmp/
RUN python -m pip install --no-cache-dir -r /tmp/api.lock.txt \
    && useradd --uid 10001 --create-home app
ARG VCS_REF=unreleased
LABEL org.opencontainers.image.revision=$VCS_REF
ENV DEPLOYED_COMMIT=$VCS_REF
COPY api /app/api
COPY ml /app/ml
COPY analytics /app/analytics
COPY storage /app/storage
COPY tests/e1 /app/tests/e1
COPY ci/__init__.py ci/azure_promote.py /app/ci/
ENV PYTHONPATH=/app
USER app
EXPOSE 5000
CMD ["gunicorn", "--bind", "0.0.0.0:5000", "--workers", "1", "--threads", "2", "--timeout", "15", "--access-logfile", "-", "--error-logfile", "-", "api.wsgi:app"]

FROM base AS test
USER root
COPY requirements/test.lock.txt requirements/api.lock.txt requirements/mongo.lock.txt /tmp/
COPY ci/faults /app/ci/faults
RUN python -m pip install --no-cache-dir -r /tmp/test.lock.txt
USER app

FROM base AS runtime
