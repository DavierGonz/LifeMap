FROM python:3.10.20-slim-bookworm@sha256:019e31cc1f52e4139d338bca44f27b71339a9da261738fd80fab9cf4595152fc
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /app
COPY requirements/producer.lock.txt requirements/mongo.lock.txt /tmp/
RUN python -m pip install --no-cache-dir -r /tmp/producer.lock.txt \
    && useradd --uid 10001 --create-home app
COPY streaming /app/streaming
USER app
ENTRYPOINT ["python", "-m", "streaming.producer"]
