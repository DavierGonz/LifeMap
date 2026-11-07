FROM python:3.10.20-slim-bookworm@sha256:019e31cc1f52e4139d338bca44f27b71339a9da261738fd80fab9cf4595152fc
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_DISABLE_PIP_VERSION_CHECK=1
COPY requirements/report.lock.txt /tmp/report.lock.txt
RUN python -m pip install --no-cache-dir -r /tmp/report.lock.txt
WORKDIR /app
