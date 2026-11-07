FROM spark:3.5.8-java17-python3@sha256:5363cd7d22758958565206f67a764356ebebb9fa8f5721e9c55aa86a78efafda
USER root
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PIP_DISABLE_PIP_VERSION_CHECK=1
COPY requirements/spark.lock.txt requirements/mongo.lock.txt /tmp/
RUN python3 -m pip install --no-cache-dir -r /tmp/spark.lock.txt
COPY docker/spark/install_connector.py /opt/lifemap/install_connector.py
RUN python3 /opt/lifemap/install_connector.py \
    && mkdir -p /data /models /scratch \
    && chown -R 185:0 /data /models /scratch && chmod 2775 /data /models
COPY docker/spark/spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY scripts/check_spark.py /opt/lifemap/check_spark.py
COPY ml /opt/lifemap/ml
COPY analytics /opt/lifemap/analytics
COPY storage /opt/lifemap/storage
ENV PYTHONPATH=/opt/lifemap
ENV SPARK_DAEMON_MEMORY=256m SPARK_LOCAL_DIRS=/scratch PYSPARK_PYTHON=python3
USER 185:0
