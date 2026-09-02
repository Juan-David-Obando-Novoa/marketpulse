# ---------------------------------------------------------------------------
# Spark image with the Iceberg, Kafka and S3 runtimes baked in.
#
# The jars are downloaded at BUILD time rather than resolved with
# --packages at submit time. Two reasons, both operational: a streaming job
# that resolves Ivy dependencies on every restart cannot start during a network
# incident, which is exactly when it is restarting; and pinning the coordinates
# here makes the runtime reproducible instead of dependent on what Maven
# Central serves that morning.
# ---------------------------------------------------------------------------
FROM apache/spark:3.5.3-scala2.12-java11-python3-ubuntu

USER root

ARG ICEBERG_VERSION=1.9.2
ARG SPARK_VERSION=3.5.3
ARG SCALA_VERSION=2.12
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.262

ENV SPARK_JARS=/opt/spark/jars
ENV MAVEN=https://repo1.maven.org/maven2

RUN set -eux; \
    apt-get update && apt-get install -y --no-install-recommends curl ca-certificates && \
    rm -rf /var/lib/apt/lists/*; \
    # Iceberg: the Spark runtime plus the AWS bundle that provides S3FileIO.
    curl -fsSL -o "${SPARK_JARS}/iceberg-spark-runtime.jar" \
      "${MAVEN}/org/apache/iceberg/iceberg-spark-runtime-3.5_${SCALA_VERSION}/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_${SCALA_VERSION}-${ICEBERG_VERSION}.jar"; \
    curl -fsSL -o "${SPARK_JARS}/iceberg-aws-bundle.jar" \
      "${MAVEN}/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar"; \
    # Kafka source. spark-sql-kafka has three transitive jars that --packages
    # would have fetched; naming them explicitly is what makes the image
    # self-contained.
    curl -fsSL -o "${SPARK_JARS}/spark-sql-kafka-0-10.jar" \
      "${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_${SCALA_VERSION}/${SPARK_VERSION}/spark-sql-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar"; \
    curl -fsSL -o "${SPARK_JARS}/spark-token-provider-kafka-0-10.jar" \
      "${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_VERSION}/${SPARK_VERSION}/spark-token-provider-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar"; \
    curl -fsSL -o "${SPARK_JARS}/kafka-clients.jar" \
      "${MAVEN}/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar"; \
    curl -fsSL -o "${SPARK_JARS}/commons-pool2.jar" \
      "${MAVEN}/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar"; \
    # Avro, for from_avro on the Confluent-framed payloads.
    curl -fsSL -o "${SPARK_JARS}/spark-avro.jar" \
      "${MAVEN}/org/apache/spark/spark-avro_${SCALA_VERSION}/${SPARK_VERSION}/spark-avro_${SCALA_VERSION}-${SPARK_VERSION}.jar"; \
    # s3a, used only for streaming checkpoints. Iceberg data goes through
    # S3FileIO above; checkpoints need a Hadoop FileSystem implementation.
    curl -fsSL -o "${SPARK_JARS}/hadoop-aws.jar" \
      "${MAVEN}/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar"; \
    curl -fsSL -o "${SPARK_JARS}/aws-java-sdk-bundle.jar" \
      "${MAVEN}/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar"

COPY docker/spark/spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY docker/spark/log4j2.properties /opt/spark/conf/log4j2.properties

# The application source is baked in AND reachable through PYTHONPATH rather
# than pip-installed. Two reasons: the Spark base image ships a pip too old to
# install a PEP 517 project in editable mode, and more importantly the compose
# file bind-mounts ./src over this path for local iteration -- an installed
# copy in site-packages would shadow the mount depending on sys.path order,
# which presents as "my edit did nothing" and is genuinely unpleasant to
# diagnose. One copy, one path.
#
# Only the runtime dependencies are installed; the driver needs no console
# scripts, since jobs are submitted as file paths.
COPY pyproject.toml README.md /opt/marketpulse/
COPY src /opt/marketpulse/src
RUN pip install --no-cache-dir "pydantic>=2.7,<3" "pydantic-settings>=2.3,<3" \
        "structlog>=24.1" "fastavro>=1.9" "orjson>=3.10" "prometheus-client>=0.20"

ENV PYTHONPATH=/opt/marketpulse/src:$PYTHONPATH
ENV PYSPARK_PYTHON=python3

# The base image's entrypoint puts /opt/spark/bin on PATH, but `docker exec`
# does not run the entrypoint -- so spark-sql and spark-submit are missing for
# every operational command and for the Dagster assets that shell in. Setting
# it on the image makes both work without absolute paths everywhere.
ENV PATH=/opt/spark/bin:/opt/spark/sbin:$PATH

WORKDIR /opt/marketpulse
USER spark
