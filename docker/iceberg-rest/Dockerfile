# ---------------------------------------------------------------------------
# Iceberg REST catalog with a PostgreSQL JDBC driver.
#
# The upstream apache/iceberg-rest-fixture image defaults to an in-memory
# SQLite catalog and does not ship a Postgres driver, so pointing CATALOG_URI
# at postgres:// fails at start-up with "No suitable driver found". That is a
# known gap -- https://github.com/apache/iceberg/issues/14802 -- and a custom
# image is the documented workaround.
#
# Why bother, rather than accepting SQLite: the catalog is the one piece of
# state whose loss makes every table in the lake unreadable (ADR-0002). An
# in-memory catalog loses every table on restart; a SQLite file has no
# concurrent-writer story worth relying on. Postgres is already running here
# for Dagster, so the marginal cost is one jar.
# ---------------------------------------------------------------------------
FROM apache/iceberg-rest-fixture:1.9.2

ARG POSTGRES_JDBC_VERSION=42.7.4
ARG MAVEN=https://repo1.maven.org/maven2

# Fetched with curl rather than ADD so the file mode is ours to set: ADD from a
# URL leaves the file 0600 and root-owned, which a non-root runtime cannot read.
RUN curl -fsSL -o /usr/lib/iceberg-rest/postgresql.jar \
      "${MAVEN}/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar" \
 && chmod 644 /usr/lib/iceberg-rest/postgresql.jar

# `java -jar` ignores -cp entirely, so adding a jar to the image is not enough:
# the entrypoint has to become an explicit classpath plus the main class. Paths
# are absolute so this does not depend on the base image's WORKDIR.
ENTRYPOINT ["java", \
  "-cp", "/usr/lib/iceberg-rest/iceberg-rest-adapter.jar:/usr/lib/iceberg-rest/postgresql.jar", \
  "org.apache.iceberg.rest.RESTCatalogServer"]
