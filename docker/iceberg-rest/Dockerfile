# ---------------------------------------------------------------------------
# Iceberg REST catalog with a PostgreSQL JDBC driver.
#
# The upstream apache/iceberg-rest-fixture image defaults to an in-memory
# SQLite catalog and ships no Postgres driver, so pointing CATALOG_URI at
# postgres:// dies at start-up with "No suitable driver found". Known gap,
# tracked as https://github.com/apache/iceberg/issues/14802, whose documented
# workaround is a custom image.
#
# Why not just accept SQLite: the catalog is the one piece of state whose loss
# makes every table in the lake unreadable (ADR-0002). In-memory loses every
# table on restart, and a SQLite file has no concurrent-writer story worth
# relying on. Postgres is already running here for Dagster, so the marginal
# cost is one jar.
# ---------------------------------------------------------------------------
FROM apache/iceberg-rest-fixture:1.9.2

ARG POSTGRES_JDBC_VERSION=42.7.4
ARG MAVEN=https://repo1.maven.org/maven2

# The base image ends on USER iceberg:iceberg (uid 1000) and chowns only the
# adapter jar, so /usr/lib/iceberg-rest itself is not writable -- a download
# there fails with curl exit 23, "failure writing output", which reads like a
# network error and is not one. Step up to root, install into a directory of
# our own, then drop back to exactly the user the base declared.
USER root
RUN mkdir -p /opt/jdbc \
 && curl -fsSL -o /opt/jdbc/postgresql.jar \
      "${MAVEN}/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar" \
 && chmod 0644 /opt/jdbc/postgresql.jar
USER iceberg:iceberg

# `java -jar` ignores -cp entirely, so adding the driver to the image is not
# enough on its own: the command has to become an explicit classpath plus the
# main class. The base declares CMD (not ENTRYPOINT), so overriding CMD is the
# minimal change and leaves compose's `command:` semantics intact.
CMD ["java", \
  "-cp", "/usr/lib/iceberg-rest/iceberg-rest-adapter.jar:/opt/jdbc/postgresql.jar", \
  "org.apache.iceberg.rest.RESTCatalogServer"]
