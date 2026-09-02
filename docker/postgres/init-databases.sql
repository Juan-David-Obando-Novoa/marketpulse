-- Two logical databases on one server: the Iceberg catalog and Dagster's own
-- run/event storage. They are separated because they have different backup and
-- retention requirements -- losing Dagster's run history is an inconvenience,
-- losing the Iceberg catalog makes every table in the lake unreadable.

CREATE DATABASE iceberg_catalog;
CREATE DATABASE dagster;

\connect iceberg_catalog;
COMMENT ON DATABASE iceberg_catalog IS
  'Iceberg JDBC catalog: namespace and table metadata pointers. Back this up.';

\connect dagster;
COMMENT ON DATABASE dagster IS
  'Dagster run, event and schedule storage.';
