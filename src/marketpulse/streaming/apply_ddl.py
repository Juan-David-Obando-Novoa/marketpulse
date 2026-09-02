"""Apply a SQL file through a Spark session that has no Hive at all.

`spark-sql` is the Hive CLI. It boots a HiveExternalCatalog before running a
single statement, which means an embedded Derby metastore in the working
directory -- and if that directory is not writable, it retries forever rather
than failing. None of this platform uses Hive: the catalog is the Iceberg REST
service, and the only reason Derby appeared is that the tool insists.

Running the DDL through the same session builder the streaming jobs use means
the tables are created with exactly the catalog configuration the jobs will
read them with, which removes a whole class of "works in one engine" surprise.

    spark-submit src/marketpulse/streaming/apply_ddl.py [path/to/file.sql]
"""

from __future__ import annotations

import sys
from pathlib import Path

from marketpulse.config import get_settings
from marketpulse.logging import configure_logging, get_logger

log = get_logger(__name__)

DEFAULT_DDL = Path(__file__).parent / "ddl" / "bronze.sql"


def split_statements(sql: str) -> list[str]:
    """Split a SQL script into executable statements.

    Quote-aware, because it has to be: the bronze DDL carries column comments
    such as 'Spark micro-batch time; differs from ingested_at by the Kafka hop'
    and a naive split on ';' cuts that table definition in half. The failure
    surfaces as a Spark parse error on a fragment, which points nowhere near
    here.

    Handles single-quoted literals with '' escaping and -- line comments
    outside them. Deliberately not a general SQL parser -- it parses this
    project's DDL, and the tests pin exactly that.
    """
    statements: list[str] = []
    current: list[str] = []
    in_string = False
    index = 0
    length = len(sql)

    while index < length:
        char = sql[index]

        if in_string:
            current.append(char)
            if char == "'":
                # '' is an escaped quote, not the end of the literal.
                if index + 1 < length and sql[index + 1] == "'":
                    current.append("'")
                    index += 2
                    continue
                in_string = False
            index += 1
            continue

        if char == "'":
            in_string = True
            current.append(char)
            index += 1
            continue

        if char == "-" and index + 1 < length and sql[index + 1] == "-":
            newline = sql.find("\n", index)
            index = length if newline == -1 else newline
            continue

        if char == ";":
            statements.append("".join(current))
            current = []
            index += 1
            continue

        current.append(char)
        index += 1

    statements.append("".join(current))
    return [statement.strip() for statement in statements if statement.strip()]


def main(argv: list[str] | None = None) -> int:
    args = argv if argv is not None else sys.argv[1:]
    path = Path(args[0]) if args else DEFAULT_DDL

    settings = get_settings()
    configure_logging(
        level=settings.observability.log_level,
        json_logs=settings.observability.json_logs,
        service_name="marketpulse-ddl",
    )

    statements = split_statements(path.read_text(encoding="utf-8"))
    log.info("ddl.loaded", path=str(path), statements=len(statements))

    from marketpulse.streaming.common import build_spark_session  # noqa: PLC0415

    spark = build_spark_session("marketpulse-ddl", settings)
    try:
        for index, statement in enumerate(statements, start=1):
            summary = " ".join(statement.split())[:90]
            log.info("ddl.executing", step=f"{index}/{len(statements)}", statement=summary)
            spark.sql(statement)
    finally:
        spark.stop()

    log.info("ddl.complete", statements=len(statements))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
