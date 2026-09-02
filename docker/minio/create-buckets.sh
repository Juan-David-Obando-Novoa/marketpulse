#!/bin/sh
# Idempotent bucket provisioning. Runs to completion and exits; services that
# need the buckets depend on it with service_completed_successfully.
set -eu

BUCKET="${MP_S3__BUCKET:-lakehouse}"

mc alias set local http://minio:9000 "${AWS_ACCESS_KEY_ID}" "${AWS_SECRET_ACCESS_KEY}"

mc mb --ignore-existing "local/${BUCKET}"
mc mb --ignore-existing "local/${BUCKET}-quarantine"

# Versioning on the warehouse bucket is a safety net that is separate from
# Iceberg snapshots: snapshots protect against a bad commit, versioning
# protects against a bad delete of the underlying objects.
mc version enable "local/${BUCKET}" || true

echo "buckets ready:"
mc ls local
