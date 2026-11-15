#!/bin/bash
set -e

DATA_DIR=/var/lib/postgresql/data

if [ -z "$(ls -A "$DATA_DIR")" ]; then
  echo "Data dir empty, cloning from primary..."
  pg_basebackup -h pg-primary -p 5432 -U replicator -D "$DATA_DIR" -R -X stream -P
  chmod 700 "$DATA_DIR"
fi

exec postgres