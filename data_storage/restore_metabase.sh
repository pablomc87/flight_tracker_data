#!/bin/bash

# Restore PostgreSQL data
source .env
docker compose exec -T postgres_metabase pg_restore -U ${MB_DB_USER} -d ${MB_DB_DBNAME} -c < data_storage/metabase_backup.dump