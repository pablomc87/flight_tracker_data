#!/bin/bash

# Backup PostgreSQL data
source .env
docker compose exec -t postgres_metabase pg_dump -U ${MB_DB_USER} -Fc ${MB_DB_DBNAME} > data_storage/metabase_backup.dump
