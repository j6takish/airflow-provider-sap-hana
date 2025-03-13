#!/bin/bash

set -euo pipefail
HANA_USER="SYSTEM"
HANA_PASSWORD="${MASTER_PASSWORD}"
HANA_INSTANCE="90"
HANA_DATABASE="HXE"
HDBSQL_PATH="/usr/sap/HXE/HDB${HANA_INSTANCE}/exe/hdbsql"

${HDBSQL_PATH} -a -x -i ${HANA_INSTANCE} -d ${HANA_DATABASE} -u ${HANA_USER} -p "${HANA_PASSWORD}" -B UTF8 \
"
  DO
    BEGIN
      DECLARE schemaExists INTEGER := 0;

      SELECT COUNT(*) INTO schemaExists
      FROM sys.schemas
      WHERE schema_name = 'AIRFLOW';

      IF schemaExists = 0 THEN
        EXEC 'CREATE SCHEMA airflow';
      END IF;
    END;
"
