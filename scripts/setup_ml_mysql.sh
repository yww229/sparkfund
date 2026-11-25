#!/usr/bin/env bash
set -euo pipefail
source "$(dirname -- "${BASH_SOURCE[0]}")/common.sh"

check_mysql_client
[[ -n ${DEMO_MYSQL_SETUP_USER:-} ]] || die 'Set DEMO_MYSQL_SETUP_USER in demo.env.'
mysql_client "$DEMO_MYSQL_SETUP_USER" \
    -e "CREATE DATABASE IF NOT EXISTS \`$demo_schema\` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin"
mysql_client "$DEMO_MYSQL_SETUP_USER" --database="$demo_schema" < "$project_dir/sql/setup_ml.sql"
