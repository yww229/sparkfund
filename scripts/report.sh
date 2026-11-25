#!/usr/bin/env bash
# Print one run's manifest, result counts, ML metrics and log path.
set -euo pipefail
source "$(dirname -- "${BASH_SOURCE[0]}")/common.sh"

run_id=${1:-}
valid_run_id "$run_id" || die 'Use RUN_ID.'
check_mysql_client
[[ -n ${DEMO_MYSQL_USER:-} ]] || die 'Set DEMO_MYSQL_USER in demo.env.'

{
    printf "SET @demo_run_id = '%s';\n" "$run_id"
    cat "$project_dir/sql/report.sql"
} | mysql_client "$DEMO_MYSQL_USER" --table --database="$demo_schema"
