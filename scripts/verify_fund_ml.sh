#!/usr/bin/env bash
set -euo pipefail
source "$(dirname -- "${BASH_SOURCE[0]}")/common.sh"

run_id=${1:-}
[[ $# -eq 1 ]] && valid_run_id "$run_id" || die 'Use bash scripts/verify_fund_ml.sh RUN_ID.'
check_mysql_client
[[ -n ${DEMO_MYSQL_USER:-} ]] || die 'Set DEMO_MYSQL_USER in demo.env.'

result=$(
    {
        printf "SET @demo_run_id = '%s';\n" "$run_id"
        cat "$project_dir/sql/verify_fund_ml.sql"
    } | mysql_client "$DEMO_MYSQL_USER" --batch --raw --skip-column-names --database="$demo_schema"
)
check_verification "$result"
