#!/usr/bin/env bash
set -euo pipefail
source "$(dirname -- "${BASH_SOURCE[0]}")/common.sh"

direct_run=${1:-}
preagg_run=${2:-}
[[ $# -eq 2 ]] && valid_run_id "$direct_run" && valid_run_id "$preagg_run" || die 'Use DIRECT_RUN_ID PREAGG_RUN_ID.'
check_mysql_client
[[ -n ${DEMO_MYSQL_USER:-} ]] || die 'Set DEMO_MYSQL_USER in demo.env.'

result=$(
    {
        printf "SET @demo_direct_run = '%s', @demo_preagg_run = '%s';\n" "$direct_run" "$preagg_run"
        cat "$project_dir/sql/compare_daily.sql"
    } | mysql_client "$DEMO_MYSQL_USER" --batch --raw --skip-column-names --database="$demo_schema"
)
check_verification "$result"
