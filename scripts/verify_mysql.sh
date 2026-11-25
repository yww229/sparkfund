#!/usr/bin/env bash
set -euo pipefail
source "$(dirname -- "${BASH_SOURCE[0]}")/common.sh"

run_id=${1:-}
valid_run_id "$run_id" || die 'Use RUN_ID JOB [DATE_FROM DATE_TO [STRATEGY]].'
shift
select_job_window "$@"
case "$job:$strategy" in
    customer_fund:direct|daily_fund_type:direct|daily_fund_type:preagg) ;;
    *) die "Unsupported job/strategy combination: $job/$strategy" ;;
esac
check_mysql_client
[[ -n ${DEMO_MYSQL_USER:-} ]] || die 'Set DEMO_MYSQL_USER in demo.env.'

result=$(
    {
        printf "SET @demo_run_id = '%s', @demo_job = '%s', @demo_strategy = '%s';\n" "$run_id" "$job" "$strategy"
        printf "SET @demo_from = '%s', @demo_to = '%s';\n" "$date_from" "$date_to"
        cat "$project_dir/sql/verify.sql"
    } | mysql_client "$DEMO_MYSQL_USER" --batch --raw --skip-column-names --database="$demo_schema"
)
check_verification "$result"
