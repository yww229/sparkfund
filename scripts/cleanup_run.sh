#!/usr/bin/env bash
# Remove one run's rows and its model directory so the ID can be reused.
set -euo pipefail
source "$(dirname -- "${BASH_SOURCE[0]}")/common.sh"

run_id=${1:-}
valid_run_id "$run_id" || die 'Use RUN_ID [--yes].'
check_mysql_client
[[ -n ${DEMO_MYSQL_USER:-} ]] || die 'Set DEMO_MYSQL_USER in demo.env.'

if [[ ${2:-} != --yes ]]; then
    [[ -t 0 ]] || die 'Pass --yes when stdin is not a terminal.'
    read -r -p "Delete all rows for run $run_id in $demo_schema? [y/N] " answer
    [[ $answer == [yY] ]] || die 'Cancelled.'
fi

{
    printf "SET @demo_run_id = '%s';\n" "$run_id"
    cat "$project_dir/sql/cleanup.sql"
} | mysql_client "$DEMO_MYSQL_USER" --table --database="$demo_schema"

if [[ -n ${DEMO_MODEL_ROOT:-} ]]; then
    case $DEMO_MODEL_ROOT in
        hdfs://*|s3a://*|oss://*)
            printf 'SKIP: remove %s/%s on its remote storage if needed.\n' "$DEMO_MODEL_ROOT" "$run_id"
            ;;
        *)
            model_path=${DEMO_MODEL_ROOT#file://}/$run_id
            if [[ -d $model_path ]]; then
                rm -rf -- "$model_path"
                printf 'Removed %s\n' "$model_path"
            fi
            ;;
    esac
fi

printf 'Run %s cleaned; logs/%s.log is kept for the audit trail.\n' "$run_id" "$run_id"
