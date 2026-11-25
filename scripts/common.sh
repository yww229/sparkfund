#!/usr/bin/env bash

project_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)

die() {
    printf 'ERROR: %s\n' "$*" >&2
    exit 1
}

demo_schema=${DEMO_MYSQL_SCHEMA:-spark_project_demo}
[[ $demo_schema =~ ^[A-Za-z0-9_]{1,64}$ ]] || die 'DEMO_MYSQL_SCHEMA must use letters, digits or underscores.'
demo_port=${DEMO_MYSQL_PORT:-3306}
default_from=${DEMO_DATE_FROM:-2025-01-06}
default_to=${DEMO_DATE_TO:-2025-01-31}

select_job_window() {
    [[ $# -eq 1 || $# -eq 3 || $# -eq 4 ]] || die 'Use JOB [DATE_FROM DATE_TO [STRATEGY]].'
    job=$1
    date_from=${2-$default_from}
    date_to=${3-$default_to}
    strategy=${4-direct}
    [[ $job =~ ^[a-z][a-z0-9_]{0,31}$ ]] || die 'Invalid job name.'
    [[ $strategy == direct || $strategy == preagg ]] || die 'Use direct or preagg.'
    local date_format='^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
    [[ $date_from =~ $date_format && $date_to =~ $date_format ]] || die 'Use YYYY-MM-DD for both dates.'
    [[ ! $date_from > $date_to ]] || die 'First date must be on or before last date.'
}

valid_run_id() {
    [[ $1 =~ ^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$ ]]
}

check_mysql_host() {
    [[ -n ${DEMO_MYSQL_HOST:-} ]] || die 'Set DEMO_MYSQL_HOST in demo.env and run source demo.env.'
    [[ $demo_port =~ ^[1-9][0-9]{0,4}$ ]] && (( demo_port <= 65535 )) || die 'Invalid DEMO_MYSQL_PORT.'
}

mysql_client() {
    local account=$1 password=${DEMO_MYSQL_PASSWORD:-}
    shift
    if [[ -n ${DEMO_MYSQL_SETUP_USER:-} && $account == "$DEMO_MYSQL_SETUP_USER" ]]; then
        password=${DEMO_MYSQL_SETUP_PASSWORD:-$password}
    fi
    local args=(--protocol=TCP --host="$DEMO_MYSQL_HOST" --port="$demo_port"
        --user="$account" --connect-timeout=8)
    # Keep the password out of `ps`: MYSQL_PWD, then ~/.my.cnf, then a prompt.
    if [[ -n $password ]]; then
        MYSQL_PWD="$password" mysql "${args[@]}" "$@"
    elif [[ -r ${HOME:-/nonexistent}/.my.cnf ]]; then
        mysql "${args[@]}" "$@"
    else
        mysql "${args[@]}" --password "$@"
    fi
}

check_mysql_client() {
    check_mysql_host
    command -v mysql >/dev/null || die 'MySQL CLI missing from PATH.'
}

check_verification() {
    local result=$1
    printf '%s\n' "$result"
    [[ ${result##*$'\n'} == PASS ]] || die 'Independent MySQL verification failed.'
}

content_digest() {
    local python_bin=${DEMO_PACK_PYTHON:-python3}
    "$python_bin" - "$project_dir" <<'PY'
import hashlib
import sys
from pathlib import Path

root = Path(sys.argv[1])
paths = [root / "app.py"]
paths += sorted((root / "spark_project").rglob("*.py"))
paths += sorted((root / "scripts").glob("*.sh"))
paths += sorted((root / "sql").glob("*.sql"))

digest = hashlib.sha256()
for path in sorted(set(paths)):
    digest.update(path.relative_to(root).as_posix().encode("utf-8"))
    digest.update(b"\0")
    digest.update(path.read_bytes())
print(digest.hexdigest())
PY
}

resolve_code_version() {
    if [[ -d $project_dir/.git ]]; then
        command -v git >/dev/null || die 'Git is required to identify the checked-out revision.'
        local commit tracked_changes
        commit=$(git -C "$project_dir" rev-parse HEAD)
        tracked_changes=$(git -C "$project_dir" status --porcelain --untracked-files=normal -- app.py spark_project scripts sql)
        if [[ -n $tracked_changes ]]; then
            code_version="${commit:0:12}-dirty"
        else
            code_version=$commit
        fi
    elif [[ -r $project_dir/REVISION ]]; then
        local base
        base=$(tr -d '[:space:]' < "$project_dir/REVISION")
        code_version="${base:0:12}-$(content_digest | cut -c1-8)"
    else
        code_version="snapshot-$(content_digest | cut -c1-12)"
    fi
    [[ $code_version =~ ^([0-9a-f]{40}|[0-9a-f]{12}-(dirty|[0-9a-f]{8})|snapshot-[0-9a-f]{12})$ ]] \
        || die 'The code revision could not be identified.'
}

verify_command_for() {
    local job=$1 run_id=$2 date_from=$3 date_to=$4 strategy=$5
    if [[ $job == fund_purchase_lr ]]; then
        printf 'bash scripts/verify_fund_ml.sh %s' "$run_id"
    else
        printf 'bash scripts/verify_mysql.sh %s %s %s %s %s' \
            "$run_id" "$job" "$date_from" "$date_to" "$strategy"
    fi
}
