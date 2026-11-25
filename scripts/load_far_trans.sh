#!/usr/bin/env bash
set -euo pipefail
source "$(dirname -- "${BASH_SOURCE[0]}")/common.sh"

[[ $# -le 1 ]] || die 'Usage: bash scripts/load_far_trans.sh [FAR-Trans.zip]'
check_mysql_client
[[ -n ${DEMO_MYSQL_SETUP_USER:-} ]] || die 'Set DEMO_MYSQL_SETUP_USER in demo.env.'
pack_python=${DEMO_PACK_PYTHON:-python3}
command -v "$pack_python" >/dev/null || die 'Python 3 is required to import FAR-Trans.'

# Resolve a supplied relative ZIP path before entering the project directory.
if [[ $# -eq 1 ]]; then
    zip_dir=$(cd -- "$(dirname -- "$1")" && pwd)
    set -- "$zip_dir/$(basename -- "$1")"
fi
cd "$project_dir"
"$pack_python" -m spark_project.fund_ml.import_far_trans "$@" \
    | mysql_client "$DEMO_MYSQL_SETUP_USER" --skip-force --database="$demo_schema"
