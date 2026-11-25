#!/usr/bin/env bash
set -euo pipefail
source "$(dirname -- "${BASH_SOURCE[0]}")/common.sh"

select_job_window "$@"
check_mysql_host
[[ -n ${DEMO_MYSQL_USER:-} ]] || die 'Set DEMO_MYSQL_USER in demo.env.'
[[ ${SPARK_MASTER:-} == spark://* ]] || die 'Set SPARK_MASTER to the Standalone master spark:// URL.'
[[ -f ${SPARK_MYSQL_JAR:-} && -r ${SPARK_MYSQL_JAR:-} ]] || die 'Set SPARK_MYSQL_JAR to a readable Connector/J JAR.'
[[ -n ${PYSPARK_PYTHON:-} ]] || die 'Set PYSPARK_PYTHON to the Spark-compatible Python in demo.env.'
[[ -n ${PYSPARK_DRIVER_PYTHON:-} ]] || die 'Set PYSPARK_DRIVER_PYTHON to the Spark-compatible Python in demo.env.'
spark_submit=${DEMO_SPARK_SUBMIT:-spark-submit}
pack_python=${DEMO_PACK_PYTHON:-python3}
command -v "$spark_submit" >/dev/null || die 'Spark submit missing from PATH. Set DEMO_SPARK_SUBMIT to its absolute path.'
command -v "$pack_python" >/dev/null || die 'Python 3 is required to build the Python bundle.'

# An explicit DEMO_RUN_ID lets an operator retry a cleaned-up run without changing the ID.
run_id=${DEMO_RUN_ID:-$("$pack_python" -c 'from uuid import uuid4; print(uuid4().hex)')}
valid_run_id "$run_id" || die 'DEMO_RUN_ID must use 1-64 letters, digits, underscores or hyphens.'
resolve_code_version

build_dir=$(mktemp -d)
trap 'rm -f -- "$build_dir/spark_project.zip"; rmdir -- "$build_dir"' EXIT
bundle="$build_dir/spark_project.zip"

# Ship project modules to Spark in an importable ZIP.
"$pack_python" - "$project_dir" "$bundle" <<'PY'
import sys
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

root = Path(sys.argv[1])
with ZipFile(sys.argv[2], "w", ZIP_DEFLATED) as archive:
    for path in sorted((root / "spark_project").rglob("*.py")):
        archive.write(str(path), path.relative_to(root).as_posix())
PY

if [[ -z ${DEMO_MYSQL_PASSWORD:-} ]]; then
    [[ -t 0 ]] || die 'Use an interactive shell to enter the MySQL password.'
    read -rsp "MySQL password for $DEMO_MYSQL_USER: " DEMO_MYSQL_PASSWORD
    printf '\n'
fi
[[ -n $DEMO_MYSQL_PASSWORD ]] || die 'Empty MySQL password.'
export DEMO_MYSQL_USER DEMO_MYSQL_PASSWORD
export DEMO_JDBC_URL="jdbc:mysql://${DEMO_MYSQL_HOST}:${demo_port}/${demo_schema}"
if [[ -n ${DEMO_JDBC_OPTIONS:-} ]]; then
    DEMO_JDBC_URL+="?${DEMO_JDBC_OPTIONS}"
fi

set --
if [[ ${DEMO_SHOW_PLAN:-0} == 1 ]]; then
    set -- --plan
fi
# pyspark.ml.linalg imports NumPy, so Python workers need the extra site directory too.
executor_confs=()
if [[ -n ${PYTHONPATH:-} ]]; then
    executor_confs+=(--conf "spark.executorEnv.PYTHONPATH=$PYTHONPATH")
fi

mkdir -p "$project_dir/logs"
log_file="$project_dir/logs/$run_id.log"

{
    printf 'SUBMIT_RUN_ID=%s CODE_VERSION=%s LOG_PATH=%s\n' "$run_id" "$code_version" "$log_file"
    printf 'VERIFY_COMMAND=%s\n' "$(verify_command_for "$job" "$run_id" "$date_from" "$date_to" "$strategy")"
    "$spark_submit" \
        --master "$SPARK_MASTER" --deploy-mode client \
        --jars "$SPARK_MYSQL_JAR" --py-files "$bundle" \
        --driver-memory 1g --executor-memory 1g --executor-cores 1 --total-executor-cores 1 \
        --conf spark.sql.shuffle.partitions=2 --conf spark.speculation=false --conf spark.task.maxFailures=1 \
        --conf "spark.pyspark.python=$PYSPARK_PYTHON" --conf "spark.pyspark.driver.python=$PYSPARK_DRIVER_PYTHON" \
        "${executor_confs[@]}" \
        "$project_dir/app.py" --job "$job" --run-id "$run_id" --code-version "$code_version" \
        --log-path "$log_file" \
        --date-from "$date_from" --date-to "$date_to" --strategy "$strategy" "$@"
} 2>&1 | tee "$log_file"
