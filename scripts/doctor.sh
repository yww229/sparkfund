#!/usr/bin/env bash
# Pre-flight check. No Spark job is submitted; the model directory may be created.
set -uo pipefail
source "$(dirname -- "${BASH_SOURCE[0]}")/common.sh"

failures=0
pass() { printf 'OK    %s\n' "$*"; }
skip() { printf 'SKIP  %s\n' "$*"; }
fail() { printf 'FAIL  %s\n' "$*"; failures=$((failures + 1)); }

printf 'project_dir=%s\n\n' "$project_dir"

if ! command -v java >/dev/null 2>&1; then
    fail 'java not on PATH (Spark 2.4.3 needs Java 8)'
else
    java_version=$(java -version 2>&1 | head -1)
    if [[ $java_version == *'"1.8.'* ]]; then
        pass "java $java_version"
    else
        fail "java is ${java_version}; Spark 2.4.3 expects 1.8"
    fi
fi

spark_submit=${DEMO_SPARK_SUBMIT:-spark-submit}
if command -v "$spark_submit" >/dev/null 2>&1; then
    pass "spark-submit=$(command -v "$spark_submit")"
else
    fail 'spark-submit not found; install Spark 2.4.3 or set DEMO_SPARK_SUBMIT'
fi

pack_python=${DEMO_PACK_PYTHON:-python3}
if command -v "$pack_python" >/dev/null 2>&1; then
    pass "DEMO_PACK_PYTHON=$pack_python ($("$pack_python" -c 'import sys; print(sys.version.split()[0])'))"
else
    fail 'DEMO_PACK_PYTHON is not runnable; the entrypoint needs it to bundle modules'
fi

if [[ ${SPARK_MASTER:-} == spark://* ]]; then
    pass "SPARK_MASTER=$SPARK_MASTER"
else
    fail 'SPARK_MASTER must look like spark://host:port'
fi

if [[ -r ${SPARK_MYSQL_JAR:-} ]]; then
    pass "SPARK_MYSQL_JAR=$SPARK_MYSQL_JAR"
else
    fail 'SPARK_MYSQL_JAR is unset or unreadable (Connector/J 8.0.x)'
fi

python_bin=${PYSPARK_PYTHON:-}
if [[ -z $python_bin ]]; then
    fail 'PYSPARK_PYTHON is unset (run source demo.env first)'
elif [[ ! -x $python_bin ]]; then
    fail "PYSPARK_PYTHON=$python_bin does not exist or is not executable"
else
    python_version=$("$python_bin" -c 'import sys; print("%d.%d" % sys.version_info[:2])' 2>/dev/null)
    case ${python_version:-} in
        3.[4-7]) pass "PYSPARK_PYTHON=$python_bin (Python $python_version)" ;;
        *) fail "PYSPARK_PYTHON=$python_bin runs Python ${python_version:-unknown}; need 3.4-3.7" ;;
    esac
    if "$python_bin" -c 'import numpy' >/dev/null 2>&1; then
        pass 'numpy available in PYSPARK_PYTHON'
    else
        fail "numpy missing in $python_bin"
    fi
fi

if [[ -z ${PYSPARK_DRIVER_PYTHON:-} ]]; then
    fail 'PYSPARK_DRIVER_PYTHON is unset (run source demo.env first)'
elif [[ ! -x ${PYSPARK_DRIVER_PYTHON:-} ]]; then
    fail "PYSPARK_DRIVER_PYTHON=$PYSPARK_DRIVER_PYTHON does not exist or is not executable"
else
    pass "PYSPARK_DRIVER_PYTHON=$PYSPARK_DRIVER_PYTHON"
fi

if [[ -n ${PYTHONPATH:-} ]]; then
    if [[ -d $PYTHONPATH ]]; then
        pass "PYTHONPATH=$PYTHONPATH (forwarded to executors by run.sh)"
    else
        fail "PYTHONPATH=$PYTHONPATH does not exist; executors cannot import numpy"
    fi
else
    skip 'PYTHONPATH is empty; set it if numpy lives outside the interpreter site-packages'
fi

resolve_code_version && pass "code_version=$code_version"

if [[ -n ${DEMO_MODEL_ROOT:-} ]]; then
    case $DEMO_MODEL_ROOT in
        hdfs://*|s3a://*|oss://*)
            skip "DEMO_MODEL_ROOT=$DEMO_MODEL_ROOT (remote storage; local writability not tested)"
            ;;
        *)
            model_path=${DEMO_MODEL_ROOT#file://}
            if mkdir -p "$model_path" 2>/dev/null && [[ -w $model_path ]]; then
                pass "DEMO_MODEL_ROOT=$DEMO_MODEL_ROOT"
            else
                fail "DEMO_MODEL_ROOT=$DEMO_MODEL_ROOT is not writable"
            fi
            ;;
    esac
else
    fail 'DEMO_MODEL_ROOT is unset (required by fund_purchase_lr)'
fi

if mkdir -p "$project_dir/logs" 2>/dev/null && [[ -w $project_dir/logs ]]; then
    pass "logs directory=$project_dir/logs"
else
    fail "logs directory is not writable: $project_dir/logs"
fi

if [[ -n ${DEMO_MYSQL_HOST:-} ]]; then
    pass "DEMO_MYSQL_HOST=$DEMO_MYSQL_HOST:$demo_port schema=$demo_schema"
else
    fail 'DEMO_MYSQL_HOST is unset'
fi

if command -v mysql >/dev/null 2>&1; then
    pass 'mysql CLI found'
else
    fail 'mysql CLI missing from PATH'
fi

setup_user=${DEMO_MYSQL_SETUP_USER:-}
if [[ -z $setup_user ]]; then
    fail 'DEMO_MYSQL_SETUP_USER is unset'
elif [[ -n ${DEMO_MYSQL_HOST:-} ]]; then
    if [[ -z ${DEMO_MYSQL_SETUP_PASSWORD:-} && ! -r ${HOME:-/nonexistent}/.my.cnf && ! -t 0 ]]; then
        skip "MySQL login for $setup_user not tested (no password, no ~/.my.cnf, not a terminal)"
    elif mysql_client "$setup_user" --batch --skip-column-names -e 'SELECT 1' </dev/null >/dev/null 2>&1; then
        pass "MySQL login works for $setup_user"
        if [[ $(mysql_client "$setup_user" --batch --skip-column-names </dev/null \
            -e "SHOW DATABASES LIKE '$demo_schema'" 2>/dev/null) == "$demo_schema" ]]; then
            pass "schema $demo_schema exists"
        else
            skip "schema $demo_schema not created yet (scripts/setup_mysql.sh creates it)"
        fi
    else
        fail "MySQL login failed for $setup_user (check DEMO_MYSQL_* and ~/.my.cnf)"
    fi
fi

printf '\n'
if (( failures > 0 )); then
    printf '%d check(s) failed.\n' "$failures"
    exit 1
fi
printf 'All checks passed.\n'
