#!/usr/bin/env bash
# One-command walkthrough: doctor -> schema -> data -> ETL -> verify -> ML -> verify.
set -euo pipefail
here=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
source "$here/common.sh"

printf '== 1/6 environment check ==\n'
bash "$here/doctor.sh"

printf '\n== 2/6 demo schema ==\n'
trade_rows=$(mysql_client "${DEMO_MYSQL_SETUP_USER:?Set DEMO_MYSQL_SETUP_USER}" \
    --batch --skip-column-names </dev/null \
    -e "SELECT COUNT(*) FROM $demo_schema.demo_fund_trade" 2>/dev/null || true)
if [[ ${trade_rows:-0} -ge 1540 ]]; then
    printf 'SKIP: %s.demo_fund_trade already has %s rows\n' "$demo_schema" "$trade_rows"
else
    bash "$here/setup_mysql.sh"
fi

printf '\n== 3/6 FAR-Trans dataset ==\n'
bash "$here/setup_ml_mysql.sh"
dataset="$project_dir/data/FAR-Trans.zip"
if [[ -r $dataset ]]; then
    bash "$here/load_far_trans.sh" "$dataset"
else
    printf 'FAR-Trans.zip not found under data/; downloading the pinned release.\n'
    bash "$here/load_far_trans.sh"
fi

printf '\n== 4/6 ETL job and verification ==\n'
etl_log=$(mktemp)
ml_log=$(mktemp)
trap 'rm -f -- "$etl_log" "$ml_log"' EXIT
bash "$here/run.sh" customer_fund 2>&1 | tee "$etl_log"
etl_run=$(sed -n 's/^SUBMIT_RUN_ID=\([^ ]*\).*/\1/p' "$etl_log" | tail -1)
[[ -n $etl_run ]] || die 'Could not read the ETL run id from the output.'
check_verification "$(bash "$here/verify_mysql.sh" "$etl_run" customer_fund "$default_from" "$default_to" direct)"

if [[ ${DEMO_COMPARE:-0} == 1 ]]; then
    printf '\n== 4b/6 direct vs preagg comparison ==\n'
    direct_log=$(mktemp)
    preagg_log=$(mktemp)
    trap 'rm -f -- "$etl_log" "$ml_log" "$direct_log" "$preagg_log"' EXIT
    bash "$here/run.sh" daily_fund_type "$default_from" "$default_to" direct 2>&1 | tee "$direct_log"
    direct_run=$(sed -n 's/^SUBMIT_RUN_ID=\([^ ]*\).*/\1/p' "$direct_log" | tail -1)
    bash "$here/run.sh" daily_fund_type "$default_from" "$default_to" preagg 2>&1 | tee "$preagg_log"
    preagg_run=$(sed -n 's/^SUBMIT_RUN_ID=\([^ ]*\).*/\1/p' "$preagg_log" | tail -1)
    [[ -n $direct_run && -n $preagg_run ]] || die 'Could not read both comparison run ids.'
    bash "$here/compare_daily.sh" "$direct_run" "$preagg_run"
fi

printf '\n== 5/6 ML job ==\n'
bash "$here/run.sh" fund_purchase_lr 2>&1 | tee "$ml_log"
ml_run=$(sed -n 's/^SUBMIT_RUN_ID=\([^ ]*\).*/\1/p' "$ml_log" | tail -1)
[[ -n $ml_run ]] || die 'Could not read the ML run id from the output.'

printf '\n== 6/6 ML verification ==\n'
check_verification "$(bash "$here/verify_fund_ml.sh" "$ml_run")"

printf '\nDEMO COMPLETE\nETL_RUN_ID=%s\nML_RUN_ID=%s\n' "$etl_run" "$ml_run"
