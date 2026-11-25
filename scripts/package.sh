#!/usr/bin/env bash
# Build dist/spark-project-fund-ml.zip with a consistent REVISION.
set -euo pipefail
source "$(dirname -- "${BASH_SOURCE[0]}")/common.sh"

pack_python=${DEMO_PACK_PYTHON:-python3}
command -v "$pack_python" >/dev/null || die 'DEMO_PACK_PYTHON is required to create the archive.'
resolve_code_version
[[ $code_version != *-dirty ]] || die 'Commit or stash code changes before packaging.'

if [[ -d $project_dir/.git ]]; then
    git -C "$project_dir" rev-parse HEAD > "$project_dir/REVISION.tmp"
fi

stage=$(mktemp -d)
trap 'rm -rf -- "$stage"; rm -f -- "$project_dir/REVISION.tmp"' EXIT
root="$stage/spark-project-fund-ml"
mkdir -p "$root"

for item in app.py README.md ML_DEMO.md NOTICE REVISION config scripts sql spark_project tests; do
    [[ -e $project_dir/$item ]] && cp -a "$project_dir/$item" "$root/"
done
find "$root" -name '__pycache__' -type d -prune -exec rm -rf {} +
find "$root" -name '*.py[cod]' -delete

if [[ -r $project_dir/REVISION.tmp ]]; then
    mv "$project_dir/REVISION.tmp" "$root/REVISION"
fi

# Real deployments ship one ZIP; Git never tracks it.
if [[ -r $project_dir/data/FAR-Trans.zip ]]; then
    mkdir -p "$root/data"
    cp -a "$project_dir/data/FAR-Trans.zip" "$root/data/"
fi

mkdir -p "$project_dir/dist"
output="$project_dir/dist/spark-project-fund-ml.zip"
rm -f -- "$output"
(cd "$stage" && "$pack_python" -m zipfile -c "$output" "spark-project-fund-ml")

printf 'CODE_VERSION=%s\nPACKAGE=%s\n' "$code_version" "$output"
