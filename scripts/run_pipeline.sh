#!/usr/bin/env bash
# Run the full pipeline: Bronze -> Silver -> Gold, for the EMS and collisions datasets.
#
#   PIPELINE_ENV=dev  bash scripts/run_pipeline.sh          # samples, a few minutes
#   PIPELINE_ENV=prod bash scripts/run_pipeline.sh          # full data
#   FORCE=1 PIPELINE_ENV=prod bash scripts/run_pipeline.sh  # run Silver and Gold even if no source changed
#
# Executed notebooks and run.log are saved in logs/<run time>/.
# The script stops at the first failed notebook (for example a failed quality check).
# If both sources are unchanged (Bronze logged "skipped_unchanged"), Silver and Gold are skipped.

set -euo pipefail

cd "$(dirname "$0")/.."

export PIPELINE_ENV="${PIPELINE_ENV:-dev}"
FORCE="${FORCE:-0}"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
LOG_DIR="logs/${RUN_ID}"
mkdir -p "${LOG_DIR}"
exec > >(tee "${LOG_DIR}/run.log") 2>&1

# Status of the latest Bronze ingestion log of a dataset ($1 = ems or collisions)
bronze_status() {
DATASET_KEY="$1" python3 - <<'PY'
import glob, json, os, sys
sys.path.insert(0, "src")
from config_loader import load_config

cfg = load_config(f"config/{os.environ['PIPELINE_ENV']}.yaml")
key = os.environ["DATASET_KEY"]
name = cfg["source"]["dataset_name"] if key == "ems" else cfg["collisions"]["dataset_name"]
log_dir = os.path.abspath(os.path.join(cfg["bronze"]["root_path"], "_ingestion_log"))
logs = []
for path in glob.glob(os.path.join(log_dir, f"{name}_*.json")):
    with open(path) as f:
        logs.append(json.load(f))
print(max(logs, key=lambda e: e["ingested_at_utc"])["status"] if logs else "none")
PY
}

run_notebook() {
  echo "--- $1 ---"
  local step_start
  step_start=$(date +%s)
  jupyter nbconvert --to notebook --execute "notebooks/$1.ipynb" \
    --output "$1_executed.ipynb" --output-dir "${LOG_DIR}" \
    --ExecutePreprocessor.timeout=-1 \
    --ExecutePreprocessor.kernel_name=python3
  echo "$1 finished in $(( $(date +%s) - step_start )) s"
}

echo "Pipeline run ${RUN_ID}  (environment: ${PIPELINE_ENV})"
pipeline_start=$(date +%s)

run_notebook 01_bronze_ingest
run_notebook 04_bronze_collisions

ems_status="$(bronze_status ems)"
collisions_status="$(bronze_status collisions)"
echo "Bronze status: EMS=${ems_status}, collisions=${collisions_status}"

if [ "${ems_status}" = "skipped_unchanged" ] && [ "${collisions_status}" = "skipped_unchanged" ] && [ "${FORCE}" != "1" ]; then
  echo "No new data in either source. Silver and Gold are skipped (use FORCE=1 to run them anyway)."
  echo "Pipeline run ${RUN_ID} finished in $(( $(date +%s) - pipeline_start )) s (nothing to do)."
  echo "Executed notebooks and outputs: ${LOG_DIR}"
  exit 0
fi

run_notebook 02_silver_transform
run_notebook 05_silver_collisions
run_notebook 03_gold_aggregate
run_notebook 06_gold_ems_collisions

echo "Pipeline run ${RUN_ID} finished successfully in $(( $(date +%s) - pipeline_start )) s."
echo "Executed notebooks and outputs: ${LOG_DIR}"