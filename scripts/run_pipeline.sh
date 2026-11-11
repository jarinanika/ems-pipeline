#!/usr/bin/env bash
# Run the full pipeline: Bronze -> Silver -> Gold.
#
# Usage (from anywhere):
#   PIPELINE_ENV=dev  bash scripts/run_pipeline.sh    # 2,000-row sample, takes a minute or two
#   PIPELINE_ENV=prod bash scripts/run_pipeline.sh    # full data, takes several minutes
#   FORCE=1 PIPELINE_ENV=prod bash scripts/run_pipeline.sh   # run Silver and Gold even if Bronze found no new data
#
# Each notebook is executed headless. The executed copy, with all printed outputs
# (timings, quality tables, checks), is saved in logs/<run time>/.
# The script stops at the first notebook that fails (for example a failed quality check),
# so later layers never run on bad data.
#
# The source publishes irregularly. Bronze compares a fingerprint of the source file with the
# last ingestion. If the file is unchanged, Bronze logs "skipped_unchanged" and this script
# stops after Bronze, because Silver and Gold would only rebuild identical results.

set -euo pipefail

cd "$(dirname "$0")/.."

export PIPELINE_ENV="${PIPELINE_ENV:-prod}"
FORCE="${FORCE:-0}"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
LOG_DIR="logs/${RUN_ID}"
mkdir -p "${LOG_DIR}"

# Status of the most recent Bronze ingestion log: success, skipped_unchanged, or none
bronze_status() {
python3 - <<'PY'
import glob, json, os, sys
sys.path.insert(0, "src")
from config_loader import load_config

cfg = load_config(f"config/{os.environ['PIPELINE_ENV']}.yaml")
log_dir = os.path.abspath(os.path.join(cfg["bronze"]["root_path"], "_ingestion_log"))
logs = []
for path in glob.glob(os.path.join(log_dir, f"{cfg['source']['dataset_name']}_*.json")):
    with open(path) as f:
        logs.append(json.load(f))
print(max(logs, key=lambda e: e["ingested_at_utc"])["status"] if logs else "none")
PY
}

echo "Pipeline run ${RUN_ID}  (environment: ${PIPELINE_ENV})"
pipeline_start=$(date +%s)

for nb in 01_bronze_ingest 02_silver_transform 03_gold_aggregate; do
  echo "--- ${nb} ---"
  step_start=$(date +%s)
  jupyter nbconvert --to notebook --execute "notebooks/${nb}.ipynb" \
    --output "${nb}_executed.ipynb" --output-dir "${LOG_DIR}" \
    --ExecutePreprocessor.timeout=-1 \
    --ExecutePreprocessor.kernel_name=python3
  echo "${nb} finished in $(( $(date +%s) - step_start )) s"

  if [ "${nb}" = "01_bronze_ingest" ]; then
    status="$(bronze_status)"
    echo "Bronze status: ${status}"
    if [ "${status}" = "skipped_unchanged" ] && [ "${FORCE}" != "1" ]; then
      echo "No new data since the last ingestion. Silver and Gold are skipped (use FORCE=1 to run them anyway)."
      echo "Pipeline run ${RUN_ID} finished in $(( $(date +%s) - pipeline_start )) s (nothing to do)."
      echo "Executed notebooks and outputs: ${LOG_DIR}"
      exit 0
    fi
  fi
done

echo "Pipeline run ${RUN_ID} finished successfully in $(( $(date +%s) - pipeline_start )) s."
echo "Executed notebooks and outputs: ${LOG_DIR}"
