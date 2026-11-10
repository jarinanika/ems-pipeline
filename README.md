# EMS Incident Dispatch Pipeline (Bronze → Silver → Gold)

A batch data pipeline that turns 30.4 million raw NYC EMS incident dispatch records into trusted, analysis-ready tables for studying emergency response times.

- **Source:** NYC Open Data, *EMS Incident Dispatch Data* (FDNY), dataset ID `76xm-jjuj`. About 6.5 GB CSV, 30,389,767 rows, 31 columns, 2005 to 2026.
- **Analytical questions:** (1) how do response times vary by borough and time period? (2) how do they vary by call type and severity?
- **Tools:** PySpark 3.5 (local mode on the project machine), Parquet, pytest, GitHub Actions.
- **Team:** GROUP-22: Jarin Anika and Mehedi Hasan Sabbir

## Architecture

| Layer | Purpose | Output |
|---|---|---|
| **Bronze** | Keep the raw data exactly as received, with traceability. No cleaning, no type changes. | `data/bronze/ems_incident_dispatch/snapshot_date=YYYY-MM-DD/` (Parquet, all columns as text) and a JSON run log |
| **Silver** | Clean, typed, flagged, deduplicated incident records. One row per `INCIDENT_ID`. | `data/silver/ems_incident_dispatch/data/` (Parquet, partitioned by `incident_year`), rejected records, and a quality log |
| **Gold** | Small tables for the two analytical questions. | Three CSV tables and `_gold_run_log.json` in `data/gold/ems_incident_dispatch/` |

### Gold tables

| Table | Grain |
|---|---|
| `response_by_borough_month.csv` | borough × year × month |
| `response_by_borough_day_hour.csv` | borough × day of week × hour |
| `response_by_call_severity.csv` | final call type × final severity level × year |

Every table has the incident count, and for dispatch, incident response, and travel time: valid-record count, average, median, 90th percentile (seconds), and excluded-record count. Only records with a valid response-time flag and no data issue enter the averages and percentiles.

## Repository structure

```
config/        dev.yaml (2,000-row sample) and prod.yaml (full data): paths and Spark settings
src/           config_loader.py, silver_rules.py, gold_rules.py (tested transformation logic)
notebooks/     01_bronze_ingest, 02_silver_transform, 03_gold_aggregate
tests/         pytest unit tests for the Silver and Gold rules
scripts/       run_pipeline.sh (runs all three notebooks in order)
.github/       workflows/ci.yml (runs the tests on every push)
```

## Setup

1. Python 3.11 and Java (8, 11, or 17).
2. `pip install -r requirements.txt` (PySpark 3.5.0, PyYAML, pandas, pytest).
3. Download the CSV from NYC Open Data (dataset `76xm-jjuj`) and set its path in the config file (`source.input_path`).
4. Check the paths and Spark settings in `config/prod.yaml` for your machine.

## Configuration: development and production

| | `config/dev.yaml` | `config/prod.yaml` |
|---|---|---|
| Data | 2,000-row sample (`data_sample/`) | full 6.5 GB file |
| Output | `output/bronze`, `output/silver`, `output/gold` | `data/bronze`, `data/silver`, `data/gold` |
| Spark | 2 cores, 2 GB heap | 2 cores, 4 GB heap, 400 shuffle partitions |

The environment is chosen with the `PIPELINE_ENV` variable (`dev` by default, so nothing touches the full data by accident).

## Run

**Whole pipeline, one command** (stops at the first failed notebook or failed quality check):

```bash
PIPELINE_ENV=dev  bash scripts/run_pipeline.sh    # quick test on the sample
PIPELINE_ENV=prod bash scripts/run_pipeline.sh    # full data
```

Executed notebooks with all outputs (timings, quality tables, checks) are saved in `logs/<run time>/`.

**Step by step:** open the notebooks in order and run all cells. Set `PIPELINE_ENV=prod` in the environment, or temporarily change the `ENV` line in the first cell.

**Tests:** `python -m pytest tests -v` (18 tests, also run by GitHub Actions on every push).

## Quality and monitoring

| Evidence | Where |
|---|---|
| Bronze run log: run ID, snapshot date, row count, column list | `bronze/_ingestion_log/*.json` |
| Silver quality log: rows in, rejected, duplicates, flagged counts, logical checks | `silver/.../_quality_log/*.json` |
| Gold run log (readiness marker): status, checks passed, row counts, Silver snapshot used | `gold/.../_gold_run_log.json` |
| Rejected records, kept with a reason | `silver/.../rejected/` |

Checks that stop the run: Silver row counts must add up and every `INCIDENT_ID` must be unique; Gold incident totals must equal the Silver row count, and Gold valid counts must equal the Silver profile. Gold is published only after these pass, and the old readiness marker is deleted at the start of each Gold run.

## Results on the full data (snapshot 2026-11-02)

| Metric | Value |
|---|---|
| Rows in / rows out (Silver) | 30,389,767 / 30,389,767 |
| Rejected, duplicate incident IDs | 0, 0 |
| Flagged: invalid dispatch response | 242,125 (0.8%) |
| Flagged: invalid incident response | 1,128,836 (3.7%) |
| Flagged: logical data issue | 15,290 (0.05%), mostly hospital arrival before departure |
| Silver size | 1.93 GB in 22 Parquet files (about 70% smaller than the CSV) |
| Gold rows | 1,424 / 963 / 5,205 |

Performance tuning (same machine and Spark settings): the timed Silver steps went from about 19 minutes to about 6 minutes with identical results, by checking duplicates on one column instead of all 31, computing quality numbers from the written Parquet, and writing one file per year instead of 100.

## Main design decisions

- **Flag, don't delete:** invalid response times stay in Silver with a flag, so no emergency disappears from the counts. Only records with no ID or no incident time are rejected.
- **Headline measures are median and 90th percentile:** response times are strongly right-skewed (for incident response the median is 449 s while the 99th percentile is 3,658 s), so averages alone mislead. Outliers are not capped, because long delays are real performance.
- **Bronze snapshots never overwrite:** each download is a dated snapshot, because the source publishes irregularly.
- **Silver is rebuilt from a Bronze snapshot:** overwrite is safe because a re-run gives the same result.

## Known limitations and next steps

- The source has no regular publishing schedule. Bronze skips a repeated snapshot date, but it does not yet detect unchanged content under a new date. Planned: compare the source's last-updated time or file checksum with the last run before ingesting.
- GitHub Actions runs the tests but cannot reach the project machine, so the full pipeline run is started on that machine.
- `UNKNOWN` borough (219 records) and the source code `UNKNOW` for call type are kept as they are.
- Planned extension: add the NYC Motor Vehicle Collisions dataset (`h9gi-nx95`) as a second source and join it to the borough × month Gold table.
