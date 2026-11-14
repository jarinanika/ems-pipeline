"""Check whether NYC Open Data has published new rows since our last download.

The source (dataset 76xm-jjuj) publishes irregularly. NYC Open Data runs on Socrata, whose
metadata endpoint reports `rowsUpdatedAt`, the time the data last changed. We compare it with
the time our CSV was downloaded, which is stored in source_state.json at the repository root.

Usage:
    python src/source_check.py                                  # check (used by the weekly workflow)
    python src/source_check.py --record --file data_sample/EMS_Incident_Dispatch_Data.csv
    python src/source_check.py --record --at 2026-11-01T12:00:00+00:00
    python src/source_check.py --record                         # "downloaded just now"
"""
import argparse
import datetime as dt
import json
import os
import urllib.request

DATASETS = {"ems": "76xm-jjuj", "collisions": "h9gi-nx95"}
DATASET_ID = DATASETS["ems"]          # the dataset checked by default; --dataset changes it
METADATA_URL = f"https://data.cityofnewyork.us/api/views/{DATASET_ID}"
_ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
STATE_FILE = os.path.join(_ROOT, "source_state.json")


def fetch_rows_updated_at():
    """When the source last changed its data, as a UTC datetime (Socrata field `rowsUpdatedAt`)."""
    override = os.environ.get("SOURCE_METADATA_FILE")   # lets tests run without internet
    if override:
        with open(override) as f:
            meta = json.load(f)
    else:
        request = urllib.request.Request(
            METADATA_URL, headers={"User-Agent": "ems-pipeline-source-check"}
        )
        with urllib.request.urlopen(request, timeout=30) as response:
            meta = json.load(response)
    return dt.datetime.fromtimestamp(int(meta["rowsUpdatedAt"]), tz=dt.timezone.utc)


def is_new_data(rows_updated_at, downloaded_at):
    """True if the source changed its data after our file was downloaded."""
    return rows_updated_at > downloaded_at


def load_state(path=None):
    """The recorded download time, or None if nothing has been recorded yet."""
    path = path or STATE_FILE          # looked up when called, so tests can point it elsewhere
    if not os.path.exists(path):
        return None
    with open(path) as f:
        state = json.load(f)
    return dt.datetime.fromisoformat(state["data_downloaded_at_utc"])


def save_state(downloaded_at, path=None):
    path = path or STATE_FILE
    with open(path, "w") as f:
        json.dump(
            {"dataset_id": DATASET_ID, "data_downloaded_at_utc": downloaded_at.isoformat()},
            f, indent=2,
        )
        f.write("\n")


def write_github_output(new_data, rows_updated_at):
    """Pass the result to the next step of a GitHub Actions workflow (no-op elsewhere)."""
    target = os.environ.get("GITHUB_OUTPUT")
    if target:
        with open(target, "a") as f:
            f.write(f"new_data={'true' if new_data else 'false'}\n")
            f.write(f"source_updated_at={rows_updated_at.isoformat()}\n")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dataset", choices=sorted(DATASETS), default="ems",
                           help="which source to check (default: ems)")
    parser.add_argument("--record", action="store_true",
                        help="record when our current CSV was downloaded")
    parser.add_argument("--file", help="with --record: use this file's modification time")
    parser.add_argument("--at", help="with --record: an exact ISO time, e.g. 2026-11-01T12:00:00+00:00")
    args = parser.parse_args(argv)
    global DATASET_ID, METADATA_URL, STATE_FILE
    if args.dataset != "ems":
        DATASET_ID = DATASETS[args.dataset]
        METADATA_URL = f"https://data.cityofnewyork.us/api/views/{DATASET_ID}"
        STATE_FILE = os.path.join(_ROOT, f"source_state_{args.dataset}.json")

    if args.record:
        if args.at:
            when = dt.datetime.fromisoformat(args.at)
            if when.tzinfo is None:
                when = when.replace(tzinfo=dt.timezone.utc)
        elif args.file:
            when = dt.datetime.fromtimestamp(os.path.getmtime(args.file), tz=dt.timezone.utc)
        else:
            when = dt.datetime.now(dt.timezone.utc)
        save_state(when)
        print("Recorded download time:", when.isoformat())
        return 0

    rows_updated_at = fetch_rows_updated_at()
    downloaded_at = load_state()
    print("Source data last updated:", rows_updated_at.isoformat())

    if downloaded_at is None:
        print("No download time recorded yet. Run: python src/source_check.py --record --file <your csv>")
        new_data = False
    else:
        print("Our file was downloaded: ", downloaded_at.isoformat())
        new_data = is_new_data(rows_updated_at, downloaded_at)
        print("NEW DATA AVAILABLE." if new_data else "Up to date: no new data since our download.")

    write_github_output(new_data, rows_updated_at)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
