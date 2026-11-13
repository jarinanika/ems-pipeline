import datetime as dt
import json
import os

import source_check as sc

UTC = dt.timezone.utc


def write_metadata(tmp_path, when):
    path = tmp_path / "meta.json"
    path.write_text(json.dumps({"rowsUpdatedAt": int(when.timestamp())}))
    return str(path)


def test_new_data_only_when_source_changed_after_download():
    downloaded = dt.datetime(2026, 11, 1, tzinfo=UTC)
    assert sc.is_new_data(dt.datetime(2026, 11, 5, tzinfo=UTC), downloaded) is True
    assert sc.is_new_data(dt.datetime(2026, 10, 20, tzinfo=UTC), downloaded) is False


def test_state_round_trip(tmp_path):
    state = str(tmp_path / "state.json")
    assert sc.load_state(state) is None                      # nothing recorded yet
    when = dt.datetime(2026, 11, 1, 12, 0, tzinfo=UTC)
    sc.save_state(when, state)
    assert sc.load_state(state) == when


def test_check_reports_new_data_and_writes_github_output(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(sc, "STATE_FILE", str(tmp_path / "state.json"))
    sc.save_state(dt.datetime(2026, 11, 1, tzinfo=UTC))
    out = tmp_path / "gh_output.txt"
    monkeypatch.setenv("GITHUB_OUTPUT", str(out))
    monkeypatch.setenv("SOURCE_METADATA_FILE", write_metadata(tmp_path, dt.datetime(2026, 11, 8, tzinfo=UTC)))
    assert sc.main([]) == 0
    assert "new_data=true" in out.read_text()
    assert "NEW DATA AVAILABLE" in capsys.readouterr().out


def test_check_reports_up_to_date(tmp_path, monkeypatch):
    monkeypatch.setattr(sc, "STATE_FILE", str(tmp_path / "state.json"))
    sc.save_state(dt.datetime(2026, 11, 1, tzinfo=UTC))
    out = tmp_path / "gh_output.txt"
    monkeypatch.setenv("GITHUB_OUTPUT", str(out))
    monkeypatch.setenv("SOURCE_METADATA_FILE", write_metadata(tmp_path, dt.datetime(2026, 10, 1, tzinfo=UTC)))
    sc.main([])
    assert "new_data=false" in out.read_text()


def test_missing_state_does_not_raise_an_alarm(tmp_path, monkeypatch):
    monkeypatch.setattr(sc, "STATE_FILE", str(tmp_path / "no_state_here.json"))
    out = tmp_path / "gh_output.txt"
    monkeypatch.setenv("GITHUB_OUTPUT", str(out))
    monkeypatch.setenv("SOURCE_METADATA_FILE", write_metadata(tmp_path, dt.datetime(2026, 11, 8, tzinfo=UTC)))
    sc.main([])
    assert "new_data=false" in out.read_text()


def test_record_uses_the_file_modification_time(tmp_path, monkeypatch):
    monkeypatch.setattr(sc, "STATE_FILE", str(tmp_path / "state.json"))
    csv = tmp_path / "data.csv"
    csv.write_text("a\n1\n")
    stamp = dt.datetime(2026, 10, 30, 8, 0, tzinfo=UTC).timestamp()
    os.utime(csv, (stamp, stamp))
    assert sc.main(["--record", "--file", str(csv)]) == 0
    assert sc.load_state() == dt.datetime(2026, 10, 30, 8, 0, tzinfo=UTC)


def test_record_with_an_exact_time(tmp_path, monkeypatch):
    monkeypatch.setattr(sc, "STATE_FILE", str(tmp_path / "state.json"))
    sc.main(["--record", "--at", "2026-11-01T12:00:00+00:00"])
    assert sc.load_state() == dt.datetime(2026, 11, 1, 12, 0, tzinfo=UTC)
