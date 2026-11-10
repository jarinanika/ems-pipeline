from datetime import date

import gold_rules as gr

SCHEMA = (
    "borough_std string, incident_date date, FINAL_CALL_TYPE string, "
    "FINAL_SEVERITY_LEVEL_CODE int, DISPATCH_RESPONSE_SECONDS_QY int, "
    "INCIDENT_RESPONSE_SECONDS_QY int, INCIDENT_TRAVEL_TM_SECONDS_QY int, "
    "is_valid_dispatch_response boolean, is_valid_incident_response boolean, "
    "has_data_issue boolean"
)
D1 = date(2005, 1, 1)   # a Saturday
D2 = date(2005, 1, 2)   # a Sunday


def make_df(spark, rows):
    """Small Silver-like table. Row = (borough, date, call, severity, dispatch s,
    incident s, travel s, valid dispatch, valid incident, has data issue)."""
    return spark.createDataFrame(rows, schema=SCHEMA)


def test_invalid_records_are_excluded_from_averages(spark):
    df = make_df(spark, [
        ("BRONX", D1, "SICK", 5, 60, 300, 200, True, True, False),
        ("BRONX", D1, "SICK", 5, 120, 600, 400, True, True, False),
        ("BRONX", D1, "SICK", 5, 30, None, None, True, False, False),   # invalid incident response
    ])
    row = gr.build_gold_table(df, ["borough_std"]).first()
    assert row["incident_count"] == 3
    assert row["incident_response_valid_count"] == 2
    assert row["incident_response_avg_seconds"] == 450.0
    assert row["incident_response_excluded_count"] == 1
    assert row["dispatch_valid_count"] == 3           # the dispatch time of that row is still valid
    assert row["dispatch_avg_seconds"] == 70.0
    assert row["dispatch_excluded_count"] == 0


def test_records_with_data_issue_are_counted_but_not_measured(spark):
    df = make_df(spark, [
        ("BRONX", D1, "SICK", 5, 60, 300, 200, True, True, False),
        ("BRONX", D1, "SICK", 5, 999, 9999, 9999, True, True, True),    # flagged logical issue
    ])
    row = gr.build_gold_table(df, ["borough_std"]).first()
    assert row["incident_count"] == 2
    assert row["incident_response_valid_count"] == 1
    assert row["incident_response_avg_seconds"] == 300.0


def test_median_and_p90(spark):
    df = make_df(spark, [
        ("BRONX", D1, "SICK", 5, 10, v, v, True, True, False) for v in [100, 200, 300, 400, 500]
    ])
    row = gr.build_gold_table(df, ["borough_std"]).first()
    assert row["incident_response_median_seconds"] == 300
    assert 400 <= row["incident_response_p90_seconds"] <= 500


def test_each_group_gets_its_own_row(spark):
    df = make_df(spark, [
        ("BRONX", D1, "SICK", 5, 10, 100, 80, True, True, False),
        ("BRONX", D1, "SICK", 5, 10, 200, 90, True, True, False),
        ("QUEENS", D1, "SICK", 5, 10, 300, 95, True, True, False),
    ])
    rows = {r["borough_std"]: r for r in gr.build_gold_table(df, ["borough_std"]).collect()}
    assert rows["BRONX"]["incident_count"] == 2
    assert rows["QUEENS"]["incident_count"] == 1


def test_group_with_no_valid_records_gives_null_not_an_error(spark):
    df = make_df(spark, [("BRONX", D1, "SICK", 5, None, None, None, False, False, False)])
    row = gr.build_gold_table(df, ["borough_std"]).first()
    assert row["incident_response_valid_count"] == 0
    assert row["incident_response_excluded_count"] == 1
    assert row["incident_response_avg_seconds"] is None


def test_month_and_weekday_number(spark):
    df = make_df(spark, [
        ("BRONX", D1, "SICK", 5, 10, 100, 80, True, True, False),
        ("BRONX", D2, "SICK", 5, 10, 100, 80, True, True, False),
    ])
    out = gr.add_day_number(gr.add_month(df)).orderBy("incident_date").collect()
    assert out[0]["incident_month"] == 1
    assert out[0]["incident_day_number"] == 7    # Saturday
    assert out[1]["incident_day_number"] == 1    # Sunday


def test_call_and_severity_columns_are_renamed(spark):
    df = make_df(spark, [("BRONX", D1, "SICK", 5, 10, 100, 80, True, True, False)])
    out = gr.rename_call_and_severity(df)
    assert "final_call_type" in out.columns and "final_severity_level" in out.columns
    assert "FINAL_CALL_TYPE" not in out.columns