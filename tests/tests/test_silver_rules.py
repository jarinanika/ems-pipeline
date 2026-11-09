from datetime import datetime

import pytest
from pyspark.sql import SparkSession

import silver_rules as sr

COLUMNS = (
    sr.TIMESTAMP_COLS + sr.INT_COLS
    + ["INCIDENT_ID", "ZIPCODE", "BOROUGH",
       "VALID_DISPATCH_RSPNS_TIME_INDC", "VALID_INCIDENT_RSPNS_TIME_INDC"]
)


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("silver_tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def make_df(spark, rows):
    """Build a small all-string DataFrame (like Bronze). Missing keys become null."""
    data = [tuple(r.get(c) for c in COLUMNS) for r in rows]
    schema = ", ".join(f"{c} string" for c in COLUMNS)
    return spark.createDataFrame(data, schema=schema)


# ---------- type conversion ----------

def test_timestamp_am_and_pm(spark):
    df = make_df(spark, [
        {"INCIDENT_ID": "1", "INCIDENT_DATETIME": "01/01/2005 12:00:24 AM"},
        {"INCIDENT_ID": "2", "INCIDENT_DATETIME": "01/01/2005 12:30:00 PM"},
    ])
    rows = sr.convert_types(df).orderBy("INCIDENT_ID").collect()
    assert rows[0]["INCIDENT_DATETIME"] == datetime(2005, 1, 1, 0, 0, 24)
    assert rows[1]["INCIDENT_DATETIME"] == datetime(2005, 1, 1, 12, 30, 0)


def test_unparseable_date_becomes_null(spark):
    df = make_df(spark, [{"INCIDENT_ID": "1", "INCIDENT_DATETIME": "not a date"}])
    assert sr.convert_types(df).first()["INCIDENT_DATETIME"] is None


def test_decimal_text_becomes_integer(spark):
    df = make_df(spark, [{"INCIDENT_ID": "1", "INCIDENT_RESPONSE_SECONDS_QY": "942.0"}])
    assert sr.convert_types(df).first()["INCIDENT_RESPONSE_SECONDS_QY"] == 942


def test_zipcode_loses_decimal_and_keeps_leading_zero(spark):
    df = make_df(spark, [
        {"INCIDENT_ID": "1", "ZIPCODE": "11217.0"},
        {"INCIDENT_ID": "2", "ZIPCODE": "7001.0"},
    ])
    rows = sr.convert_types(df).orderBy("INCIDENT_ID").collect()
    assert rows[0]["ZIPCODE"] == "11217"
    assert rows[1]["ZIPCODE"] == "07001"


# ---------- flags and derived fields ----------

def test_validity_flags_y_n_and_missing(spark):
    df = make_df(spark, [
        {"INCIDENT_ID": "1", "INCIDENT_DATETIME": "01/01/2005 12:00:00 AM",
         "VALID_INCIDENT_RSPNS_TIME_INDC": "Y", "VALID_DISPATCH_RSPNS_TIME_INDC": "N"},
        {"INCIDENT_ID": "2", "INCIDENT_DATETIME": "01/01/2005 12:00:00 AM"},
    ])
    out = sr.add_flags_and_derived(sr.convert_types(df)).orderBy("INCIDENT_ID").collect()
    assert out[0]["is_valid_incident_response"] is True
    assert out[0]["is_valid_dispatch_response"] is False
    assert out[1]["is_valid_incident_response"] is False   # missing flag -> False, never null


def test_derived_calendar_fields_and_borough(spark):
    df = make_df(spark, [{
        "INCIDENT_ID": "1", "INCIDENT_DATETIME": "01/01/2005 03:15:00 PM",
        "BOROUGH": "RICHMOND / STATEN ISLAND",
    }])
    row = sr.add_flags_and_derived(sr.convert_types(df)).first()
    assert row["incident_year"] == 2005
    assert row["incident_hour"] == 15
    assert row["incident_day_of_week"] == "Saturday"
    assert row["borough_std"] == "STATEN ISLAND"
    assert row["BOROUGH"] == "RICHMOND / STATEN ISLAND"     # original is kept


# ---------- reject and deduplicate ----------

def test_rows_without_id_or_time_are_rejected(spark):
    df = make_df(spark, [
        {"INCIDENT_ID": "1", "INCIDENT_DATETIME": "01/01/2005 12:00:00 AM"},
        {"INCIDENT_ID": None, "INCIDENT_DATETIME": "01/01/2005 12:00:00 AM"},
        {"INCIDENT_ID": "3", "INCIDENT_DATETIME": None},
    ])
    accepted, rejected = sr.split_rejected(sr.convert_types(df))
    assert accepted.count() == 1
    assert rejected.count() == 2


def test_duplicate_incident_ids_keep_one_row(spark):
    df = make_df(spark, [
        {"INCIDENT_ID": "1", "INCIDENT_DATETIME": "01/01/2005 12:00:00 AM"},
        {"INCIDENT_ID": "1", "INCIDENT_DATETIME": "01/01/2005 12:00:00 AM"},
        {"INCIDENT_ID": "2", "INCIDENT_DATETIME": "01/01/2005 12:00:00 AM"},
    ])
    assert sr.dedupe_by_incident_id(df).count() == 2


# ---------- logical checks ----------

def test_on_scene_before_assignment_is_flagged(spark):
    df = make_df(spark, [{
        "INCIDENT_ID": "1", "INCIDENT_DATETIME": "01/01/2005 12:00:00 AM",
        "FIRST_ASSIGNMENT_DATETIME": "01/01/2005 12:10:00 AM",
        "FIRST_ON_SCENE_DATETIME": "01/01/2005 12:05:00 AM",
    }])
    assert sr.flag_logical_issues(sr.convert_types(df)).first()["has_data_issue"] is True


def test_missing_hospital_times_are_not_an_error(spark):
    df = make_df(spark, [{
        "INCIDENT_ID": "1", "INCIDENT_DATETIME": "01/01/2005 12:00:00 AM",
        "FIRST_ASSIGNMENT_DATETIME": "01/01/2005 12:02:00 AM",
        "FIRST_ON_SCENE_DATETIME": "01/01/2005 12:10:00 AM",
    }])
    assert sr.flag_logical_issues(sr.convert_types(df)).first()["has_data_issue"] is False


def test_negative_response_seconds_is_flagged(spark):
    df = make_df(spark, [{
        "INCIDENT_ID": "1", "INCIDENT_DATETIME": "01/01/2005 12:00:00 AM",
        "DISPATCH_RESPONSE_SECONDS_QY": "-5",
    }])
    assert sr.flag_logical_issues(sr.convert_types(df)).first()["has_data_issue"] is True