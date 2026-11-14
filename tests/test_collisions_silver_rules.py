from datetime import date

import collisions_silver_rules as cr

COLUMNS = ["COLLISION_ID", "CRASH_DATE", "CRASH_TIME", "BOROUGH", "ZIP_CODE",
           "LATITUDE", "LONGITUDE"] + cr.COUNT_COLS


def make_df(spark, rows):
    """All-string table like Bronze. Missing keys become null."""
    data = [tuple(r.get(c) for c in COLUMNS) for r in rows]
    return spark.createDataFrame(data, schema=", ".join(f"{c} string" for c in COLUMNS))


def test_date_hour_and_calendar_fields(spark):
    df = make_df(spark, [{"COLLISION_ID": "1", "CRASH_DATE": "07/04/2013", "CRASH_TIME": "9:05"}])
    row = cr.convert_types(df).first()
    assert row["collision_date"] == date(2013, 7, 4)
    assert (row["collision_year"], row["collision_month"], row["collision_hour"]) == (2013, 7, 9)


def test_missing_id_and_unparseable_date_are_rejected(spark):
    df = make_df(spark, [
        {"COLLISION_ID": "1", "CRASH_DATE": "07/04/2013"},
        {"COLLISION_ID": None, "CRASH_DATE": "07/04/2013"},
        {"COLLISION_ID": "3", "CRASH_DATE": "not a date"},
    ])
    accepted, rejected = cr.split_rejected(cr.convert_types(df))
    assert accepted.count() == 1
    assert rejected.count() == 2


def test_borough_is_standardized_and_blank_becomes_unknown(spark):
    df = make_df(spark, [
        {"COLLISION_ID": "1", "BOROUGH": " staten island "},
        {"COLLISION_ID": "2", "BOROUGH": None},
        {"COLLISION_ID": "3", "BOROUGH": ""},
        {"COLLISION_ID": "4", "BOROUGH": "BROOKLYN"},
    ])
    rows = cr.convert_types(df).orderBy("COLLISION_ID").collect()
    assert [r["borough_std"] for r in rows] == ["STATEN ISLAND", "UNKNOWN", "UNKNOWN", "BROOKLYN"]


def test_zip_code_loses_decimal_and_keeps_leading_zero(spark):
    df = make_df(spark, [
        {"COLLISION_ID": "1", "ZIP_CODE": "7001.0"},
        {"COLLISION_ID": "2", "ZIP_CODE": "11217"},
        {"COLLISION_ID": "3", "ZIP_CODE": None},
    ])
    rows = cr.convert_types(df).orderBy("COLLISION_ID").collect()
    assert [r["ZIP_CODE"] for r in rows] == ["07001", "11217", None]


def test_counts_become_integers_and_missing_stays_null(spark):
    df = make_df(spark, [
        {"COLLISION_ID": "1", "NUMBER_OF_PERSONS_INJURED": "2"},
        {"COLLISION_ID": "2", "NUMBER_OF_PERSONS_INJURED": None},
    ])
    rows = cr.convert_types(df).orderBy("COLLISION_ID").collect()
    assert [r["NUMBER_OF_PERSONS_INJURED"] for r in rows] == [2, None]


def test_location_flag(spark):
    df = make_df(spark, [
        {"COLLISION_ID": "1", "LATITUDE": "40.7", "LONGITUDE": "-73.9"},
        {"COLLISION_ID": "2", "LATITUDE": "0", "LONGITUDE": "0"},
        {"COLLISION_ID": "3", "LATITUDE": None, "LONGITUDE": None},
    ])
    rows = cr.flag_issues(cr.convert_types(df)).orderBy("COLLISION_ID").collect()
    assert [r["has_valid_location"] for r in rows] == [True, False, False]


def test_negative_count_and_future_date_are_flagged(spark):
    df = make_df(spark, [
        {"COLLISION_ID": "1", "CRASH_DATE": "07/04/2013", "NUMBER_OF_PERSONS_INJURED": "1"},
        {"COLLISION_ID": "2", "CRASH_DATE": "07/04/2013", "NUMBER_OF_PERSONS_INJURED": "-1"},
        {"COLLISION_ID": "3", "CRASH_DATE": "01/01/2999"},
    ])
    rows = cr.flag_issues(cr.convert_types(df)).orderBy("COLLISION_ID").collect()
    assert [r["has_data_issue"] for r in rows] == [False, True, True]


def test_duplicate_collision_ids_keep_one_row(spark):
    df = make_df(spark, [
        {"COLLISION_ID": "1", "CRASH_DATE": "07/04/2013"},
        {"COLLISION_ID": "1", "CRASH_DATE": "07/04/2013"},
        {"COLLISION_ID": "2", "CRASH_DATE": "07/04/2013"},
    ])
    assert cr.dedupe_by_collision_id(df).count() == 2