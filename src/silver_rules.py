from functools import reduce
from pyspark.sql import functions as F

TS_FORMAT = "MM/dd/yyyy hh:mm:ss a"

TIMESTAMP_COLS = [
    "INCIDENT_DATETIME", "FIRST_ASSIGNMENT_DATETIME", "FIRST_ACTIVATION_DATETIME",
    "FIRST_ON_SCENE_DATETIME", "FIRST_TO_HOSP_DATETIME", "FIRST_HOSP_ARRIVAL_DATETIME",
    "INCIDENT_CLOSE_DATETIME",
]

INT_COLS = [
    "INITIAL_SEVERITY_LEVEL_CODE", "FINAL_SEVERITY_LEVEL_CODE",
    "DISPATCH_RESPONSE_SECONDS_QY", "INCIDENT_RESPONSE_SECONDS_QY",
    "INCIDENT_TRAVEL_TM_SECONDS_QY", "POLICEPRECINCT", "CITYCOUNCILDISTRICT",
    "COMMUNITYDISTRICT", "COMMUNITYSCHOOLDISTRICT", "CONGRESSIONALDISTRICT",
]


def convert_types(df):
    """Text -> timestamps and integers. ZIP stays text, 5 digits, no '.0'."""
    conversions = {}
    for c in TIMESTAMP_COLS:
        if c in df.columns:
            conversions[c] = F.to_timestamp(F.col(c), TS_FORMAT)
    for c in INT_COLS:
        if c in df.columns:
            conversions[c] = F.col(c).cast("double").cast("int")
    df = df.withColumns(conversions)
    if "ZIPCODE" in df.columns:
        df = df.withColumn(
            "ZIPCODE",
            F.lpad(F.col("ZIPCODE").cast("double").cast("int").cast("string"), 5, "0"),
        )
    return df


def add_flags_and_derived(df):
    """Validity flags (Y -> True, anything else -> False), calendar fields, borough."""
    return (
        df
        .withColumn("is_valid_dispatch_response",
                    F.coalesce(F.col("VALID_DISPATCH_RSPNS_TIME_INDC") == "Y", F.lit(False)))
        .withColumn("is_valid_incident_response",
                    F.coalesce(F.col("VALID_INCIDENT_RSPNS_TIME_INDC") == "Y", F.lit(False)))
        .withColumn("incident_date", F.to_date("INCIDENT_DATETIME"))
        .withColumn("incident_year", F.year("INCIDENT_DATETIME"))
        .withColumn("incident_hour", F.hour("INCIDENT_DATETIME"))
        .withColumn("incident_day_of_week", F.date_format("INCIDENT_DATETIME", "EEEE"))
        .withColumn("borough_std",
                    F.when(F.col("BOROUGH") == "RICHMOND / STATEN ISLAND", "STATEN ISLAND")
                     .otherwise(F.col("BOROUGH")))
    )


def split_rejected(df):
    """Reject rows with no incident ID or no incident time. Returns (accepted, rejected)."""
    has_key = F.col("INCIDENT_ID").isNotNull() & F.col("INCIDENT_DATETIME").isNotNull()
    return df.filter(has_key), df.filter(~has_key)


def dedupe_by_incident_id(df):
    """Keep one record per incident (the grain of Silver)."""
    return df.dropDuplicates(["INCIDENT_ID"])


def _out_of_order(earlier, later):
    return (F.col(earlier).isNotNull() & F.col(later).isNotNull()
            & (F.col(later) < F.col(earlier)))


def _is_negative(col):
    return F.coalesce(F.col(col) < 0, F.lit(False))


def logical_check_expressions():
    """The five logical checks, as a dict of name -> true/false column."""
    return {
        "assignment_before_incident": _out_of_order("INCIDENT_DATETIME", "FIRST_ASSIGNMENT_DATETIME"),
        "on_scene_before_assignment": _out_of_order("FIRST_ASSIGNMENT_DATETIME", "FIRST_ON_SCENE_DATETIME"),
        "hospital_arrival_before_departure": _out_of_order("FIRST_TO_HOSP_DATETIME", "FIRST_HOSP_ARRIVAL_DATETIME"),
        "close_before_incident": _out_of_order("INCIDENT_DATETIME", "INCIDENT_CLOSE_DATETIME"),
        "negative_response_seconds": (_is_negative("DISPATCH_RESPONSE_SECONDS_QY")
                                      | _is_negative("INCIDENT_RESPONSE_SECONDS_QY")
                                      | _is_negative("INCIDENT_TRAVEL_TM_SECONDS_QY")),
    }


def flag_logical_issues(df):
    any_issue = reduce(lambda a, b: a | b, logical_check_expressions().values())
    return df.withColumn("has_data_issue", any_issue)