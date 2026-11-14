from functools import reduce
from pyspark.sql import functions as F

BOROUGHS = ["BRONX", "BROOKLYN", "MANHATTAN", "QUEENS", "STATEN ISLAND"]

COUNT_COLS = [
    "NUMBER_OF_PERSONS_INJURED", "NUMBER_OF_PERSONS_KILLED",
    "NUMBER_OF_PEDESTRIANS_INJURED", "NUMBER_OF_PEDESTRIANS_KILLED",
    "NUMBER_OF_CYCLIST_INJURED", "NUMBER_OF_CYCLIST_KILLED",
    "NUMBER_OF_MOTORIST_INJURED", "NUMBER_OF_MOTORIST_KILLED",
]


def convert_types(df):
    """Text -> dates, integers, doubles. Blank or unknown boroughs become UNKNOWN."""
    borough = F.upper(F.trim(F.col("BOROUGH")))
    out = df
    counts = {c: F.col(c).cast("int") for c in COUNT_COLS if c in df.columns}
    if counts:
        out = out.withColumns(counts)
    return (
        out
        .withColumn("collision_date", F.to_date(F.col("CRASH_DATE"), "MM/dd/yyyy"))
        .withColumn("collision_year", F.year("collision_date"))
        .withColumn("collision_month", F.month("collision_date"))
        .withColumn("collision_hour", F.split(F.col("CRASH_TIME"), ":").getItem(0).cast("int"))
        .withColumn("borough_std", F.when(borough.isin(*BOROUGHS), borough).otherwise("UNKNOWN"))
        .withColumn("ZIP_CODE", F.lpad(F.col("ZIP_CODE").cast("double").cast("int").cast("string"), 5, "0"))
        .withColumn("LATITUDE", F.col("LATITUDE").cast("double"))
        .withColumn("LONGITUDE", F.col("LONGITUDE").cast("double"))
    )


def split_rejected(df):
    """Reject rows with no COLLISION_ID or no parseable date. Returns (accepted, rejected)."""
    has_key = F.col("COLLISION_ID").isNotNull() & F.col("collision_date").isNotNull()
    return df.filter(has_key), df.filter(~has_key)


def dedupe_by_collision_id(df):
    """One record per collision (the grain of Silver)."""
    return df.dropDuplicates(["COLLISION_ID"])


def flag_issues(df):
    """Flags, never deletes: has_valid_location (inside the NYC area) and has_data_issue."""
    in_nyc = F.col("LATITUDE").between(40.4, 41.0) & F.col("LONGITUDE").between(-74.3, -73.6)
    negatives = [F.coalesce(F.col(c) < 0, F.lit(False)) for c in COUNT_COLS if c in df.columns]
    future_date = F.coalesce(F.col("collision_date") > F.current_date(), F.lit(False))
    any_negative = reduce(lambda a, b: a | b, [F.lit(False)] + negatives)
    return (
        df
        .withColumn("has_valid_location", F.coalesce(in_nyc, F.lit(False)))
        .withColumn("has_data_issue", any_negative | future_date)
    )