from pyspark.sql import functions as F

# measure name -> (Silver column, validity flag that must be true)
MEASURES = {
    "dispatch":          ("DISPATCH_RESPONSE_SECONDS_QY",  "is_valid_dispatch_response"),
    "incident_response": ("INCIDENT_RESPONSE_SECONDS_QY",  "is_valid_incident_response"),
    "travel":            ("INCIDENT_TRAVEL_TM_SECONDS_QY", "is_valid_incident_response"),
}


def eligible(col, flag):
    """The value of `col` if the record is eligible (valid flag, no data issue), else null.
    Aggregate functions skip nulls, so every statistic covers eligible records only."""
    return F.when(F.col(flag) & ~F.col("has_data_issue"), F.col(col))


def add_month(df):
    """Add the calendar month (1-12) from the incident date."""
    return df.withColumn("incident_month", F.month("incident_date"))


def build_gold_table(df, group_cols):
    """One row per combination of `group_cols`, with counts and response-time statistics."""
    aggs = [F.count("*").alias("incident_count")]
    for name, (col, flag) in MEASURES.items():
        v = eligible(col, flag)
        aggs += [
            F.count(v).alias(f"{name}_valid_count"),
            F.round(F.avg(v), 1).alias(f"{name}_avg_seconds"),
            F.percentile_approx(v, 0.5).alias(f"{name}_median_seconds"),
            F.percentile_approx(v, 0.9).alias(f"{name}_p90_seconds"),
        ]
    out = df.groupBy(*group_cols).agg(*aggs)
    for name in MEASURES:
        out = out.withColumn(
            f"{name}_excluded_count",
            F.col("incident_count") - F.col(f"{name}_valid_count"),
        )
    return out


def add_day_number(df):
    """Add the weekday number (1 = Sunday ... 7 = Saturday) so days sort correctly.
    Sorting by the day name would give Friday, Monday, Saturday..."""
    return df.withColumn("incident_day_number", F.dayofweek("incident_date"))


def rename_call_and_severity(df):
    """Friendlier Gold column names for the call type and severity fields."""
    return (df
            .withColumnRenamed("FINAL_CALL_TYPE", "final_call_type")
            .withColumnRenamed("FINAL_SEVERITY_LEVEL_CODE", "final_severity_level"))