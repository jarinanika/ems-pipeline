from pyspark.sql import functions as F

KEYS = ["borough_std", "incident_year", "incident_month"]


def aggregate_collisions_month(col_df):
    """Collisions per borough and month, as a small pandas table."""
    return (
        col_df.groupBy("borough_std", "collision_year", "collision_month")
        .agg(
            F.count("*").alias("collision_count"),
            F.sum("NUMBER_OF_PERSONS_INJURED").alias("persons_injured"),
            F.sum("NUMBER_OF_PERSONS_KILLED").alias("persons_killed"),
        )
        .toPandas()
        .rename(columns={"collision_year": "incident_year", "collision_month": "incident_month"})
    )


def join_ems_collisions(ems, collisions):
    """Left join: every EMS borough-month row is kept, whether or not collision data exists."""
    out = ems.merge(collisions, on=KEYS, how="left")
    out["has_collision_data"] = out["collision_count"].notna()
    out["collisions_per_1000_ems_incidents"] = (
        out["collision_count"] / out["incident_count"] * 1000
    ).round(1)
    return out


def unmatched_collisions(ems, collisions):
    """Collision rows whose borough and month have no EMS row (for example UNKNOWN borough)."""
    merged = collisions.merge(ems[KEYS].drop_duplicates(), on=KEYS, how="left", indicator=True)
    return merged[merged["_merge"] == "left_only"].drop(columns="_merge")