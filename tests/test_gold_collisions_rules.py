import pandas as pd

import gold_collisions_rules as gc


def ems_table():
    return pd.DataFrame({
        "borough_std": ["BRONX", "BRONX", "QUEENS"],
        "incident_year": [2005, 2013, 2013],
        "incident_month": [1, 1, 1],
        "incident_count": [1000, 2000, 500],
    })


def collisions_table():
    return pd.DataFrame({
        "borough_std": ["BRONX", "UNKNOWN"],
        "incident_year": [2013, 2013],
        "incident_month": [1, 1],
        "collision_count": [300, 40],
    })


def test_every_ems_row_is_kept_and_totals_do_not_change():
    out = gc.join_ems_collisions(ems_table(), collisions_table())
    assert len(out) == 3
    assert out["incident_count"].sum() == 3500


def test_months_without_collision_data_are_flagged():
    out = gc.join_ems_collisions(ems_table(), collisions_table())
    assert out["has_collision_data"].tolist() == [False, True, False]


def test_collisions_per_1000_ems_incidents():
    out = gc.join_ems_collisions(ems_table(), collisions_table())
    assert out.loc[1, "collisions_per_1000_ems_incidents"] == 150.0


def test_collisions_without_an_ems_row_are_reported_not_lost():
    ems, col = ems_table(), collisions_table()
    unmatched = gc.unmatched_collisions(ems, col)
    assert unmatched["borough_std"].tolist() == ["UNKNOWN"]
    matched = gc.join_ems_collisions(ems, col)["collision_count"].sum()
    assert matched + unmatched["collision_count"].sum() == col["collision_count"].sum()