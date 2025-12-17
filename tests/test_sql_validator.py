import pytest

from chatbot_service import validate_sql, SCHEMA


def test_reject_multiple_statements():
    ok, reason = validate_sql("select 1; select 2", SCHEMA)
    assert not ok
    assert "Multiple statements" in reason


def test_reject_ddl():
    ok, reason = validate_sql("drop table london_bike_data", SCHEMA)
    assert not ok
    assert "Only read-only" in reason


def test_reject_disallowed_table():
    ok, reason = validate_sql("select * from secret_table limit 5", SCHEMA)
    assert not ok
    assert "not allowed" in reason


def test_require_limit_for_non_agg():
    ok, reason = validate_sql("select * from london_bike_data", SCHEMA)
    assert not ok
    assert "include a LIMIT" in reason


def test_allow_with_limit():
    ok, reason = validate_sql("select * from london_bike_data limit 10", SCHEMA)
    assert ok
    assert reason == ""


def test_allow_aggregate_without_limit():
    sql = "select start_station_name, count(*) from london_bike_data group by 1 order by 2 desc"
    ok, reason = validate_sql(sql, SCHEMA)
    assert ok
    assert reason == ""



