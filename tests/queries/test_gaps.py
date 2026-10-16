import dataclasses

from datetime import date

import pytest

from pipe_gaps import queries


DUMMY_TABLE = "project.dataset.table"


def _sql(query):
    return " ".join(query.render().split())


@pytest.mark.parametrize("query_class", [queries.GapsQuery, queries.OpenGapsQuery])
def test_constructor_takes_only_template_variables(query_class):
    fields = [field.name for field in dataclasses.fields(query_class)]

    assert fields == ["source_gaps", "start_date", "end_date", "ssvids", "min_gap_length"]
    assert query_class(source_gaps=DUMMY_TABLE).output_type == queries.Gap


def test_gaps_query_reads_every_gap_with_timestamps():
    sql = _sql(queries.GapsQuery(source_gaps=DUMMY_TABLE))

    assert f"SELECT * FROM `{DUMMY_TABLE}` WHERE 1=1" in sql
    assert "is_closed = FALSE" not in sql
    assert "UNIX_MICROS" not in sql


def test_gaps_query_filters_start_date_before_the_dedup_and_the_rest_after():
    """start_date is on the dedup key, so filtering it first can't change the last version, and
    it prunes partitions. The other filters could drop a gap's last version and let an older one
    take its place, so they go after.
    """
    sql = _sql(
        queries.GapsQuery(
            source_gaps=DUMMY_TABLE,
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 2),
            ssvids=["1234", "5678"],
            min_gap_length=6,
        )
    )
    dedup = sql.index("QUALIFY ROW_NUMBER()")

    assert sql.index("AND DATE(start_timestamp) >= '2024-01-01'") < dedup
    assert sql.index("AND DATE(end_timestamp) < '2024-01-02'") > dedup
    assert sql.index("AND ssvid IN ('1234', '5678')") > dedup
    assert sql.index("AND (NOT is_closed OR duration_h > 6)") > dedup


def test_open_gaps_query_reads_open_rows_and_only_the_ids_of_closed_ones():
    """is_closed = FALSE / TRUE (not NOT is_closed) lets the table's clustering prune the rest."""
    sql = _sql(queries.OpenGapsQuery(source_gaps=DUMMY_TABLE, start_date=date(2012, 1, 1)))

    assert (
        f"SELECT * FROM `{DUMMY_TABLE}` WHERE is_closed = FALSE "
        "AND DATE(start_timestamp) >= '2012-01-01'"
    ) in sql
    assert (
        f"SELECT DISTINCT gap_id, start_timestamp FROM `{DUMMY_TABLE}` WHERE is_closed = TRUE "
        "AND DATE(start_timestamp) >= '2012-01-01'"
    ) in sql
    assert (
        "LEFT JOIN closed_ids USING (gap_id, start_timestamp) WHERE closed_ids.gap_id IS NULL"
    ) in sql
    assert "NOT is_closed" not in sql


def test_open_gaps_query_returns_timestamps_as_seconds():
    sql = _sql(queries.OpenGapsQuery(source_gaps=DUMMY_TABLE))

    assert "CAST(UNIX_MICROS(start_timestamp) AS FLOAT64) / 1000000 AS start_timestamp" in sql
    assert "CAST(UNIX_MICROS(end_timestamp) AS FLOAT64) / 1000000 AS end_timestamp" in sql
