from datetime import datetime

from gfw.common.query import Query

from pipe_gaps import queries


EXPECTED = """
SELECT gap_id,
       ssvid,
       VERSION,
       positions_hours_before,
       positions_hours_before_ter,
       positions_hours_before_sat,
       positions_hours_before_dyn,
       distance_m,
       duration_h,
       implied_speed_knots,
       CAST(UNIX_MICROS(start_timestamp) AS FLOAT64) / 1000000 AS start_timestamp,
       start_seg_id,
       start_msgid,
       start_lat,
       start_lon,
       start_ais_class,
       start_receiver_type,
       start_distance_from_shore_m,
       start_distance_from_port_m,
       CAST(UNIX_MICROS(end_timestamp) AS FLOAT64) / 1000000 AS end_timestamp,
       end_msgid,
       end_seg_id,
       end_lat,
       end_lon,
       end_ais_class,
       end_receiver_type,
       end_distance_from_shore_m,
       end_distance_from_port_m,
       is_closed
FROM
    (
        SELECT *
        FROM `world-fishing-827.pipe_ais_v3_internal.raw_gaps` QUALIFY ROW_NUMBER() OVER (
        PARTITION BY gap_id, start_timestamp ORDER BY VERSION DESC) = 1
    )
WHERE 1 = 1
  AND DATE(start_timestamp) >= '2024-01-01'
  AND DATE(end_timestamp) < '2024-01-02'
  AND ssvid IN ('1234',
                '5678')
  AND NOT is_closed
"""


def test_gaps_query():
    start_date = datetime(2024, 1, 1).date()
    end_date = datetime(2024, 1, 2).date()

    # Test without ssvids filter.
    query = queries.GapsQuery(start_date=start_date)
    query.render()

    # Test with ssvids filter.
    query = queries.GapsQuery(start_date=start_date, ssvids=["1234"])
    query.render()

    # Test with end_date filter.
    query = queries.GapsQuery(
        start_date=start_date,
        end_date=end_date,
        is_closed=False,
        ssvids=["1234", "5678"],
        use_timestamp=True,
    )
    assert query.output_type == queries.Gap

    rendered = query.render(formatted=True)

    expected_formatted = Query.format(EXPECTED)
    assert rendered.strip() == expected_formatted.strip()
