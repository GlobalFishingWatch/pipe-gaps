from datetime import datetime

from gfw.common.query import Query

from pipe_gaps import queries

DUMMY_TABLE = "project.dataset.table"

EXPECTED = """
SELECT
  ssvid,
  msgid,
  seg_id,
  CAST(UNIX_MICROS(timestamp) AS FLOAT64) / 1000000 AS timestamp,
  lat,
  lon,
  receiver_type,
  distance_from_shore_m,
  distance_from_port_m,
  (
  CASE
    WHEN type IN ('AIS.1', 'AIS.2', 'AIS.3') THEN 'A'
    WHEN type IN ('AIS.18','AIS.19') THEN 'B'
    ELSE NULL
  END
  ) as ais_class
FROM
  `{messages_table}`
WHERE DATE(timestamp) >= '2024-01-01'
  AND DATE(timestamp) < '2024-01-02'
  AND ssvid IN ('1234')
  AND seg_id IN (SELECT seg_id
    FROM `{segments_table}`
    WHERE 1 = 1
    AND good_seg2
    AND NOT overlapping_and_short )
""".format(messages_table=DUMMY_TABLE, segments_table=DUMMY_TABLE)


def test_messages_query():
    start_date = datetime(2024, 1, 1).date()
    end_date = datetime(2024, 1, 2).date()

    # Test without ssvids filter.
    query = queries.MessagesQuery(
      source_messages=DUMMY_TABLE,
      source_segments=DUMMY_TABLE,
      start_date=start_date, end_date=end_date)
    query.render()

    # Test with ssvids filter.
    query = queries.MessagesQuery(
      source_messages=DUMMY_TABLE,
      source_segments=DUMMY_TABLE,
      start_date=start_date,
      end_date=end_date,
      ssvids=["1234"],
      filter_not_overlapping_and_short=True,
      filter_good_seg=True,
    )
    rendered = query.render(formatted=True)
    expected_formatted = Query.format(EXPECTED)

    assert rendered.strip() == expected_formatted.strip()

    assert query.output_type == queries.Message
