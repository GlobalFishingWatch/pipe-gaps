from datetime import timedelta

from pipe_gaps.pipelines.detect.messages import Messages

from tests.conftest import create_message, utc_datetime


def test_first_message_at_or_after_is_order_independent_on_timestamp_ties():
    ts = utc_datetime(2024, 1, 1, 12, 0, 0)
    earlier = create_message(time=ts - timedelta(hours=1), seg_id="seg_a", msgid="earlier")
    m_a = create_message(time=ts, seg_id="seg_a", msgid="a", lat=10.0, lon=20.0)
    m_b = create_message(time=ts, seg_id="seg_b", msgid="b", lat=30.0, lon=40.0)

    forward = Messages([earlier, m_a, m_b])
    reverse = Messages([earlier, m_b, m_a])

    pick_forward = forward.first_message_at_or_after(ts.timestamp())
    pick_reverse = reverse.first_message_at_or_after(ts.timestamp())

    assert pick_forward == pick_reverse
