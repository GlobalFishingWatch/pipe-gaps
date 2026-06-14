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


# --- first_message_at_or_after: positive paths (lock in correct behavior) ---


def test_first_message_at_or_after_returns_exact_match():
    ts = utc_datetime(2024, 1, 1, 12)
    before = create_message(time=ts - timedelta(hours=2), msgid="before")
    on_ts = create_message(time=ts, msgid="on_ts")
    after = create_message(time=ts + timedelta(hours=2), msgid="after")

    msgs = Messages([before, on_ts, after])

    assert msgs.first_message_at_or_after(ts.timestamp()) == on_ts


def test_first_message_at_or_after_returns_next_when_between_messages():
    before = create_message(time=utc_datetime(2024, 1, 1, 10), msgid="before")
    after = create_message(time=utc_datetime(2024, 1, 1, 14), msgid="after")

    msgs = Messages([before, after])

    # A timestamp strictly between the two messages must return the later one.
    requested = utc_datetime(2024, 1, 1, 12).timestamp()
    assert msgs.first_message_at_or_after(requested) == after


def test_first_message_at_or_after_returns_earliest_when_before_all_messages():
    earliest = create_message(time=utc_datetime(2024, 1, 1, 10), msgid="earliest")
    latest = create_message(time=utc_datetime(2024, 1, 1, 20), msgid="latest")

    msgs = Messages([earliest, latest])

    # Requested time precedes every message, so the earliest message is itself
    # "at or after" it -- returning it is correct (timestamp >= requested holds).
    requested = utc_datetime(2024, 1, 1, 0).timestamp()
    result = msgs.first_message_at_or_after(requested)

    assert result == earliest
    assert result["timestamp"] >= requested


# --- first_message_at_or_after: the bug (no message qualifies) ---
#
# When EVERY message predates the requested timestamp, there is no "first
# message at or after" it. The current implementation does:
#
#     idx = binary_search_first_ge(...)        # -> -1 (nothing >= timestamp)
#     return self.sorted[max(idx, 0)]          # -> sorted[0], the EARLIEST msg
#
# i.e. it silently returns the earliest message, whose timestamp is strictly
# BEFORE the requested time -- violating the method's "at or after" contract.
#
# The tests below fail today and should pass once the no-match case is handled.
# They are intentionally agnostic about HOW it is signalled: if the fix returns
# a sentinel other than None (or raises), adjust the expectation to match.


def test_first_message_at_or_after_does_not_return_earlier_message_when_none_qualify():
    earliest = create_message(time=utc_datetime(2024, 1, 1, 10), msgid="earliest")
    latest = create_message(time=utc_datetime(2024, 1, 1, 12), msgid="latest")

    msgs = Messages([earliest, latest])

    # Strictly after every message in the collection.
    requested = utc_datetime(2024, 1, 2, 0).timestamp()
    result = msgs.first_message_at_or_after(requested)

    # Contract: either there is no qualifying message (None), or the one
    # returned is genuinely at or after the requested timestamp. It must never
    # be a message that predates `requested`.
    assert result is None or result["timestamp"] >= requested


def test_first_message_at_or_after_when_none_qualify_does_not_silently_pick_sorted_zero():
    earliest = create_message(time=utc_datetime(2024, 1, 1, 10), msgid="earliest")
    latest = create_message(time=utc_datetime(2024, 1, 1, 12), msgid="latest")

    msgs = Messages([earliest, latest])

    requested = utc_datetime(2024, 1, 5, 0).timestamp()
    result = msgs.first_message_at_or_after(requested)

    # Today the bug surfaces as returning `earliest` (sorted[0]). Pinning this
    # symptom directly makes the regression obvious if max(idx, 0) comes back.
    assert result is not earliest
