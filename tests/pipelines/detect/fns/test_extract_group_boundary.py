from types import SimpleNamespace
from collections import namedtuple
from datetime import date

from pipe_gaps.pipelines.detect.fns.extract_group_boundary import ExtractGroupBoundary

from tests.conftest import create_message, utc_datetime


SSVID = "446013750"
WINDOW_OFFSET_S = 12 * 3600  # 12 hours, matching the pipeline default (n_hours_before).

_Key = namedtuple("_Key", ["ssvid"])


class _FakeWindow:
    """Minimal stand-in for a Beam ``IntervalWindow``.

    ``ExtractGroupBoundary.process`` only ever reads ``window.start.seconds()``,
    so we expose exactly that. This avoids depending on the Beam ``Timestamp``
    constructor API and keeps the test runnable without a Beam runner, exactly
    like ``test_process_boundaries.py`` calls ``dofn.process(...)`` directly.
    """

    def __init__(self, start_s: float) -> None:
        self.start = SimpleNamespace(seconds=lambda: start_s)


def _extract(messages, window_start_s, offset_s=WINDOW_OFFSET_S, date_range=None):
    dofn = ExtractGroupBoundary(window_offset_s=offset_s, date_range=date_range)
    window = _FakeWindow(window_start_s)
    return list(dofn.process((_Key(SSVID), messages), window=window))


def test_extract_group_boundary_start_picks_first_message_in_window_body():
    """Positive path: ``start`` is the first message at/after window_start + offset.

    A message in the overlap (lookback) region before the offset must be ignored
    in favour of the first message in the window body. Guards against a fix that
    over-corrects and breaks the normal selection.
    """
    window_start = utc_datetime(2024, 1, 1, 0, 0, 0)
    start_time = window_start.timestamp() + WINDOW_OFFSET_S  # noon

    overlap = create_message(time=utc_datetime(2024, 1, 1, 6), ssvid=SSVID, msgid="overlap")
    first_in_body = create_message(time=utc_datetime(2024, 1, 1, 13), ssvid=SSVID, msgid="body_1")
    later = create_message(time=utc_datetime(2024, 1, 1, 20), ssvid=SSVID, msgid="body_2")

    boundaries = _extract([overlap, first_in_body, later], window_start_s=window_start.timestamp())

    assert len(boundaries) == 1
    assert boundaries[0].start == first_in_body
    assert boundaries[0].start["timestamp"] >= start_time


def test_extract_group_boundary_start_is_at_or_after_window_start_plus_offset():
    """The bug, at the seam where it bites.

    When a window contains messages only in its overlap (lookback) region, there
    is no message at or after ``window.start + offset``. ``first_message_at_or_after``
    then hits its ``self.sorted[max(idx, 0)]`` clamp and returns the EARLIEST
    message -- which predates the boundary -- so ``Boundary.start`` is silently
    wrong. That ``start`` is later used as ``right.start`` in
    ``ProcessBoundaries.consecutive_boundaries()`` (``left.end + [right.start]``),
    perturbing cross-window gap detection.

    This test is agnostic about HOW the no-match case is fixed: not emitting a
    boundary, or setting ``start`` to None, both satisfy the contract below.
    Today it fails because the clamp emits ``start`` = the 02:00 message.
    """
    window_start = utc_datetime(2024, 1, 1, 0, 0, 0)
    start_time = window_start.timestamp() + WINDOW_OFFSET_S  # noon

    # Every message lands strictly before start_time (i.e. in the overlap region).
    messages = [
        create_message(time=utc_datetime(2024, 1, 1, 2), ssvid=SSVID, msgid="m02"),
        create_message(time=utc_datetime(2024, 1, 1, 6), ssvid=SSVID, msgid="m06"),
        create_message(time=utc_datetime(2024, 1, 1, 10), ssvid=SSVID, msgid="m10"),
    ]

    boundaries = _extract(messages, window_start_s=window_start.timestamp())

    # Contract: any emitted boundary's start must be genuinely at/after the
    # boundary time -- never a message that predates window_start + offset.
    for b in boundaries:
        assert b.start is None or b.start["timestamp"] >= start_time


# --- date_range path: first_message_in_range (currently untested branch) ---
#
# When date_range is provided, ExtractGroupBoundary also calls
# first_message_at_or_after(range_start) to populate first_message_in_range.
# No existing test exercises this branch at all (_extract defaults date_range
# to None), so the no-match case carries the same clamp bug as `start`.


def test_extract_group_boundary_first_message_in_range_is_at_or_after_range_start():
    """The bug on the date_range branch.

    When every message predates date_range[0], there is no message "at or after"
    the range start, so first_message_at_or_after hits its ``self.sorted[max(idx, 0)]``
    clamp and returns the EARLIEST message -- which predates the range. That value
    is stored as ``Boundary.first_message_in_range`` and later trusted by
    ProcessBoundaries as the ON message when closing an open gap
    (``get_first_message_inside_range`` -> ``_close_open_gap``), so a pre-range
    value silently corrupts gap recovery.

    Contract: ``first_message_in_range`` is either None (no qualifying message)
    or a message at/after the range start. Today it fails: the clamp stores the
    02:00 message, which predates the 2024-01-02 range start.
    """
    range_start = date(2024, 1, 2)
    range_end = date(2024, 1, 3)

    # Window opens the day before the range; every message lands before range start.
    window_start = utc_datetime(2024, 1, 1, 0, 0, 0)
    messages = [
        create_message(time=utc_datetime(2024, 1, 1, 2), ssvid=SSVID, msgid="m02"),
        create_message(time=utc_datetime(2024, 1, 1, 20), ssvid=SSVID, msgid="m20"),
    ]

    boundaries = _extract(
        messages,
        window_start_s=window_start.timestamp(),
        date_range=(range_start, range_end),
    )

    range_start_ts = utc_datetime(2024, 1, 2, 0, 0, 0).timestamp()
    for b in boundaries:
        fmir = b.first_message_in_range
        assert fmir is None or fmir["timestamp"] >= range_start_ts