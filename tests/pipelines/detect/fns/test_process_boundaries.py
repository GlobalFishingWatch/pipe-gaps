import copy
from datetime import date, timedelta

from pipe_gaps.core import GapDetector
from pipe_gaps.common.key import Key
from pipe_gaps.pipelines.detect.fns.extract_group_boundary import Boundary
from pipe_gaps.pipelines.detect.fns.process_boundaries import (
    Boundaries,
    ProcessBoundaries,
)

from tests.conftest import create_message, utc_datetime


SSVID = "446013750"


def _ssvid_message(time, seg_id, msgid, lat=10.0, lon=20.0):
    return create_message(
        time=time,
        ssvid=SSVID,
        seg_id=seg_id,
        msgid=msgid,
        lat=lat,
        lon=lon,
    )


def test_boundaries_ordering_is_deterministic_when_first_message_timestamps_tie():
    ts = utc_datetime(2024, 1, 1, 12, 0, 0)
    later = ts + timedelta(hours=2)

    boundary_a = Boundary(
        ssvid=SSVID,
        start=_ssvid_message(ts, seg_id="seg_a", msgid="a_start"),
        end=[_ssvid_message(later, seg_id="seg_a", msgid="a_end")],
    )
    boundary_b = Boundary(
        ssvid=SSVID,
        start=_ssvid_message(ts, seg_id="seg_b", msgid="b_start"),
        end=[_ssvid_message(later, seg_id="seg_b", msgid="b_end")],
    )

    forward = Boundaries([boundary_a, boundary_b])
    reverse = Boundaries([boundary_b, boundary_a])

    assert forward.first_boundary() == reverse.first_boundary()
    assert forward.last_boundary() == reverse.last_boundary()
    assert forward.consecutive_boundaries() == reverse.consecutive_boundaries()


def test_get_first_message_inside_range_is_deterministic_on_timestamp_ties():
    range_start_date = date(2024, 1, 2)
    range_end_date = date(2024, 1, 3)
    ts_at_range = utc_datetime(2024, 1, 2, 0, 0, 0)
    earlier = utc_datetime(2024, 1, 1, 23, 0, 0)

    m_a = _ssvid_message(ts_at_range, seg_id="seg_a", msgid="a", lat=1.0)
    m_b = _ssvid_message(ts_at_range, seg_id="seg_b", msgid="b", lat=2.0)

    boundary_a = Boundary(
        ssvid=SSVID,
        start=_ssvid_message(earlier, seg_id="seg_a", msgid="a_pre"),
        end=[_ssvid_message(earlier, seg_id="seg_a", msgid="a_pre")],
        first_message_in_range=m_a,
    )
    boundary_b = Boundary(
        ssvid=SSVID,
        start=_ssvid_message(earlier, seg_id="seg_b", msgid="b_pre"),
        end=[_ssvid_message(earlier, seg_id="seg_b", msgid="b_pre")],
        first_message_in_range=m_b,
    )

    forward = Boundaries([boundary_a, boundary_b])
    reverse = Boundaries([boundary_b, boundary_a])

    pick_forward = forward.get_first_message_inside_range((range_start_date, range_end_date))
    pick_reverse = reverse.get_first_message_inside_range((range_start_date, range_end_date))

    assert pick_forward == pick_reverse


def test_process_boundaries_closes_open_gap_with_same_on_regardless_of_input_order():
    """End-to-end determinism check at the boundary-recovery seam.

    Constructs an open gap (in side inputs) plus two candidate ON messages tied
    on timestamp at the range start. ProcessBoundaries.process must pick the
    same ON message -- and thus emit byte-identical closed gaps -- regardless
    of the order boundaries arrive in.
    """
    range_start_date = date(2024, 1, 2)
    range_end_date = date(2024, 1, 3)
    ts_at_range = utc_datetime(2024, 1, 2, 0, 0, 0)
    off_ts = utc_datetime(2023, 12, 25, 0, 0, 0)
    pre_range = utc_datetime(2024, 1, 1, 22, 0, 0)

    gap_detector = GapDetector(threshold=12, normalize_output=True)
    key = Key(["ssvid"])

    off_m = _ssvid_message(off_ts, seg_id="seg_off", msgid="off")
    open_gap = gap_detector.create_gap(off_m=off_m, previous_positions=[])

    m_a = _ssvid_message(ts_at_range, seg_id="seg_a", msgid="a", lat=1.0)
    m_b = _ssvid_message(ts_at_range, seg_id="seg_b", msgid="b", lat=2.0)

    boundary_a = Boundary(
        ssvid=SSVID,
        start=_ssvid_message(pre_range, seg_id="seg_a", msgid="a_pre"),
        end=[_ssvid_message(pre_range, seg_id="seg_a", msgid="a_pre")],
        first_message_in_range=m_a,
    )
    boundary_b = Boundary(
        ssvid=SSVID,
        start=_ssvid_message(pre_range, seg_id="seg_b", msgid="b_pre"),
        end=[_ssvid_message(pre_range, seg_id="seg_b", msgid="b_pre")],
        first_message_in_range=m_b,
    )

    dofn = ProcessBoundaries(
        gap_detector=gap_detector,
        key=key,
        eval_last=False,
        date_range=(range_start_date, range_end_date),
    )

    def _run(boundaries):
        # GapDetector.create_gap mutates its `base_gap` in place when closing
        # an open gap. The open gap from side inputs and every emitted gap dict
        # are therefore deep-copied here so the two runs cannot share state.
        si = {SSVID: [copy.deepcopy(open_gap)]}
        return [copy.deepcopy(g) for g in dofn.process((SSVID, boundaries), side_inputs=si)]

    assert _run([boundary_a, boundary_b]) == _run([boundary_b, boundary_a])


# --- None propagation from first_message_at_or_after (the fix thread) ---
#
# Once first_message_at_or_after returns None for the no-match case,
# ExtractGroupBoundary can emit a Boundary whose `start` and/or
# `first_message_in_range` is None. These tests feed those None-bearing
# boundaries into Boundaries/ProcessBoundaries to pin exactly where None breaks
# the consumer. No existing test ever constructs a boundary with a None field.


def test_process_boundaries_handles_boundary_with_none_start():
    """A window with no message at/after window_start+offset yields start=None.

    ``Boundaries.__init__`` sorts by ``first_message()`` (== ``start``) using
    ``timestamp_msgid_key``, and ``consecutive_boundaries()`` builds
    ``left.end + [right.start]`` which is handed to the gap detector. A None
    ``start`` therefore flows straight into the sort key and the detector input.

    Contract: ProcessBoundaries must handle a missing in-window start without
    crashing. Today this raises in ``Boundaries.__init__`` (timestamp lookup on
    None) before any gap logic runs.
    """
    ts = utc_datetime(2024, 1, 1, 12, 0, 0)

    none_start = Boundary(
        ssvid=SSVID,
        start=None,
        end=[_ssvid_message(ts, seg_id="seg_a", msgid="a_end")],
    )
    normal = Boundary(
        ssvid=SSVID,
        start=_ssvid_message(ts + timedelta(hours=2), seg_id="seg_b", msgid="b_start"),
        end=[_ssvid_message(ts + timedelta(hours=4), seg_id="seg_b", msgid="b_end")],
    )

    dofn = ProcessBoundaries(
        gap_detector=GapDetector(threshold=12, normalize_output=True),
        key=Key(["ssvid"]),
        eval_last=False,
    )

    # Should not raise: a missing in-window start must be handled, not crash.
    list(dofn.process((SSVID, [none_start, normal])))


def test_get_first_message_inside_range_handles_none_first_message_in_range():
    """A lookback window before the range carries first_message_in_range=None.

    first_message_in_range = first_message_at_or_after(range_start), so it is None
    for any window whose messages all predate range_start -- i.e. the before-range
    lookback windows, which are retained on purpose (FilterWindowsByDateRange only
    trims windows starting after the range) and collected globally per ssvid. Such
    a boundary has a perfectly valid `start` (a pre-range message) but a None
    first_message_in_range. This is a realizable production state, not contrived:
    a valid in-range `start` with None first_message_in_range is impossible, but a
    pre-range `start` with None first_message_in_range is exactly a lookback window.

    ``get_first_message_inside_range`` builds
    ``[b.start] + b.end + [b.first_message_in_range]`` and evaluates
    ``m["timestamp"]`` on every element, so the None raises ``TypeError`` before
    the filter can skip it. Today this path crashes once first_message_at_or_after
    starts returning None; every existing test supplies a real message here.

    Contract: skip the None element and still return the earliest in-range
    message -- here, the in-range boundary's message, never crash.
    """
    range_start_date = date(2024, 1, 2)
    range_end_date = date(2024, 1, 3)
    pre_range = utc_datetime(2024, 1, 1, 22, 0, 0)   # before range_start
    in_range = utc_datetime(2024, 1, 2, 6, 0, 0)     # at/after range_start

    lookback = Boundary(
        ssvid=SSVID,
        start=_ssvid_message(pre_range, seg_id="seg_a", msgid="a_start"),
        end=[_ssvid_message(pre_range, seg_id="seg_a", msgid="a_end")],
        # All messages predate range_start -> first_message_in_range is None.
    )
    in_range_boundary = Boundary(
        ssvid=SSVID,
        start=_ssvid_message(in_range, seg_id="seg_b", msgid="b_start"),
        end=[_ssvid_message(in_range, seg_id="seg_b", msgid="b_end")],
        first_message_in_range=_ssvid_message(in_range, seg_id="seg_b", msgid="b_start"),
    )

    boundaries = Boundaries([lookback, in_range_boundary])

    result = boundaries.get_first_message_inside_range((range_start_date, range_end_date))

    range_start_ts = utc_datetime(2024, 1, 2, 0, 0, 0).timestamp()
    assert result is not None
    assert result["timestamp"] >= range_start_ts
