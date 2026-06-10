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
