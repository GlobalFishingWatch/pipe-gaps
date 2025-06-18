from datetime import datetime, timezone, timedelta

import pytest

from pipe_gaps.core import GapDetector, GapDetectionError
from tests.conftest import create_message


def test_detect_correct_gaps(messages):
    gd = GapDetector(threshold=timedelta(hours=1, minutes=20))
    gaps = gd.detect(messages)
    assert len(gaps) == 7

    gd = GapDetector(threshold=timedelta(hours=1, minutes=20), show_progress=True)
    gaps = gd.detect(messages)
    assert len(gaps) == 7


def test_missing_keys():
    messages = [
        create_message(time=datetime(2024, 1, 1, 1)),
        create_message(time=datetime(2024, 1, 1, 3)),
        create_message(time=datetime(2024, 1, 1, 5)),
        create_message(time=datetime(2024, 1, 1, 9)),
    ]

    gd = GapDetector(threshold=2, n_hours_before=12)
    gaps = gd.detect(messages)
    assert len(gaps) == 1
    assert gaps[0]["positions_hours_before"] == 3

    for key in gd.mandatory_keys():
        wrong_messages = [{k: v for k, v in m.items() if k != key} for m in messages]
        with pytest.raises(GapDetectionError):
            gd.detect(wrong_messages)


def test_messages_n_hours_before():
    messages = [
        create_message(time=datetime(2023, 12, 31, 18), receiver_type="terrestrial"),
        create_message(time=datetime(2023, 12, 31, 19), receiver_type="terrestrial"),
        create_message(time=datetime(2023, 12, 31, 20), receiver_type="terrestrial"),
        create_message(time=datetime(2023, 12, 31, 23), receiver_type="satellite"),
        create_message(time=datetime(2024, 1, 1, 0), receiver_type="satellite"),    # Gap 1
        create_message(time=datetime(2024, 1, 1, 3), receiver_type="terrestrial"),
    ]

    gd = GapDetector(threshold=2, n_hours_before=6)
    gaps = gd.detect(messages, start_time=datetime(2024, 1, 1, tzinfo=timezone.utc))
    assert len(gaps) == 1

    gap = gaps[0]
    assert gap[GapDetector.KEY_HOURS_BEFORE] == 5
    assert gap[GapDetector.KEY_HOURS_BEFORE_SAT] == 2
    assert gap[GapDetector.KEY_HOURS_BEFORE_TER] == 3


def test_normalize_output(messages):
    gd = GapDetector(threshold=timedelta(hours=1, minutes=20), normalize_output=True)
    gaps = gd.detect(messages)
    # from pprint import pprint
    # pprint(gaps)
    assert len(gaps) == 7


def test_create_gap_on_off_same_timestamp():
    off_m = create_message(time=datetime(2023, 12, 31, 19), lon=40)
    on_m = create_message(time=datetime(2023, 12, 31, 19), lon=90)

    gd = GapDetector(threshold=2, n_hours_before=6)
    gd.create_gap(off_m, on_m)
