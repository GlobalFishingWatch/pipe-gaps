import pytest

from datetime import timedelta, datetime
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
        create_message(time=datetime(2024, 1, 1, 2)),
    ]

    gd = GapDetector(threshold=0.5)
    gaps = gd.detect(messages)
    assert len(gaps) == 1

    for key in gd.mandatory_keys():
        wrong_messages = [{k: v for k, v in m.items() if k != key} for m in messages]
        with pytest.raises(GapDetectionError):
            gd.detect(wrong_messages)


def test_messages_n_hours_before():
    messages = [
        create_message(time=datetime(2024, 1, 1, 1)),
        create_message(time=datetime(2024, 1, 1, 2), receiver_type="satellite"),    # -3 hours
        create_message(time=datetime(2024, 1, 1, 3), receiver_type="satellite"),    # -3 hours
        create_message(time=datetime(2024, 1, 1, 4), receiver_type="terrestrial"),  # -3 hours
        create_message(time=datetime(2024, 1, 1, 5)),  # OFF message. Start of gap.
        create_message(time=datetime(2024, 1, 1, 8)),

    ]

    gd = GapDetector(threshold=2, n_hours_before=3)
    gaps = gd.detect(messages)
    assert len(gaps) == 1

    gap = gaps[0]
    assert gap[GapDetector.KEY_HOURS_BEFORE] == 3
    assert gap[GapDetector.KEY_HOURS_BEFORE_SAT] == 2
    assert gap[GapDetector.KEY_HOURS_BEFORE_TER] == 1


def test_normalize_output(messages):
    gd = GapDetector(threshold=timedelta(hours=1, minutes=20), normalize_output=True)
    gaps = gd.detect(messages)
    from pprint import pprint
    pprint(gaps)
    assert len(gaps) == 7
