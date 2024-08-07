import pytest

from datetime import timedelta, datetime
from pipe_gaps.core import gap_detector as gd


def test_detect_correct_gaps(messages):
    gaps = gd.detect(messages, threshold=timedelta(hours=1, minutes=20))
    assert len(gaps) == 7

    gaps = gd.detect(messages, threshold=timedelta(hours=1, minutes=20), show_progress=True)
    assert len(gaps) == 7


def test_missing_keys():
    messages = [
        {
            "ssvid": "226013750",
            "msgid": "295fa26f-cee9-1d86-8d28-d5ed96c32835",
            "timestamp": datetime(2024, 1, 1, 1).timestamp(),
            "distance_from_shore_m": 1
        },
        {
            "ssvid": "226013750",
            "msgid": "295fa26f-cee9-1d86-8d28-d5ed96c32835",
            "timestamp": datetime(2024, 1, 1, 2).timestamp(),
            "distance_from_shore_m": 1
        }
    ]

    gaps = gd.detect(messages, threshold=0.5)
    assert len(gaps) == 1

    for key in gd.mandatory_keys():
        wrong_messages = [{k: v for k, v in m.items() if k != key} for m in messages]
        with pytest.raises(gd.GapDetectionError):
            gd.detect(wrong_messages, threshold=0.5)
