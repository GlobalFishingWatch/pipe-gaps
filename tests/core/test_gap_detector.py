import pytest

from datetime import timedelta, datetime
from pipe_gaps.core import GapDetector, GapDetectionError


def test_detect_correct_gaps(messages):
    gd = GapDetector(threshold=timedelta(hours=1, minutes=20))
    gaps = gd.detect(messages)
    assert len(gaps) == 7

    gd = GapDetector(threshold=timedelta(hours=1, minutes=20), show_progress=True)
    gaps = gd.detect(messages)
    assert len(gaps) == 7


def test_missing_keys():
    messages = [
        {
            "ssvid": "226013750",
            "msgid": "295fa26f-cee9-1d86-8d28-d5ed96c32835",
            "timestamp": datetime(2024, 1, 1, 1).timestamp(),
            "receiver_type": "terrestrial",
            "lat": None,
            "lon": None,
            "distance_from_shore_m": 1.0

        },
        {
            "ssvid": "226013750",
            "msgid": "295fa26f-cee9-1d86-8d28-d5ed96c32835",
            "timestamp": datetime(2024, 1, 1, 2).timestamp(),
            "receiver_type": "terrestrial",
            "lat": None,
            "lon": None,
            "distance_from_shore_m": 1.0
        }
    ]
    gd = GapDetector(threshold=0.5)
    gaps = gd.detect(messages)
    assert len(gaps) == 1

    for key in gd.mandatory_keys():
        wrong_messages = [{k: v for k, v in m.items() if k != key} for m in messages]
        with pytest.raises(GapDetectionError):
            gd.detect(wrong_messages)


def test_normalize_output(messages):
    gd = GapDetector(threshold=timedelta(hours=1, minutes=20), normalize_output=True)
    gaps = gd.detect(messages)
    from pprint import pprint
    pprint(gaps)
    assert len(gaps) == 7
