from datetime import timedelta
from pipe_gaps.core import gap_detector as gd


def test_gap_detector(messages):
    gaps = gd.detect(messages, threshold=timedelta(hours=1, minutes=20))
    assert len(gaps) == 7

    gaps = gd.detect(messages, threshold=timedelta(hours=1, minutes=20), show_progress=True)
    assert len(gaps) == 7
