import pytest
from datetime import datetime

from pipe_gaps.data import get_sample_messages


@pytest.fixture(scope="module")
def messages():
    return get_sample_messages().copy()


class TestCases:
    # Cases with gap between years.
    GAP_BETWEEN_YEARS = [
        {
            "messages": [
                {
                    "ssvid": "226013750",
                    "timestamp": datetime(2023, 12, 31, 23).timestamp(),
                    "distance_from_shore_m": 1
                },
                {
                    "ssvid": "446013750",
                    "timestamp": datetime(2024, 1, 1, 1).timestamp(),
                    "distance_from_shore_m": 1
                }
            ],
            "expected_gaps": 0,
            "id": "gap_between_years_different_ssvid"
        },
        {
            "messages": [
                {
                    "ssvid": "226013750",
                    "timestamp": datetime(2023, 12, 31, 23).timestamp(),
                    "distance_from_shore_m": 1
                },
                {
                    "ssvid": "226013750",
                    "timestamp": datetime(2024, 1, 1, 1).timestamp(),
                    "distance_from_shore_m": 1
                }
            ],
            "expected_gaps": 1,
            "id": "gap_between_years_same_ssvid"
        }
    ]
