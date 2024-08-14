import pytest
from datetime import datetime

from pipe_gaps.data import get_sample_messages


@pytest.fixture(scope="module")
def messages():
    return get_sample_messages().copy()


class TestCases:
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
            "threshold": 1,
            "expected_gaps": 0,
            "id": "different_ssvid"
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
            "threshold": 1,
            "expected_gaps": 1,
            "id": "same_ssvid_one_gap"
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
                },
                {
                    "ssvid": "226013750",
                    "timestamp": datetime(2024, 1, 1, 3).timestamp(),
                    "distance_from_shore_m": 1
                }
            ],
            "threshold": 2,
            "expected_gaps": 0,
            "id": "same_ssvid_zero_gaps"
        }
    ]
