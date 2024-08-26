import pytest
from datetime import datetime

from pipe_gaps.data import get_sample_messages
from pipe_gaps import utils

utils.setup_logger(
    warning_level=[
        "apache_beam",
    ]
)


@pytest.fixture(scope="module")
def messages():
    return get_sample_messages().copy()


@pytest.fixture()
def input_file(tmp_path, messages):
    path = tmp_path.joinpath("test.json")
    utils.json_save(messages, path)

    return path


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
            "id": "different_ssvid_zero_gaps"
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
                },
                {
                    "ssvid": "446013750",
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
            "expected_gaps": 3,
            "id": "different_ssvid_three_gaps"
        }
    ]

    OPEN_GAPS = [
        {
            "messages": [
                {
                    "ssvid": "226013750",
                    "timestamp": datetime(2024, 8, 20, 12).timestamp(),
                    "distance_from_shore_m": 1
                },
                {
                    "ssvid": "446013750",
                    "timestamp": datetime(2024, 1, 20, 13).timestamp(),
                    "distance_from_shore_m": 1
                }
            ],
            "threshold": 6,
            "expected_gaps": 2,
            "id": "different_ssvid_two_open_gaps"
        },
        {
            "messages": [
                {
                    "ssvid": "226013750",
                    "timestamp": datetime(2024, 8, 20, 20).timestamp(),
                    "distance_from_shore_m": 1
                }
            ],
            "threshold": 6,
            "expected_gaps": 0,
            "id": "one_ssvid_no_open_gap"
        },
    ]

    CLOSING_GAPS = [
        {
            "messages": [
                {
                    "ssvid": "226013750",
                    "timestamp": datetime(2024, 1, 5, 12).timestamp(),
                    "distance_from_shore_m": 1
                },
                {
                    "ssvid": "226013750",
                    "timestamp": datetime(2024, 1, 5, 13).timestamp(),
                    "distance_from_shore_m": 1
                }
            ],
            "open_gaps": [
                {
                    "OFF": {
                        "ssvid": "226013750",
                        "timestamp": datetime(2024, 1, 1, 1).timestamp(),
                        "distance_from_shore_m": 1
                    },
                    "ON": None
                }
            ],
            "threshold": 6,
            "expected_gaps": 1,
            "id": "one_ssvid_one_closed_gap"
        },
    ]
