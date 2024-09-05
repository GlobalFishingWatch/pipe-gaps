import pytest

from typing import Optional
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


def create_message(ssvid: str, time: datetime, lat: float = 65.4, lon: Optional[float] = None):
    return {
        "ssvid": ssvid,
        "msgid": "295fa26f-cee9-1d86-8d28-d5ed96c32835",
        "timestamp": time.timestamp(),
        "receiver_type": "terrestrial",
        "lat": lat,
        "lon": lon,
        "distance_from_shore_m": 1.0
    }


class TestCases:
    GAP_BETWEEN_YEARS = [
        {
            "messages": [
                create_message(ssvid="226013750", time=datetime(2023, 12, 31, 23)),
                create_message(ssvid="446013750", time=datetime(2024, 1, 1, 1)),
            ],
            "threshold": 1,
            "expected_gaps": 0,
            "id": "different_ssvid_zero_gaps"
        },
        {
            "messages": [
                create_message(ssvid="226013750", time=datetime(2023, 12, 31, 23)),
                create_message(ssvid="226013750", time=datetime(2024, 1, 1, 1)),
            ],
            "threshold": 1,
            "expected_gaps": 1,
            "id": "same_ssvid_one_gap"
        },
        {
            "messages": [
                create_message(ssvid="226013750", time=datetime(2023, 12, 31, 23)),
                create_message(ssvid="226013750", time=datetime(2024, 1, 1, 1)),
                create_message(ssvid="226013750", time=datetime(2024, 1, 1, 3)),
            ],
            "threshold": 2,
            "expected_gaps": 0,
            "id": "same_ssvid_zero_gaps"
        },
        {
            "messages": [
                create_message(ssvid="226013750", time=datetime(2023, 12, 31, 23)),
                create_message(ssvid="226013750", time=datetime(2024, 1, 1, 1)),
                create_message(ssvid="226013750", time=datetime(2024, 1, 1, 3)),
                create_message(ssvid="446013750", time=datetime(2023, 12, 31, 23)),
                create_message(ssvid="446013750", time=datetime(2024, 1, 1, 1)),
            ],
            "threshold": 1,
            "expected_gaps": 3,
            "id": "different_ssvid_three_gaps"
        }
    ]

    OPEN_GAPS = [
        {
            "messages": [
                create_message(ssvid="226013750", time=datetime(2024, 8, 20, 12)),
                create_message(ssvid="446013750", time=datetime(2024, 1, 20, 13)),
            ],
            "threshold": 6,
            "expected_gaps": 2,
            "id": "different_ssvid_two_open_gaps"
        },
        {
            "messages": [
                create_message(ssvid="226013750", time=datetime(2024, 8, 20, 20)),
            ],
            "threshold": 6,
            "expected_gaps": 0,
            "id": "one_ssvid_no_open_gap"
        },
    ]

    CLOSING_GAPS = [
        {
            "messages": [
                create_message(ssvid="226013750", time=datetime(2024, 1, 5, 12)),
                create_message(ssvid="226013750", time=datetime(2024, 1, 5, 13)),
            ],
            "open_gaps": [
                {
                    "ssvid": "210023456",
                    "gap_id": "0eb742651071b9e4f192b643511a3e4f",
                    "gap_start": datetime(2024, 1, 1, 1).timestamp(),
                    "gap_start_msgid": "3b793b64-46e4-80eb-82ae-1262a2b8eeab",
                    "gap_start_distance_from_shore_m": 97000.0,
                    "gap_start_lat": 44.5,
                    "gap_start_lon": 60.1,
                    "gap_start_receiver_type": "terrestrial",
                    "is_closed": False
                },
                {
                    "ssvid": "226013750",
                    "gap_id": "0eb742651071b9e4f192b643511a3e4f",
                    "gap_start": datetime(2024, 1, 1, 1).timestamp(),
                    "gap_start_msgid": "3b793b64-46e4-80eb-82ae-1262a2b8eeab",
                    "gap_start_distance_from_shore_m": 97000.0,
                    "gap_start_lat": 44.5,
                    "gap_start_lon": 60.1,
                    "gap_start_receiver_type": "terrestrial",
                    "is_closed": False
                },
            ],
            "threshold": 6,
            "expected_gaps": 1,
            "id": "two_ssvid_one_closed_gap"
        },
    ]
