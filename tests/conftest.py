import pytest

from typing import Optional
from datetime import datetime, timezone

from pipe_gaps import utils
from pipe_gaps.core import GapDetector
from pipe_gaps.data import get_sample_messages


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


def create_message(
    time: datetime,
    ssvid: str = "446013750",
    lat: float = 65.4,
    lon: Optional[float] = None,
    ais_class: str = "A",
    receiver_type: str = "terrestrial",
    **kwargs
):
    return {
        "ssvid": ssvid,
        "msgid": "295fa26f-cee9-1d86-8d28-d5ed96c32835",
        "timestamp": time.replace(tzinfo=timezone.utc).timestamp(),
        "receiver_type": receiver_type,
        "lat": lat,
        "lon": lon,
        "ais_class": ais_class,
        **kwargs
    }


def create_open_gap(time: datetime = datetime(2024, 1, 1), ssvid: str = "446013750"):
    gd = GapDetector(normalize_output=True)

    gap = gd.create_gap(off_m=create_message(ssvid=ssvid, time=datetime(2024, 1, 1, 12)))

    gap["gap_start"] = gap.pop("start_timestamp")  # Remove after input schema is fixed.

    return gap


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
                create_message(ssvid="226013750", time=datetime(2023, 1, 1, 1)),
                create_message(ssvid="226013750", time=datetime(2023, 12, 1, 1)),
                create_message(ssvid="226013750", time=datetime(2024, 1, 1, 1)),
            ],
            "threshold": 1,
            "expected_gaps": 2,
            "id": "same_ssvid_two_gaps"
        },
        {
            "messages": [
                create_message(ssvid="226013750", time=datetime(2023, 12, 31, 1)),
                create_message(ssvid="226013750", time=datetime(2024, 1, 1, 1)),
                create_message(ssvid="226013750", time=datetime(2024, 1, 1, 3)),
            ],
            "threshold": 24,
            "expected_gaps": 0,
            "id": "same_ssvid_zero_gaps"
        },
        {
            "messages": [
                create_message(ssvid="226013750", time=datetime(2023, 12, 31, 12)),
                create_message(ssvid="226013750", time=datetime(2024, 1, 1, 1)),
                create_message(ssvid="226013750", time=datetime(2024, 1, 1, 15)),
                create_message(ssvid="446013750", time=datetime(2023, 12, 31, 12)),
                create_message(ssvid="446013750", time=datetime(2024, 1, 1, 1)),
            ],
            "threshold": 12,
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
                create_open_gap(ssvid="210023456", time=datetime(2023, 12, 1)),
                create_open_gap(ssvid="226013750", time=datetime(2023, 12, 1)),
            ],
            "threshold": 6,
            "expected_gaps": 1,
            "id": "two_ssvid_one_closed_gap"
        },
    ]

    NO_GAPS_DUPLICATION = [
        {
            "messages": [
                create_message(ssvid="446013750", time=datetime(2024, 1, 1, 1)),
                create_message(ssvid="446013750", time=datetime(2024, 1, 1, 1)),
                create_message(ssvid="446013750", time=datetime(2024, 1, 1, 5)),
            ],
            "threshold": 1,
            "expected_gaps": 1,
            "id": "input_message_with_same_ssvid_and_timestamp"
        }
    ]

    GAP_BETWEEN_DAYS = [
        # We only want to detect gaps in the second day (the current day).
        # The previous day was processed yesterday, it is only fetched
        # to compare the last message against the first one of current day,
        # and to be able to compute positions before N hours before gap.
        {
            "messages": [
                create_message(time=datetime(2024, 1, 1, 8)),
                create_message(time=datetime(2024, 1, 1, 10)),
                create_message(time=datetime(2024, 1, 1, 13)),
                create_message(time=datetime(2024, 1, 1, 14)),
                create_message(time=datetime(2024, 1, 1, 17)),
                create_message(time=datetime(2024, 1, 1, 20)),
                create_message(time=datetime(2024, 1, 1, 21)),
                create_message(time=datetime(2024, 1, 1, 22)),  # gap 1.
                create_message(time=datetime(2024, 1, 2, 4)),   # gap 2.
                create_message(time=datetime(2024, 1, 2, 8), receiver_type="dynamic"),
                create_message(time=datetime(2024, 1, 2, 10)),  # gap 3.
                create_message(time=datetime(2024, 1, 2, 14)),
                create_message(time=datetime(2024, 1, 2, 17)),
                create_message(time=datetime(2024, 1, 2, 20)),
                create_message(time=datetime(2024, 1, 2, 23)),
            ],
            "open_gaps": [],
            "threshold": 3,
            "date_range": ("2024-01-02", "2024-01-03"),
            "expected_gaps": [
                {
                    "positions_hours_before": 6,
                    "positions_hours_before_ter": 6,
                    "positions_hours_before_sat": 0,
                    "positions_hours_before_dyn": 0
                },
                {
                    "positions_hours_before": 4,
                    "positions_hours_before_ter": 4,
                    "positions_hours_before_sat": 0,
                    "positions_hours_before_dyn": 0
                },
                {
                    "positions_hours_before": 3,
                    "positions_hours_before_ter": 2,
                    "positions_hours_before_sat": 0,
                    "positions_hours_before_dyn": 1
                }
            ],
            "id": "one_ssvid_without_open_gap"
        },
        {
            # In this case we have an open gap created on 2024-01-01T15:00:00.
            # The existing open gap should be closed,
            # taking care of the fact that may have already being closed by the
            # comparison by the last message of previous day.
            "messages": [
                create_message(time=datetime(2024, 1, 1, 12)),
                create_message(time=datetime(2024, 1, 2, 0)),    # gap 1
                create_message(time=datetime(2024, 1, 2, 10)),   # gap 2
                create_message(time=datetime(2024, 1, 2, 20)),
            ],
            "open_gaps": [
                create_open_gap(time=datetime(2024, 1, 1, 12))   # gap 3
            ],
            "threshold": 6,
            "date_range": ("2024-01-02", "2024-01-03"),
            "expected_gaps": [
                {
                    "positions_hours_before": 0,
                    "positions_hours_before_ter": 0,
                    "positions_hours_before_sat": 0,
                    "positions_hours_before_dyn": 0
                },
                {
                    "positions_hours_before": 1,
                    "positions_hours_before_ter": 1,
                    "positions_hours_before_sat": 0,
                    "positions_hours_before_dyn": 0

                },
                {
                    "positions_hours_before": 1,
                    "positions_hours_before_ter": 1,
                    "positions_hours_before_sat": 0,
                    "positions_hours_before_dyn": 0

                },
            ],
            "id": "one_ssvid_with_open_gap"
        },
    ]

    POSITIONS_HOURS_BEFORE = [
        {
            "messages": [
                create_message(time=datetime(2023, 12, 31, 12), receiver_type="terrestrial"),
                create_message(time=datetime(2023, 12, 31, 14), receiver_type="terrestrial"),
                create_message(time=datetime(2023, 12, 31, 16), receiver_type="terrestrial"),
                create_message(time=datetime(2023, 12, 31, 18), receiver_type="satellite"),
                # Gap 1
                create_message(time=datetime(2023, 12, 31, 21), receiver_type="satellite"),
                # Gap 2
                create_message(time=datetime(2024, 1, 1, 0), receiver_type="satellite"),
                create_message(time=datetime(2024, 1, 1, 1), receiver_type="satellite"),
                # Gap 3
                create_message(time=datetime(2024, 1, 1, 4), receiver_type="terrestrial"),
            ],
            "open_gaps": [],
            "threshold": 2,
            "date_range": None,
            "expected_gaps": [
                {
                    "positions_hours_before": 3,
                    "positions_hours_before_ter": 3,
                    "positions_hours_before_sat": 0,
                    "positions_hours_before_dyn": 0
                },
                {
                    "positions_hours_before": 4,
                    "positions_hours_before_ter": 3,
                    "positions_hours_before_sat": 1,
                    "positions_hours_before_dyn": 0
                },
                {
                    "positions_hours_before": 5,
                    "positions_hours_before_ter": 2,
                    "positions_hours_before_sat": 3,
                    "positions_hours_before_dyn": 0
                }
            ],
            "id": "one_ssvid"
        },
    ]
