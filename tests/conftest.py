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


def utc_datetime(*args):
    return datetime(*args, tzinfo=timezone.utc)


def create_message(
    time: datetime,
    ssvid: str = "446013750",
    msgid: str = "295fa26f-cee9-1d86-8d28-d5ed96c32835",
    seg_id: str = "123456",
    lat: float = 65.4,
    lon: Optional[float] = None,
    ais_class: str = "A",
    receiver_type: str = "terrestrial",
    **kwargs
):
    return {
        "ssvid": ssvid,
        "msgid": msgid,
        "seg_id": seg_id,
        "timestamp": time.replace(tzinfo=timezone.utc).timestamp(),
        "receiver_type": receiver_type,
        "lat": lat,
        "lon": lon,
        "ais_class": ais_class,
        **kwargs
    }


def create_open_gap(
    time: datetime = datetime(2024, 1, 1),
    ssvid: str = "446013750",
    previous_positions: list = None
):
    gd = GapDetector(normalize_output=True)

    return gd.create_gap(
        off_m=create_message(ssvid=ssvid, time=time),
        previous_positions=previous_positions
    )


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
                create_message(time=datetime(2020, 12, 20, 20)),  # Closes gap.
                create_message(time=datetime(2020, 12, 21, 2)),
                create_message(time=datetime(2020, 12, 21, 8)),
                create_message(time=datetime(2020, 12, 21, 14)),
                create_message(time=datetime(2020, 12, 21, 20)),
                create_message(time=datetime(2020, 12, 22, 1)),   # Gap 2. Open.
            ],
            "open_gaps": [
                create_open_gap(  # Gap 1. Open.
                    time=datetime(2020, 12, 19, 14),
                )
            ],
            "threshold": 6,
            "date_range": ("2020-12-20", "2020-12-23"),
            "window_period_d": 1,
            "expected_gaps": 2,
            "expected_dt": {
                utc_datetime(2020, 12, 19, 14): utc_datetime(2020, 12, 20, 20),
                utc_datetime(2020, 12, 22, 1): None
            },
            "eval_last": True,
            "id": "one_open_gap_one_closed_gap"
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
        },
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
                create_message(time=datetime(2024, 1, 1, 14)),  # This shouldn´t be detected.
                create_message(time=datetime(2024, 1, 1, 18)),
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
            # In this case we have an open gap created on 2024-01-02T00:00:00.
            # The existing open gap should be closed.
            "messages": [
                create_message(time=datetime(2024, 1, 2, 0)),      # (the open gap)
                create_message(time=datetime(2024, 1, 3, 0)),      # gap 2
                create_message(time=datetime(2024, 1, 3, 10)),     # gap 3
                create_message(time=datetime(2024, 1, 3, 17, 1)),
                create_message(time=datetime(2024, 1, 3, 17, 2)),
                create_message(time=datetime(2024, 1, 3, 17, 3)),  # gap 4 (create open gap)
            ],
            "open_gaps": [
                create_open_gap(  # gap 1 (close open gap)
                    time=datetime(2024, 1, 2, 0),
                    previous_positions=[
                        create_message(time=datetime(2024, 1, 1, 21)),
                        create_message(time=datetime(2024, 1, 1, 22)),
                        create_message(time=datetime(2024, 1, 1, 23))
                    ]
                )
            ],
            "threshold": 6,
            "date_range": ("2024-01-03", "2024-01-04"),
            "expected_gaps": [
                {
                    "positions_hours_before": 3,
                    "positions_hours_before_ter": 3,
                    "positions_hours_before_sat": 0,
                    "positions_hours_before_dyn": 0
                },
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
                    "positions_hours_before": 3,
                    "positions_hours_before_ter": 3,
                    "positions_hours_before_sat": 0,
                    "positions_hours_before_dyn": 0

                },
            ],
            "id": "one_ssvid_with_open_gaps"
        },
    ]

    GAP_BETWEEN_ARBITRARY_PERIODS = [
        # We only want to detect gaps in the date range specified.
        # Sliding windows with arbirary period will be a applied.
        # The pipeline must handle properly the boundaries between windows.
        {
            "messages": [
                create_message(time=datetime(2024, 1, 31, 8)),   # This shouldn´t be detected.
                create_message(time=datetime(2024, 1, 31, 16)),  # gap 1
                create_message(time=datetime(2024, 2, 1, 20)),   # gap 2.
                create_message(time=datetime(2024, 2, 10, 1)),   # gap 3.
                create_message(time=datetime(2024, 2, 28, 23)),  # gap 4 (2024 is leap year).
            ],
            "open_gaps": [],
            "threshold": 10,
            "date_range": ("2024-02-01", "2024-03-01"),  # We want to process february.
            "window_period_d": 1,
            "expected_gaps": 4,
            "eval_last": True,
            "id": "period_1_day"
        },
        {
            "messages": [
                create_message(time=datetime(2024, 2, 1, 4)),
                create_message(time=datetime(2024, 2, 1, 10)),
                create_message(time=datetime(2024, 2, 1, 15)),  # gap 1
                create_message(time=datetime(2024, 2, 2, 5)),
                create_message(time=datetime(2024, 2, 2, 11)),  # gap 2
                create_message(time=datetime(2024, 2, 2, 20)),
                create_message(time=datetime(2024, 2, 3, 2)),
                create_message(time=datetime(2024, 2, 3, 8)),
                create_message(time=datetime(2024, 2, 3, 14)),  # gap 3
            ],
            "open_gaps": [],
            "threshold": 6,
            "date_range": ("2024-02-01", "2024-02-04"),
            "window_period_d": 1,
            "expected_gaps": 3,
            "eval_last": True,
            "id": "period_1_day_for_3_days"
        },
        {
            "messages": [
                create_message(time=datetime(2024, 1, 1, 0)),   # This shouldn´t be detected.
            ],
            "open_gaps": [],
            "threshold": 6,
            "date_range": ("2024-01-02", "2024-01-03"),
            "window_period_d": 1,
            "expected_gaps": 0,
            "eval_last": True,
            "id": "period_1_day_no_duplicated_open_gap"
        },
        {
            "messages": [
                create_message(ssvid="446013750", time=datetime(2020, 12, 20, 11)),
                create_message(ssvid="446013750", time=datetime(2020, 12, 20, 12, 10, 6)),  # Gap
                create_message(ssvid="446013750", time=datetime(2020, 12, 20, 22)),
            ],
            "open_gaps": [],
            "threshold": 6,
            "date_range": ("2020-12-20", "2020-12-21"),
            "window_period_d": 1,
            "expected_gaps": 1,
            "eval_last": True,
            "id": "period_1_day_no_duplicated_closed_gap"
        },
        {
            "messages": [
                create_message(ssvid="446013750", time=datetime(2020, 12, 20, 10)),
                create_message(ssvid="446013750", time=datetime(2020, 12, 20, 14)),
                # This gap shouldn´t be detected (it is from previous day).
                create_message(ssvid="446013750", time=datetime(2020, 12, 20, 20)),  # Gap 1
            ],
            "open_gaps": [],
            "threshold": 6,
            "date_range": ("2020-12-21", "2020-12-22"),
            "window_period_d": 1,
            "expected_gaps": 1,
            "eval_last": True,
            "id": "period_1_day_no_duplicated_closed_gap_2"
        },
        {
            "messages": [
                create_message(time=datetime(2024, 1, 31, 12)),  # This shouldn´t be detected.
                create_message(time=datetime(2024, 1, 31, 23)),  # gap 1
                create_message(time=datetime(2024, 2, 1, 20)),   # gap 2.
                create_message(time=datetime(2024, 2, 10, 1)),   # gap 3.
                create_message(time=datetime(2024, 2, 28, 10)),  # gap 4: open gap
            ],
            "open_gaps": [],
            "threshold": 10,
            "date_range": ("2024-02-01", "2024-03-01"),  # We want to process february.
            "window_period_d": 30,
            "eval_last": True,
            "expected_gaps": 4,
            "id": "period_30_days"
        },
        {
            "messages": [
                create_message(time=datetime(2020, 12, 5, 20, 11)),  # Gap 1.
            ],
            "open_gaps": [],
            "threshold": 6,
            "date_range": ("2020-01-01", "2020-12-20"),
            "window_period_d": 180,
            "eval_last": True,
            "expected_gaps": 1,
            "id": "period_180_days_gap_after_6_pm_in_last_window",
        },
        {
            "messages": [
                create_message(time=datetime(2020, 4, 24, 23, 31)),  # Gap 1.
            ],
            "open_gaps": [],
            "threshold": 6,
            "date_range": ("2020-01-01", "2020-12-20"),
            "window_period_d": 180,
            "eval_last": True,
            "expected_gaps": 1,
            "id": "period_180_days_gap_after_6_pm_in_middle_window",
        },
    ]

    POSITIONS_HOURS_BEFORE = [
        {
            "messages": [
                create_message(time=datetime(2023, 12, 31, 12), receiver_type="dynamic"),
                create_message(time=datetime(2023, 12, 31, 14), receiver_type="terrestrial"),
                create_message(time=datetime(2023, 12, 31, 16), receiver_type="terrestrial"),
                create_message(time=datetime(2023, 12, 31, 18), receiver_type="satellite"),
                # Gap 1
                create_message(time=datetime(2023, 12, 31, 21), receiver_type="satellite"),
                # Gap 2
                create_message(time=datetime(2024, 1, 1, 0), receiver_type="unknown"),
                create_message(time=datetime(2024, 1, 1, 1)),
                # Gap 3
                create_message(time=datetime(2024, 1, 1, 4)),
            ],
            "open_gaps": [],
            "threshold": 2,
            "date_range": None,
            "expected_gaps": [
                {
                    "positions_hours_before": 3,
                    "positions_hours_before_ter": 2,
                    "positions_hours_before_sat": 0,
                    "positions_hours_before_dyn": 1
                },
                {
                    "positions_hours_before": 4,
                    "positions_hours_before_ter": 2,
                    "positions_hours_before_sat": 1,
                    "positions_hours_before_dyn": 1
                },
                {
                    "positions_hours_before": 5,
                    "positions_hours_before_ter": 2,
                    "positions_hours_before_sat": 2,
                    "positions_hours_before_dyn": 0
                }
            ],
            "id": "one_ssvid"
        },
    ]

    DAILY_MODE = [
        {
            # In this case we have a gap (2) that starts after 6 PM,
            # but there are no messages the next day.
            # The gap ends a day after tomorrow.
            "messages": {
                "2023-12-31": [
                    create_message(time=datetime(2023, 12, 31, 20)),  # gap 1.
                ],
                "2024-01-01": [
                    create_message(time=datetime(2024, 1, 1, 20)),
                    create_message(time=datetime(2024, 1, 1, 22)),    # gap 2.
                ],
                "2024-01-02": [],
                "2024-01-03": [
                    create_message(time=datetime(2024, 1, 3, 10)),
                    create_message(time=datetime(2024, 1, 3, 16)),
                    create_message(time=datetime(2024, 1, 3, 22)),
                ]
            },
            "open_gaps": [],
            "threshold": 6,
            "dates": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "expected_gaps": 3,
            "expected_dt": {
                utc_datetime(2023, 12, 31, 20): utc_datetime(2024, 1, 1, 20),
                utc_datetime(2024, 1, 1, 22): None,
                utc_datetime(2024, 1, 1, 22): utc_datetime(2024, 1, 3, 10)
            },
            "id": "gap_after_6_pm_with_end_after_tomorrow"
        },
    ]
