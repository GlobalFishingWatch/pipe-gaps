import pytest
import json
import apache_beam as beam
from datetime import date, timedelta


from apache_beam.testing.test_pipeline import TestPipeline as _TestPipeline
from apache_beam.testing.util import assert_that

from gfw.common.datetime import datetime_from_timestamp

from pipe_gaps.core import GapDetector
from pipe_gaps.common.io import json_load
from pipe_gaps.pipeline.transforms.detect_gaps import DetectGaps

from tests.conftest import TestCases


POSITIONS_HOURS_BEFORE_KEYS = [
    "positions_hours_before",
    "positions_hours_before_ter",
    "positions_hours_before_sat",
    "positions_hours_before_dyn",
]


@pytest.mark.parametrize(
    "messages, threshold, expected_gaps",
    [
        pytest.param(
            case["messages"],
            case["threshold"],
            case["expected_gaps"],
            id=case["id"]
        )
        for case in TestCases.GAP_BETWEEN_YEARS
    ]
)
def test_detect_gaps_between_years(messages, threshold, expected_gaps):
    # Setup
    gap_detector = GapDetector(threshold=threshold)

    # Run test pipeline
    with _TestPipeline() as p:
        input_pcoll = p | "CreateInput" >> beam.Create(messages)

        output = (
            input_pcoll
            | "DetectGaps" >> DetectGaps(
                gap_detector=gap_detector,
                eval_last=False,
                date_range=None,
                window_offset_h=12,
            )
        )

        # Validate the result (only lengths here, could check contents too)
        def check_output(gaps):
            assert len(gaps) == expected_gaps

        assert_that(output, check_output)


@pytest.mark.parametrize(
    "messages, open_gaps, threshold, date_range, expected_gaps",
    [
        pytest.param(
            case["messages"],
            case["open_gaps"],
            case["threshold"],
            case["date_range"],
            case["expected_gaps"],
            id=case["id"]
        )
        for case in TestCases.GAP_BETWEEN_DAYS
    ],
)
def test_detect_gaps_between_days(messages, open_gaps, threshold, date_range, expected_gaps):
    """Checks that DetectGaps correctly detects gaps between days including use of side inputs."""

    gap_detector = GapDetector(threshold=threshold)

    with _TestPipeline() as p:
        main_input = p | "CreateMessages" >> beam.Create(messages)
        side_input = p | "CreateOpenGaps" >> beam.Create(open_gaps)

        output = (
            main_input
            | "DetectGaps" >> DetectGaps(
                gap_detector=gap_detector,
                eval_last=True,
                window_period_d=1,
                date_range=date_range,
                side_inputs=side_input
            )
        )

        def check_output(gaps):
            assert len(gaps) == len(expected_gaps)

            gaps = sorted(gaps, key=lambda x: x["OFF"]["timestamp"])
            for gap, expected_gap in zip(gaps, expected_gaps):
                for k in POSITIONS_HOURS_BEFORE_KEYS:
                    assert gap[k] == expected_gap[k]

        assert_that(output, check_output)


@pytest.mark.parametrize(
    "messages, open_gaps, expected_gaps, threshold, date_range, window_period_d, eval_last",
    [
        pytest.param(
            case["messages"],
            case["open_gaps"],
            case["expected_gaps"],
            case["threshold"],
            case["date_range"],
            case["window_period_d"],
            case["eval_last"],
            id=case["id"]
        )
        for case in TestCases.GAP_BETWEEN_ARBITRARY_PERIODS
    ],
)
def test_detect_gaps_arbitrary_period(
    messages, open_gaps, expected_gaps, threshold, date_range, window_period_d, eval_last
):
    """Checks that DetectGaps correctly detects gaps with arbitrary window periods."""

    gap_detector = GapDetector(threshold=threshold)

    with _TestPipeline() as p:
        main_input = p | "CreateMessages" >> beam.Create(messages)
        side_input = p | "CreateOpenGaps" >> beam.Create(open_gaps)

        output = (
            main_input
            | "DetectGaps" >> DetectGaps(
                gap_detector=gap_detector,
                eval_last=eval_last,
                window_period_d=window_period_d,
                date_range=date_range,
                side_inputs=side_input
            )
        )

        def check_output(gaps):
            assert len(gaps) == expected_gaps
            gaps = sorted(gaps, key=lambda x: x["OFF"]["timestamp"])

        assert_that(output, check_output)


@pytest.mark.parametrize(
    "messages, threshold, expected_gaps",
    [
        pytest.param(
            case["messages"],
            case["threshold"],
            case["expected_gaps"],
            id=case["id"]
        )
        for case in TestCases.OPEN_GAPS
    ],
)
def test_detect_open_gaps(messages, threshold, expected_gaps):
    """Checks that open gaps (with no closing ON message) are correctly detected."""

    gap_detector = GapDetector(threshold=threshold)

    with _TestPipeline() as p:
        main_input = p | "CreateMessages" >> beam.Create(messages)

        output = (
            main_input
            | "DetectGaps" >> DetectGaps(
                gap_detector=gap_detector,
                eval_last=True  # Required to detect open-ended gaps
            )
        )

        def check_output(gaps):
            assert len(gaps) == expected_gaps
            if expected_gaps > 0:
                gaps = sorted(gaps, key=lambda x: x["OFF"]["timestamp"])
                for gap in gaps:
                    assert gap["ON"]["msgid"] is None

        assert_that(output, check_output)


@pytest.mark.parametrize(
    "messages, open_gaps, expected_gaps, expected_dt, threshold, date_range, window_period_d,"
    "eval_last",
    [
        pytest.param(
            case["messages"],
            case["open_gaps"],
            case["expected_gaps"],
            case["expected_dt"],
            case["threshold"],
            case["date_range"],
            case["window_period_d"],
            case["eval_last"],
            id=case["id"]
        )
        for case in TestCases.CLOSING_GAPS
    ],
)
def test_detect_closing_gaps(
    messages, open_gaps, expected_gaps, expected_dt,
    threshold, date_range, window_period_d, eval_last,
):
    """Checks that open gaps are correctly closed by later messages."""

    gap_detector = GapDetector(threshold=threshold)

    with _TestPipeline() as p:
        main_input = p | "CreateMessages" >> beam.Create(messages)
        side_input = p | "CreateOpenGaps" >> beam.Create(open_gaps)

        result = (
            main_input
            | "DetectGaps" >> DetectGaps(
                gap_detector=gap_detector,
                eval_last=eval_last,
                window_period_d=window_period_d,
                date_range=date_range,
                side_inputs=side_input,
            )
        )

        def check_output(gaps):
            assert len(gaps) == expected_gaps

            gaps = sorted(gaps, key=lambda x: x["OFF"]["timestamp"])
            for g in gaps:
                g_start = datetime_from_timestamp(g["OFF"]["timestamp"])
                expected_end = expected_dt[g_start]
                actual_end = (
                    datetime_from_timestamp(g["ON"]["timestamp"])
                    if g["ON"]["timestamp"] is not None else None
                )
                assert expected_end == actual_end

        assert_that(result, check_output)


@pytest.mark.parametrize(
    "messages, threshold, date_range, expected_gaps",
    [
        pytest.param(
            case["messages"],
            case["threshold"],
            case["date_range"],
            case["expected_gaps"],
            id=case["id"]
        )
        for case in TestCases.POSITIONS_HOURS_BEFORE
    ],
)
def test_detect_positions_hours_before(messages, threshold, date_range, expected_gaps):
    """Checks that the correct number of hours before a gap are computed."""

    gap_detector = GapDetector(threshold=threshold)

    with _TestPipeline() as p:
        main_input = p | "CreateMessages" >> beam.Create(messages)

        result = (
            main_input
            | "DetectGaps" >> DetectGaps(
                gap_detector=gap_detector,
                eval_last=False,  # Matches original logic
                window_period_d=1,
                date_range=date_range,
                side_inputs=None,
            )
        )

        def check_output(gaps):
            assert len(gaps) == len(expected_gaps)

            gaps = sorted(gaps, key=lambda x: x["OFF"]["timestamp"])

            for gap, expected_gap in zip(gaps, expected_gaps):
                for k in POSITIONS_HOURS_BEFORE_KEYS:
                    assert gap[k] == expected_gap[k]

        assert_that(result, check_output)


@pytest.mark.parametrize(
    "messages, open_gaps, threshold, dates, expected_gaps",
    [
        pytest.param(
            case["messages"],
            case["open_gaps"],
            case["threshold"],
            case["dates"],
            case["expected_gaps"],
            id=case["id"]
        )
        for case in TestCases.DAILY_MODE
    ],
)
def test_daily_mode(tmp_path, messages, open_gaps, threshold, dates, expected_gaps):
    gap_detector = GapDetector(threshold=threshold, normalize_output=True)

    open_gaps_bag = {g["gap_id"]: g for g in open_gaps}
    all_gaps = []

    for start_date in dates:
        end_date = date.fromisoformat(start_date) + timedelta(days=1)
        yesterday_date = date.fromisoformat(start_date) - timedelta(days=1)

        current_messages = messages[yesterday_date.isoformat()] + messages[start_date]

        output_file = tmp_path / f"gaps-{start_date}.json"

        with _TestPipeline() as p:
            main_inputs = p | "CreateMessages" >> beam.Create(current_messages)
            side_inputs = p | "CreateOpenGaps" >> beam.Create(list(open_gaps_bag.values()))

            detected_gaps = (
                main_inputs
                | "DetectGaps" >> DetectGaps(
                    gap_detector=gap_detector,
                    date_range=[start_date, end_date.isoformat()],
                    window_period_d=1,
                    eval_last=True,
                    side_inputs=side_inputs,
                )
            )

            # Write the output to a file in JSON Lines format
            _ = (
                detected_gaps
                | "ToJSON" >> beam.Map(json.dumps)
                | "WriteToFile" >> beam.io.WriteToText(
                    str(output_file).replace(".json", ""),
                    file_name_suffix=".json",
                    shard_name_template="",  # single file
                )
            )

        # Load and accumulate results after the pipeline
        day_gaps = json_load(output_file, lines=True)

        for g in day_gaps:
            open_gaps_bag.pop(g["gap_id"], None)
            if not g["is_closed"]:
                open_gaps_bag[g["gap_id"]] = g

        all_gaps.extend(day_gaps)

    all_gaps = sorted(all_gaps, key=lambda g: g["start_timestamp"])
    expected_gaps = sorted(expected_gaps, key=lambda g: g[0])

    assert len(all_gaps) == len(expected_gaps)

    for gap, expected_gap in zip(all_gaps, expected_gaps):
        gap_start_dt = datetime_from_timestamp(gap["start_timestamp"])
        gap_end_ts = gap["end_timestamp"]
        if gap_end_ts is not None:
            gap_end_dt = datetime_from_timestamp(gap_end_ts)
        else:
            gap_end_dt = None

        assert gap_start_dt == expected_gap[0]
        assert gap_end_dt == expected_gap[1]
