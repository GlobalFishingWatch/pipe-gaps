import pytest
import logging
import json
import apache_beam as beam
from datetime import date, timedelta

from apache_beam.testing.test_pipeline import TestPipeline as _TestPipeline
from apache_beam.testing.util import assert_that

from gfw.common.datetime import datetime_from_timestamp
from gfw.common.io import json_load

from pipe_gaps.core import GapDetector
from pipe_gaps.pipelines.detect.transforms.detect_gaps import DetectGaps

from tests.conftest import TestCases


logger = logging.getLogger(__name__)


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


def _apply_delete(gaps_table: list[dict], start_date: date) -> list[dict]:
    """Simulates the DELETE query that runs before each pipeline execution.

    Closed gaps are deleted by end_timestamp: they will be recreated when the
    pipeline reprocesses the range and finds the ON message again.

    Open gaps are deleted by start_timestamp: they will be recreated when the
    pipeline reprocesses the range and finds the OFF message again.
    """
    def should_delete(gap: dict) -> bool:
        if gap["is_closed"]:
            return datetime_from_timestamp(gap["end_timestamp"]).date() >= start_date
        else:
            return datetime_from_timestamp(gap["start_timestamp"]).date() >= start_date

    return [g for g in gaps_table if not should_delete(g)]


def _get_open_gaps(gaps_table: list[dict]) -> dict[str, dict]:
    """Returns the latest open gaps from the table, excluding those that have
    already been closed. Mirrors the production side input query:
        WHERE is_closed = FALSE
        QUALIFY ROW_NUMBER() OVER (PARTITION BY gap_id ORDER BY version DESC) = 1
    """
    closed_gap_ids = {g["gap_id"] for g in gaps_table if g["is_closed"]}
    return {
        g["gap_id"]: g
        for g in gaps_table
        if not g["is_closed"] and g["gap_id"] not in closed_gap_ids
    }


def get_dates_in_range(start_date, end_date):
    return [
        (start_date + timedelta(days=i)).isoformat()
        for i in range((end_date - start_date).days + 1)
    ]


@pytest.mark.parametrize(
    "messages, open_gaps, threshold, date_ranges, expected_gaps",
    [
        pytest.param(
            case["messages"],
            case["open_gaps"],
            case["threshold"],
            case["date_ranges"],
            case["expected_gaps"],
            id=case["id"]
        )
        for case in TestCases.INCREMENTAL_MODE
    ],
)
def test_incremental_mode(tmp_path, messages, open_gaps, threshold, date_ranges, expected_gaps):
    gap_detector = GapDetector(threshold=threshold, normalize_output=True)

    # Mirrors the production gaps table - deleted from and appended to each run.
    # Initialized with any pre-existing open gaps passed to the test.
    gaps_table = list(open_gaps)

    for start_date_str, end_date_str in date_ranges:
        logger.info(f"PROCESSING DATE RANGE: [{start_date_str}, {end_date_str}]")
        start_date = date.fromisoformat(start_date_str)
        end_date = date.fromisoformat(end_date_str)

        # Simulate delete query and rebuild side inputs from surviving rows.
        gaps_table = _apply_delete(gaps_table, start_date)
        open_gaps = _get_open_gaps(gaps_table)

        logger.info(f"TABLE AFTER DELETE: {gaps_table}")
        logger.info(f"OPEN GAPS AFTER DELETE: {list(open_gaps.values())}")

        # Build input messages
        yesterday_date = start_date - timedelta(days=1)
        dates_in_range = get_dates_in_range(yesterday_date, end_date)
        messages_in_range = [m for d in dates_in_range for m in messages.get(d, [])]

        output_file = tmp_path / f"gaps-{start_date}--{end_date}.json"
        with _TestPipeline() as p:
            main_inputs = p | "CreateMessages" >> beam.Create(messages_in_range)
            side_inputs = p | "CreateOpenGaps" >> beam.Create(list(open_gaps.values()))

            detected_gaps = (
                main_inputs
                | "DetectGaps" >> DetectGaps(
                    gap_detector=gap_detector,
                    date_range=[start_date_str, end_date_str],
                    eval_last=True,
                    side_inputs=side_inputs,
                )
            )
            _ = (
                detected_gaps
                | "ToJSON" >> beam.Map(json.dumps)
                | "WriteToFile" >> beam.io.WriteToText(
                    str(output_file).replace(".json", ""),
                    file_name_suffix=".json",
                    shard_name_template="",
                )
            )

        # Append new gaps to the table, mirroring the production append-only write.
        range_gaps = json_load(output_file, lines=True)
        gaps_table.extend(range_gaps)

        logger.info(f"TABLE AFTER RUN: {gaps_table}")
        logger.info(f"OPEN GAPS AFTER RUN: {list(_get_open_gaps(gaps_table).values())}")

    # Assert against full table contents, sorted by start_timestamp then is_closed
    # so open version (v1) always comes before closed version (v2) for the same gap.
    all_gaps = sorted(gaps_table, key=lambda g: (g["start_timestamp"], g["is_closed"]))
    expected_gaps = sorted(expected_gaps, key=lambda g: (g[0], g[1] is not None))

    assert len(all_gaps) == len(expected_gaps)
    for gap, expected_gap in zip(all_gaps, expected_gaps):
        gap_start_dt = datetime_from_timestamp(gap["start_timestamp"])
        gap_end_ts = gap["end_timestamp"]
        gap_end_dt = datetime_from_timestamp(gap_end_ts) if gap_end_ts is not None else None
        assert gap_start_dt == expected_gap[0]
        assert gap_end_dt == expected_gap[1]
