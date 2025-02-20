import pytest
from datetime import datetime

from pipe_gaps.pipeline import NaivePipeline, PipelineError, NoInputsFound
from pipe_gaps.utils import json_save, json_load, datetime_from_ts

from tests.conftest import TestCases


def get_core_config():
    return dict(kind="detect_gaps", threshold=0.5, eval_last=False)


def get_outputs_config():
    return dict(kind="json", output_prefix="gaps")


def get_input_file_config(input_file, schema):
    return dict(kind="json", input_file=input_file, schema=schema)


def test_no_messages(tmp_path):
    input_file = tmp_path.joinpath("test.json")
    json_save([], input_file)

    inputs = [get_input_file_config(input_file, schema="messages")]

    pipe = NaivePipeline.build(inputs=inputs, core=get_core_config())
    with pytest.raises(NoInputsFound):
        pipe.run()


def test_with_input_file(input_file):
    core_config = get_core_config()
    inputs = [get_input_file_config(input_file, schema="messages")]

    pipe = NaivePipeline.build(inputs=inputs, core=core_config)
    pipe.run()


def test_no_output_path(input_file):
    inputs = [get_input_file_config(input_file, schema="messages")]
    core_config = get_core_config()

    pipe = NaivePipeline.build(inputs=inputs, core=core_config)

    with pytest.raises(PipelineError):
        pipe.output_path.is_file()


def test_no_output_stats_path(input_file):
    inputs = [get_input_file_config(input_file, schema="messages")]
    core_config = get_core_config()

    pipe = NaivePipeline.build(inputs=inputs, core=core_config)

    with pytest.raises(PipelineError):
        pipe.output_path_stats.is_file()


def test_distance_from_shore_is_null(tmp_path, messages):
    input_file = tmp_path.joinpath("test.json")
    for m in messages:
        m["distance_from_shore_m"] = None

    json_save(messages, input_file)
    core_config = get_core_config()
    inputs = [get_input_file_config(input_file, schema="messages")]

    pipe = NaivePipeline.build(inputs=inputs, core=core_config)
    pipe.run()


def test_with_input_params():
    input_query = dict(
        kind="query",
        mock_db_client=True,
        query_name="messages",
        query_params=dict(
            start_date=datetime(2024, 1, 1).date().isoformat(),
            end_date=datetime(2024, 1, 1).date().isoformat()
        )
    )
    inputs = [input_query]
    core_config = get_core_config()

    pipe = NaivePipeline.build(inputs=inputs, core=core_config)
    pipe.run()


def test_save_json(tmp_path, input_file):
    inputs = [get_input_file_config(input_file, schema="messages")]
    core_config = get_core_config()
    outputs_config = [get_outputs_config()]

    pipe = NaivePipeline.build(
        inputs=inputs,
        core=core_config,
        outputs=outputs_config,
        work_dir=tmp_path
    )

    pipe.run()

    assert pipe.output_path.is_file()


def test_save_stats(tmp_path, input_file):
    inputs = [get_input_file_config(input_file, schema="messages")]
    core_config = get_core_config()
    core_config["threshold"] = 0.1

    pipe = NaivePipeline.build(
        inputs=inputs, core=core_config, work_dir=tmp_path, save_stats=True)

    pipe.run()
    assert pipe.output_path_stats.is_file()


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
    ],
)
def test_gap_between_years(tmp_path, messages, threshold, expected_gaps):
    # Checks that a gap between years is properly detected.

    input_file = tmp_path.joinpath("test.json")
    json_save(messages, input_file)

    core_config = get_core_config()
    core_config["threshold"] = threshold

    inputs = [get_input_file_config(input_file, schema="messages")]

    pipe = NaivePipeline.build(
        inputs=inputs,
        work_dir=tmp_path,
        core=core_config,
        outputs=[get_outputs_config()]
    )
    pipe.run()

    gaps = json_load(pipe.output_path, lines=True)
    assert len(gaps) == expected_gaps


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
def test_open_gaps(tmp_path, messages, threshold, expected_gaps):
    # Checks that a gap between years is properly detected.

    input_file = tmp_path.joinpath("test.json")
    json_save(messages, input_file)

    core_config = get_core_config()
    core_config["threshold"] = threshold
    core_config["eval_last"] = True

    inputs = [get_input_file_config(input_file, schema="messages")]

    pipe = NaivePipeline.build(
        inputs=inputs,
        work_dir=tmp_path,
        core=core_config,
        outputs=[get_outputs_config()]
    )
    pipe.run()

    gaps = json_load(pipe.output_path, lines=True)
    assert len(gaps) == expected_gaps

    if len(gaps) > 0:
        for gap in gaps:
            assert gap["ON"]["msgid"] is None


@pytest.mark.parametrize(
    "messages,open_gaps,expected_gaps,expected_dt,threshold,date_range,window_period_d,eval_last",
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
def test_closing_gaps(
    tmp_path,
    messages, open_gaps, expected_gaps, expected_dt,
    threshold, date_range, window_period_d, eval_last,
):
    # Checks that an existing open gap is properly closed.

    input_file = tmp_path.joinpath("messages-test.json")
    json_save(messages, input_file)
    inputs = [get_input_file_config(input_file, schema="messages")]

    side_input_file = tmp_path.joinpath("open-gaps-test.json")
    json_save(open_gaps, side_input_file, lines=True)
    side_inputs_config = dict(kind="json", input_file=side_input_file, schema="gaps", lines=True)
    side_inputs = [side_inputs_config]

    core_config = get_core_config()
    core_config["threshold"] = threshold
    core_config["window_period_d"] = window_period_d
    core_config["date_range"] = date_range
    core_config["eval_last"] = eval_last

    pipe = NaivePipeline.build(
        inputs=inputs,
        side_inputs=side_inputs,
        work_dir=tmp_path,
        core=core_config,
        outputs=[get_outputs_config()]

    )
    pipe.run()

    gaps = json_load(pipe.output_path, lines=True)
    assert len(gaps) == expected_gaps

    for g in gaps:
        g_start_dt = datetime_from_ts(g["OFF"]["timestamp"])

        g_end_ts = g["ON"]["timestamp"]
        if g_end_ts is not None:
            g_end_ts = datetime_from_ts(g["ON"]["timestamp"])

        g_expected_end_dt = expected_dt[g_start_dt]
        assert g_expected_end_dt == g_end_ts
