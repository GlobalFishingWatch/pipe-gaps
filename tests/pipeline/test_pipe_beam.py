import pytest
from datetime import datetime, timezone

from pipe_gaps.pipeline.processes import DetectGaps
from pipe_gaps.pipeline.beam.transforms import Core, ReadFromJson
from pipe_gaps.pipeline import BeamPipeline, PipelineError
from pipe_gaps.pipeline import pipe_beam
from pipe_gaps.utils import json_save, json_load

from tests.conftest import TestCases


def get_input_file_config(input_file, schema):
    return dict(kind="json", input_file=input_file, schema=schema)


def get_core_config():
    return dict(kind="detect_gaps", threshold=0.5, eval_last=False)


def get_outputs_config():
    return dict(kind="json", output_prefix="gaps")


def test_with_input_file(input_file):
    core_config = get_core_config()

    inputs_config = [get_input_file_config(input_file, schema="messages")]

    pipe = BeamPipeline.build(inputs=inputs_config, core=core_config)
    pipe.run()


def test_multiple_sources(input_file):
    core = Core(core_process=DetectGaps.build())
    read1 = "Read1" >> ReadFromJson.build(input_file, schema="messages")
    read2 = "Read2" >> ReadFromJson.build(input_file, schema="messages")

    sources = [read1, read2]
    sinks = []

    pipe = BeamPipeline(sources, core, sinks)
    pipe.run()


def test_no_output_path(input_file):
    inputs = [get_input_file_config(input_file, schema="messages")]
    core_config = get_core_config()

    pipe = BeamPipeline.build(inputs=inputs, core=core_config)

    with pytest.raises(PipelineError):
        pipe.output_path.is_file()


def test_distance_from_shore_is_null(tmp_path, messages):
    input_file = tmp_path.joinpath("test.json")
    for m in messages:
        m["distance_from_shore_m"] = None

    json_save(messages, input_file)
    inputs = [get_input_file_config(input_file, schema="messages")]
    core_config = get_core_config()

    pipe = BeamPipeline.build(inputs=inputs, core=core_config)
    pipe.run()


def test_with_input_query():
    input_query = dict(
        kind="query",
        mock_db_client=True,
        query_name="messages",
        query_params=dict(
            start_date=datetime(2024, 1, 1).date().isoformat(),
            end_date=datetime(2024, 1, 1).date().isoformat()
        )
    )
    core_config = get_core_config()

    pipe = BeamPipeline.build(inputs=[input_query], core=core_config)
    pipe.run()


def test_save_json(tmp_path, input_file):
    inputs = [get_input_file_config(input_file, schema="messages")]
    core_config = get_core_config()
    outputs_config = [get_outputs_config()]

    pipe = BeamPipeline.build(
        inputs=inputs,
        work_dir=tmp_path,
        core=core_config,
        outputs=outputs_config
    )

    pipe.run()

    assert pipe.output_path.is_file()


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
    outputs_config = [get_outputs_config()]

    pipe = BeamPipeline.build(
        inputs=inputs,
        work_dir=tmp_path,
        core=core_config,
        outputs=outputs_config
    )
    pipe.run()

    gaps = json_load(pipe.output_path, lines=True)

    # for g in gaps:
    #   print("GAP", datetime.fromtimestamp(g["OFF"]["timestamp"], tz=timezone.utc))

    assert len(gaps) == expected_gaps


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
def test_gap_between_days(tmp_path, messages, open_gaps, threshold, date_range, expected_gaps):
    # Checks that a gap between days is properly detected.

    input_file = tmp_path.joinpath("test.json")
    json_save(messages, input_file)

    core_config = get_core_config()
    core_config["threshold"] = threshold
    core_config["window_period_d"] = 1
    core_config["date_range"] = date_range

    inputs = [get_input_file_config(input_file, schema="messages")]

    side_input_file = tmp_path.joinpath("open-gaps-test.json")
    json_save(open_gaps, side_input_file, lines=True)
    side_inputs_config = dict(
        kind="json", input_file=side_input_file, schema="ais_gaps", lines=True)

    side_inputs = [side_inputs_config]
    outputs_config = [get_outputs_config()]

    pipe = BeamPipeline.build(
        inputs=inputs,
        side_inputs=side_inputs,
        work_dir=tmp_path,
        core=core_config,
        outputs=outputs_config
    )
    pipe.run()

    gaps = json_load(pipe.output_path, lines=True)

    gaps = sorted(gaps, key=lambda x: x["OFF"]["timestamp"])

    for g in gaps:
        print(
            "GAP",
            "\n OFF", datetime.fromtimestamp(g["OFF"]["timestamp"], tz=timezone.utc),
            "\n ON", datetime.fromtimestamp(g["ON"]["timestamp"], tz=timezone.utc)
        )

    assert len(gaps) == len(expected_gaps)

    for gap, expected_gap in zip(gaps, expected_gaps):
        assert gap["positions_hours_before"] == expected_gap["positions_hours_before"]
        assert gap["positions_hours_before_ter"] == expected_gap["positions_hours_before_ter"]
        assert gap["positions_hours_before_sat"] == expected_gap["positions_hours_before_sat"]
        assert gap["positions_hours_before_dyn"] == expected_gap["positions_hours_before_dyn"]


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
def test_gap_between_arbitrary_period(
    tmp_path,
    messages, open_gaps, expected_gaps, threshold, date_range, window_period_d, eval_last,
):
    # Checks that a gap between arbitrary periods is properly detected.

    input_file = tmp_path.joinpath("test.json")
    json_save(messages, input_file)

    core_config = get_core_config()
    core_config["threshold"] = threshold
    core_config["window_period_d"] = window_period_d
    core_config["date_range"] = date_range
    core_config["eval_last"] = eval_last

    inputs = [get_input_file_config(input_file, schema="messages")]

    side_input_file = tmp_path.joinpath("open-gaps-test.json")
    json_save(open_gaps, side_input_file, lines=True)
    side_inputs_config = dict(
        kind="json", input_file=side_input_file, schema="ais_gaps", lines=True)

    side_inputs = [side_inputs_config]
    outputs_config = [get_outputs_config()]

    pipe = BeamPipeline.build(
        inputs=inputs,
        side_inputs=side_inputs,
        work_dir=tmp_path,
        core=core_config,
        outputs=outputs_config
    )
    pipe.run()

    gaps = json_load(pipe.output_path, lines=True)

    gaps = sorted(gaps, key=lambda x: x["OFF"]["timestamp"])

    for g in gaps:
        print(
            "GAP",
            "\n OFF", datetime.fromtimestamp(g["OFF"]["timestamp"], tz=timezone.utc),
        )
        if g["is_closed"]:
            print(" ON", datetime.fromtimestamp(g["ON"]["timestamp"], tz=timezone.utc))
        else:
            print(" ON", None)

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

    pipe = BeamPipeline.build(
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
    "messages, open_gaps, threshold, expected_gaps",
    [
        pytest.param(
            case["messages"],
            case["open_gaps"],
            case["threshold"],
            case["expected_gaps"],
            id=case["id"]
        )
        for case in TestCases.CLOSING_GAPS
    ],
)
def test_closing_gaps(tmp_path, messages, open_gaps, threshold, expected_gaps):
    # Checks that an existing open gap is properly closed.

    input_file = tmp_path.joinpath("messages-test.json")
    json_save(messages, input_file)
    inputs = [get_input_file_config(input_file, schema="messages")]

    side_input_file = tmp_path.joinpath("open-gaps-test.json")
    json_save(open_gaps, side_input_file, lines=True)
    side_inputs_config = dict(
        kind="json", input_file=side_input_file, schema="ais_gaps", lines=True)

    side_inputs = [side_inputs_config]

    core_config = get_core_config()
    core_config["threshold"] = threshold

    pipe = BeamPipeline.build(
        inputs=inputs,
        side_inputs=side_inputs,
        work_dir=tmp_path,
        core=core_config,
        outputs=[get_outputs_config()]
    )
    pipe.run()

    gaps = json_load(pipe.output_path, lines=True)
    assert len(gaps) == expected_gaps

    if len(gaps) > 0:
        for gap in gaps:
            assert gap["ON"]["msgid"] is not None


@pytest.mark.parametrize(
    "messages, threshold, expected_gaps",
    [
        pytest.param(
            case["messages"],
            case["threshold"],
            case["expected_gaps"],
            id=case["id"]
        )
        for case in TestCases.NO_GAPS_DUPLICATION
    ],
)
def test_no_duplicated_gaps(tmp_path, messages, threshold, expected_gaps):
    # Checks the output does not contain duplicated gaps.

    input_file = tmp_path.joinpath("messages-test.json")
    json_save(messages, input_file)
    inputs = [get_input_file_config(input_file, schema="messages")]

    core_config = get_core_config()
    core_config["threshold"] = threshold

    pipe = BeamPipeline.build(
        inputs=inputs,
        work_dir=tmp_path,
        core=core_config,
        outputs=[get_outputs_config()]
    )
    pipe.run()

    gaps = json_load(pipe.output_path, lines=True)
    assert len(gaps) == expected_gaps


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
def test_positions_hours_before(tmp_path, messages, threshold, date_range, expected_gaps):
    input_file = tmp_path.joinpath("messages-test-hours-before.json")
    json_save(messages, input_file)

    core_config = get_core_config()
    core_config["threshold"] = threshold
    core_config["date_range"] = date_range

    inputs = [get_input_file_config(input_file, schema="messages")]

    outputs_config = [get_outputs_config()]

    pipe = BeamPipeline.build(
        inputs=inputs,
        work_dir=tmp_path,
        core=core_config,
        outputs=outputs_config
    )
    pipe.run()

    gaps = json_load(pipe.output_path, lines=True)

    for gap in gaps:
        print("GAP: ", datetime.fromtimestamp(gap["OFF"]["timestamp"], tz=timezone.utc))

    assert len(gaps) == len(expected_gaps)

    for gap, expected_gap in zip(gaps, expected_gaps):
        assert gap["positions_hours_before"] == expected_gap["positions_hours_before"]
        assert gap["positions_hours_before_ter"] == expected_gap["positions_hours_before_ter"]
        assert gap["positions_hours_before_sat"] == expected_gap["positions_hours_before_sat"]
        assert gap["positions_hours_before_dyn"] == expected_gap["positions_hours_before_dyn"]


def test_verbose(tmp_path, input_file):
    import logging
    pipe_beam.logger.setLevel(logging.DEBUG)

    core_config = get_core_config()
    inputs = [get_input_file_config(input_file, schema="messages")]

    pipe = BeamPipeline.build(inputs=inputs, core=core_config)
    pipe.run()
