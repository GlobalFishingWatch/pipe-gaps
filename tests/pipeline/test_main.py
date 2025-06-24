import pytest
from types import SimpleNamespace

from pipe_gaps.pipeline import main


@pytest.fixture
def basic_config_kwargs():
    return {
        "date_range": ("2024-01-01", "2024-01-02"),
        "bq_input_messages": "project.dataset.messages",
        "bq_input_segments": "project.dataset.segments",
        "bq_output_gaps": "project.dataset.gaps",
        "unknown_unparsed_args": [],
        "unknown_parsed_args": {},
        "mock_db_client": True,
    }


def test_run(basic_config_kwargs):
    input_config = SimpleNamespace(**basic_config_kwargs)
    main.run(input_config)
