import pytest
from types import SimpleNamespace

from pipe_gaps.pipelines.raw_gaps import main


@pytest.fixture
def basic_config_kwargs():
    return {
        "date_range": ("2024-01-01", "2024-01-02"),
        "bq_in_messages": "project.dataset.messages",
        "bq_in_segments": "project.dataset.segments",
        "bq_out_gaps": "project.dataset.gaps",
        "unknown_unparsed_args": [],
        "unknown_parsed_args": {"project": "test-project"},
        "mock_bq_clients": True,
    }


def test_run(basic_config_kwargs):
    input_config = SimpleNamespace(**basic_config_kwargs)
    main.run(input_config)
