import pytest
from types import SimpleNamespace

from pipe_gaps.pipelines.publish import main


@pytest.fixture
def basic_config_kwargs():
    return {
        "date_range": ("2024-01-01", "2024-01-02"),
        "bq_input_gaps": "project.dataset.table",
        "bq_input_segment_info": "project.dataset.table",
        "bq_input_segs_activity": "project.dataset.table",
        "bq_input_regions": "project.dataset.table",
        "bq_input_voyages": "project.dataset.table",
        "bq_input_port_visits": "project.dataset.table",
        "bq_input_vessels_byyear": "project.dataset.table",
        "bq_output": "project.dataset.table",
        "unknown_unparsed_args": [],
        "project": "test-project",
        "mock_bq_clients": True,
    }


def test_run(basic_config_kwargs):
    input_config = SimpleNamespace(**basic_config_kwargs)
    main.run(input_config)
