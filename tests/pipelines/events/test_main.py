import pytest
from types import SimpleNamespace

from pipe_gaps.pipelines.events import main
from pipe_gaps.pipelines.events.config import GapEventsConfig
from pipe_gaps.pipelines.events.main import GapEventQuery


@pytest.fixture
def basic_config_kwargs():
    return {
        "date_range": ("2024-01-01", "2024-01-02"),
        "bq_in_raw_gaps": "project.dataset.table",
        "bq_in_segment_info": "project.dataset.table",
        "bq_in_segs_activity": "project.dataset.table",
        "bq_in_regions": "project.dataset.table",
        "bq_in_voyages": "project.dataset.table",
        "bq_in_port_visits": "project.dataset.table",
        "bq_in_vessels_byyear": "project.dataset.table",
        "bq_in_vessels_byyear_field_prefix": "ais_",
        "bq_out_gap_events": "project.dataset.table",
        "unknown_unparsed_args": [],
        "project": "test-project",
        "mock_bq_clients": True,
    }


def test_run(basic_config_kwargs):
    input_config = SimpleNamespace(**basic_config_kwargs)
    main.run(input_config)


class TestFlagField:
    """The field carrying the vessel flag is configurable (PIPELINE-4424).

    It defaults to `<field-prefix>mmsi_flag` — what the query read before the
    argument existed — and VMS pipelines override it with `gfw_best_flag`.
    """

    def _query(self, kwargs):
        config = GapEventsConfig.from_namespace(
            SimpleNamespace(**kwargs, unknown_parsed_args={}),
            version="test",
            name="test",
        )
        return GapEventQuery(config)

    def test_defaults_to_prefixed_mmsi_flag(self, basic_config_kwargs):
        query = self._query(basic_config_kwargs)
        assert query.template_vars["vessel_info_flag_field"] == "ais_mmsi_flag"

    def test_defaults_to_bare_mmsi_flag_without_prefix(self, basic_config_kwargs):
        query = self._query(
            basic_config_kwargs | {"bq_in_vessels_byyear_field_prefix": ""}
        )
        assert query.template_vars["vessel_info_flag_field"] == "mmsi_flag"

    def test_explicit_field_wins(self, basic_config_kwargs):
        query = self._query(
            basic_config_kwargs | {"bq_in_vessels_byyear_flag_field": "gfw_best_flag"}
        )
        assert query.template_vars["vessel_info_flag_field"] == "gfw_best_flag"

    def test_rendered_query_reads_the_resolved_field(self, basic_config_kwargs):
        query = self._query(
            basic_config_kwargs | {"bq_in_vessels_byyear_flag_field": "gfw_best_flag"}
        )
        sql = query.render()
        assert sql.count("gfw_best_flag AS flag") == 2
        assert "ais_mmsi_flag AS flag" not in sql
