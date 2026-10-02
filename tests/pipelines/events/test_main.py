from types import SimpleNamespace

import pytest

from gfw.common.bigquery.helper import BigQueryHelper
from pipe_gaps.pipelines.events import main
from pipe_gaps.pipelines.events.config import GapEventsConfig
from pipe_gaps.pipelines.events.main import GapEventQuery, fetch_regions_registry


@pytest.fixture
def basic_config_kwargs():
    return {
        "date_range": ("2024-01-01", "2024-01-02"),
        "bq_in_raw_gaps": "project.dataset.table",
        "bq_in_segment_info": "project.dataset.table",
        "bq_in_segs_activity": "project.dataset.table",
        "bq_in_regions": "project.dataset.table",
        "bq_in_regions_registry": "project.dataset.registry",
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


def test_end_date_filter_lets_open_gaps_through(basic_config_kwargs):
    """An open gap (no ``end_timestamp`` yet) must still be included regardless of
    ``end_date`` -- production was silently excluding every open gap from
    ``product_events``, since ``DATE(NULL) < end_date`` is ``NULL``, not ``TRUE``,
    which fails the ``WHERE`` clause instead of passing it.
    """
    config = GapEventsConfig.from_namespace(
        SimpleNamespace(**basic_config_kwargs, unknown_parsed_args={}),
        version="test",
        name="test",
    )
    sql = GapEventQuery(config).render()
    assert "end_timestamp IS NULL OR DATE(end_timestamp) <" in sql


def test_exclude_open_gaps_defaults_to_false(basic_config_kwargs):
    config = GapEventsConfig.from_namespace(
        SimpleNamespace(**basic_config_kwargs, unknown_parsed_args={}),
        version="test",
        name="test",
    )
    sql = GapEventQuery(config).render()
    assert "end_timestamp IS NOT NULL" not in sql


def test_exclude_open_gaps_drops_them_from_the_output(basic_config_kwargs):
    """VMS wants open gaps excluded until the open-gap peaks at the end of the last
    processed day (expected, not a bug) are addressed.
    """
    config = GapEventsConfig.from_namespace(
        SimpleNamespace(**basic_config_kwargs, exclude_open_gaps=True, unknown_parsed_args={}),
        version="test",
        name="test",
    )
    sql = GapEventQuery(config).render()
    assert "AND end_timestamp IS NOT NULL" in sql


def test_fetch_regions_registry_queries_the_given_table():
    bq_helper = BigQueryHelper.mocked()

    fetch_regions_registry(bq_helper, "project.dataset.registry")

    query_str = bq_helper.client.query.call_args.args[0]
    assert "project.dataset.registry" in query_str
    assert "SELECT name, description" in query_str


def test_fetch_regions_registry_always_runs_for_real_under_dry_run():
    """Even with `--dry-run`, this metadata read must actually execute.

    Otherwise it returns no rows (BigQuery dry runs never return data), `regions` ends up
    empty, and the main query renders an invalid `STRUCT<>` (see `utils.sql.j2`) -- breaking
    dry-run validation of a query that would otherwise succeed for real.
    """
    bq_helper = BigQueryHelper.mocked(dry_run=True)

    fetch_regions_registry(bq_helper, "project.dataset.registry")

    job_config = bq_helper.client.query.call_args.kwargs["job_config"]
    assert job_config.dry_run is False


class TestRegions:
    """The region struct/schema is built from the registry, not hardcoded (see
    `fetch_regions_registry`).
    """

    _REGIONS = [
        {"name": "eez", "description": "Exclusive Economic Zones."},
        {"name": "imma", "description": "International Marine Mammal Areas."},
    ]

    def _query(self, kwargs, regions=_REGIONS):
        config = GapEventsConfig.from_namespace(
            SimpleNamespace(**kwargs, unknown_parsed_args={}),
            version="test",
            name="test",
        )
        return GapEventQuery(config, regions=regions)

    def test_defaults_to_no_regions(self, basic_config_kwargs):
        query = self._query(basic_config_kwargs, regions=())
        assert query.template_vars["regions"] == []

    def test_template_vars_regions_come_from_registry_rows(self, basic_config_kwargs):
        query = self._query(basic_config_kwargs)
        assert query.template_vars["regions"] == ["eez", "imma"]

    def test_rendered_query_struct_matches_registry_regions(self, basic_config_kwargs):
        query = self._query(basic_config_kwargs)
        sql = query.render()

        assert "eez ARRAY<STRING>" in sql
        assert "imma ARRAY<STRING>" in sql
        assert "this.eez = regions.eez || [];" in sql
        assert "this.imma = regions.imma || [];" in sql

        # A region not in the registry passed in must not silently reappear from a stale
        # hardcoded template ("rfmo" itself still legitimately appears elsewhere in the query,
        # in the unrelated is_whitelisted_rfmo function).
        assert "rfmo ARRAY<STRING>" not in sql
        assert "this.rfmo = regions.rfmo || [];" not in sql
