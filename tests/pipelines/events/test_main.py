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


def test_open_gap_duration_falls_back_to_elapsed_time_to_end_date(basic_config_kwargs):
    """duration_h is NULL for an open gap (no ON message yet) -- fall back to the
    elapsed time up to end_date (the run's own "as of" boundary) instead, so an open
    gap can still be evaluated by duration-based conditions (e.g. disabling
    classification) rather than permanently failing them.

    Deliberately end_date, not CURRENT_TIMESTAMP(): a historical backfill for a given
    window must stay reproducible, not depend on whenever it happens to be rerun.
    """
    config = GapEventsConfig.from_namespace(
        SimpleNamespace(**basic_config_kwargs, unknown_parsed_args={}),
        version="test",
        name="test",
    )
    sql = GapEventQuery(config).render()

    assert "COALESCE(" in sql
    assert "TIMESTAMP_DIFF(TIMESTAMP('2024-01-02'), start_timestamp, SECOND) / 3600.0" in sql
    assert "AS effective_duration_h" in sql

    # duration_h keeps its original meaning (NULL for a still-open gap) in the output --
    # effective_duration_h is exposed as its own field instead of overwriting duration_h.
    assert "effective_duration_h AS duration_h" not in sql
    assert "duration_h,\n                    effective_duration_h," in sql


def _disabling_classification_query(kwargs):
    config = GapEventsConfig.from_namespace(
        SimpleNamespace(**kwargs, unknown_parsed_args={}),
        version="test",
        name="test",
    )
    return GapEventQuery(config)


def test_disabling_classification_disabled_by_default(basic_config_kwargs):
    """Classification is optional and additive-only (PIPELINE-4615): disabled by
    default, the rendered query carries none of this.
    """
    sql = _disabling_classification_query(basic_config_kwargs).render()

    assert "reception AS (" not in sql
    assert "LEFT JOIN reception" not in sql
    assert "intentional_disabling" not in sql
    assert "positions_per_day_sat_reception" not in sql


def test_disabling_classification_requires_reception_table_when_enabled(basic_config_kwargs):
    with pytest.raises(ValueError, match="bq_in_sat_reception"):
        _disabling_classification_query(basic_config_kwargs | {"classify_disabling": True})


def test_disabling_classification_enabled_adds_reception_join_and_classification(
    basic_config_kwargs,
):
    """Enabled, it joins satellite reception quality and adds intentional_disabling
    (and the fields that drove it) inside event_info -- it never filters out a
    gap-event row.
    """
    sql = _disabling_classification_query(
        basic_config_kwargs
        | {
            "classify_disabling": True,
            "bq_in_sat_reception": "project.dataset.sat_reception",
        }
    ).render()

    assert "FROM `project.dataset.sat_reception`" in sql
    assert "GROUP BY 1, 2, 3" in sql
    assert "FLOOR(gaps.start_lat) = reception.lat_bin" in sql
    assert "FLOOR(gaps.start_lon) = reception.lon_bin" in sql
    assert "gaps.start_ais_class = reception.class" in sql
    assert "reception.positions_per_day AS positions_per_day_sat_reception" in sql
    assert "positions_hours_before_sat" in sql
    assert "effective_duration_h >= 12" in sql
    assert "start_distance_from_shore_m > 92600" in sql
    assert "reception.positions_per_day" in sql and "> 10" in sql
    assert "positions_hours_before_sat >= 14" in sql


def test_disabling_classification_custom_thresholds_are_rendered(basic_config_kwargs):
    sql = _disabling_classification_query(
        basic_config_kwargs
        | {
            "classify_disabling": True,
            "bq_in_sat_reception": "project.dataset.sat_reception",
            "disabling_min_gap_duration_h": 6,
            "disabling_min_distance_from_shore_m": 1000,
            "disabling_min_reception_positions_per_day": 5,
            "disabling_min_positions_before": 2,
        }
    ).render()

    assert "effective_duration_h >= 6" in sql
    assert "start_distance_from_shore_m > 1000" in sql
    assert "reception.positions_per_day" in sql and "> 5" in sql
    assert "positions_hours_before_sat >= 2" in sql


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
