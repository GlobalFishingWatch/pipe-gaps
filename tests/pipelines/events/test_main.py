import re
from types import SimpleNamespace

import pytest

from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from gfw.common.bigquery.helper import BigQueryHelper
from pipe_gaps.pipelines.events import main
from pipe_gaps.pipelines.events.config import GapEventsConfig
from pipe_gaps.pipelines.events.main import GapEventQuery, fetch_regions_registry


def _tables_created_on_write(client, existing=None):
    """get_table side effect: a table exists only once a query has written to it, or if it is
    ``existing``.
    """

    def get_table(table_ref):
        key = str(table_ref)
        if existing is not None and key == str(existing.reference):
            return existing

        written = {
            str(call.kwargs["job_config"].destination)
            for call in client.query.call_args_list
            if call.kwargs["job_config"].destination is not None
        }
        if key not in written:
            raise NotFound(key)

        return bigquery.Table(key)

    return get_table


@pytest.fixture
def basic_config_kwargs():
    return {
        "labels": {"env": "test"},
        "start_date": "2024-01-01",
        "end_date": "2024-01-02",
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
        )
        return GapEventQuery.from_config(config)

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
    )
    sql = GapEventQuery.from_config(config).render()
    assert "end_timestamp IS NULL OR DATE(end_timestamp) <" in sql


def test_exclude_open_gaps_defaults_to_false(basic_config_kwargs):
    config = GapEventsConfig.from_namespace(
        SimpleNamespace(**basic_config_kwargs, unknown_parsed_args={}),
    )
    sql = GapEventQuery.from_config(config).render()
    assert "end_timestamp IS NOT NULL" not in sql


def test_exclude_open_gaps_drops_them_from_the_output(basic_config_kwargs):
    """VMS wants open gaps excluded until the open-gap peaks at the end of the last
    processed day (expected, not a bug) are addressed.
    """
    config = GapEventsConfig.from_namespace(
        SimpleNamespace(**basic_config_kwargs, exclude_open_gaps=True, unknown_parsed_args={}),
    )
    sql = GapEventQuery.from_config(config).render()
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
    )
    sql = GapEventQuery.from_config(config).render()

    assert "COALESCE(" in sql
    assert "TIMESTAMP_DIFF(TIMESTAMP('2024-01-02'), start_timestamp, SECOND) / 3600.0" in sql
    assert "AS effective_duration_h" in sql

    # duration_h keeps its original meaning (NULL for a still-open gap) in the output --
    # effective_duration_h is exposed as its own field instead of overwriting duration_h.
    assert "effective_duration_h AS duration_h" not in sql
    assert re.search(r"duration_h,\s+effective_duration_h,", sql)


def _normalize_whitespace(sql):
    return " ".join(sql.split())


def _disabling_classification_query(kwargs):
    config = GapEventsConfig.from_namespace(
        SimpleNamespace(**kwargs, unknown_parsed_args={}),
    )
    return GapEventQuery.from_config(config)


def test_disabling_classification_disabled_by_default(basic_config_kwargs):
    """Classification is optional (PIPELINE-4615): disabled by default, the query doesn't
    reference reception at all, and the classification fields are NULL ("not classified").
    """
    sql = _normalize_whitespace(_disabling_classification_query(basic_config_kwargs).render())

    assert "reception AS (" not in sql
    assert "JOIN reception" not in sql
    assert "reception.positions_per_day" not in sql
    assert "CAST(NULL AS FLOAT64) AS positions_per_day_sat_reception" in sql
    assert "CAST(NULL AS BOOL) AS intentional_disabling" in sql


def test_disabling_classification_requires_reception_table_when_enabled(basic_config_kwargs):
    with pytest.raises(ValueError, match="bq_in_sat_reception"):
        _disabling_classification_query(basic_config_kwargs | {"classify_disabling": True})


def test_disabling_classification_enabled_adds_reception_join_and_classification(
    basic_config_kwargs,
):
    """Enabled, it joins satellite reception quality and classifies intentional_disabling
    inside event_info -- it never filters out a gap-event row.
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
    assert "AS positions_per_day_sat_reception" in sql
    assert ") AS intentional_disabling" in sql
    assert "positions_hours_before_sat" in sql
    assert "effective_duration_h >= 12" in sql
    assert "start_distance_from_shore_m > 92600" in sql
    assert "reception.positions_per_day" in sql and "> 10" in sql
    assert "positions_hours_before_sat >= 14" in sql
    assert sql.count("gaps.year < 2017") == 2


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


def test_disabling_classification_skips_gaps_before_min_year(basic_config_kwargs):
    """Gaps before disabling_min_year are not classified, as satellite reception was too
    sparse before 2017 to tell (PIPELINE-4662): both intentional_disabling and
    positions_per_day_sat_reception are guarded by the year. The latter needs its own guard
    because reception is averaged over all time, so the join still matches older gaps.
    """
    sql = _disabling_classification_query(
        basic_config_kwargs
        | {
            "classify_disabling": True,
            "bq_in_sat_reception": "project.dataset.sat_reception",
            "disabling_min_year": 2019,
        }
    ).render()

    assert sql.count("gaps.year < 2019") == 2


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
        )
        return GapEventQuery.from_config(config, regions=regions)

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


@pytest.fixture
def gap_events_config_kwargs(basic_config_kwargs):
    return basic_config_kwargs | {"bq_out_gap_events": "p.d.gap_events"}


def _run(kwargs):
    bq_helper = BigQueryHelper.mocked()
    # Output tables don't exist yet: the queries create them.
    bq_helper.client.get_table.side_effect = _tables_created_on_write(bq_helper.client)
    main.run(SimpleNamespace(**kwargs), bq_client_factory=lambda **_: bq_helper.client)
    return bq_helper.client


def _destination_job_configs(client):
    return [
        call.kwargs["job_config"]
        for call in client.query.call_args_list
        if call.kwargs["job_config"].destination is not None
    ]


def test_gap_events_table_is_partitioned_by_event_start_month(gap_events_config_kwargs):
    """product_events was created unpartitioned, as the partitioning declared in its table
    config was never passed to the query.
    """
    (job_config,) = _destination_job_configs(_run(gap_events_config_kwargs))

    assert job_config.destination.table_id == "gap_events"
    assert job_config.time_partitioning.field == "event_start"
    assert job_config.time_partitioning.type_ == "MONTH"
    assert job_config.clustering_fields == ["seg_id"]


def test_gap_events_table_gets_its_schema_and_description(mocker, gap_events_config_kwargs):
    update_table_metadata = mocker.patch.object(
        BigQueryHelper, "update_table_metadata", autospec=True
    )

    _run(gap_events_config_kwargs)

    (call,) = update_table_metadata.call_args_list
    assert call.args[1] == "p.d.gap_events"
    assert len(call.kwargs["schema"]) > 0
    assert "GAP EVENTS" in call.kwargs["description"]


def test_gap_events_table_allows_recreation_with_its_config_layout(
    mocker, gap_events_config_kwargs
):
    """Recreating a table whose layout differs from its config is left to gfw-common
    (allow_table_recreation), as WRITE_TRUNCATE can't change it.
    """
    run_query = mocker.spy(BigQueryHelper, "run_query")

    _run(gap_events_config_kwargs)

    (kwargs,) = [c.kwargs for c in run_query.call_args_list if c.kwargs.get("destination")]
    assert kwargs["destination"] == "p.d.gap_events"
    assert kwargs["write_disposition"] == "WRITE_TRUNCATE"
    assert kwargs["partition_field"] == "event_start"
    assert kwargs["partition_type"] == "MONTH"
    assert kwargs["allow_table_recreation"] is True
    assert kwargs["labels"] == {"env": "test"}
