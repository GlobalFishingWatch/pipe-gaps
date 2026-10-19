from pipe_gaps.pipelines.events.table_config import (
    GapEventsTableConfig,
    GapEventsTableDescription,
    IntentionalDisablingEventsTableConfig,
    IntentionalDisablingEventsTableDescription,
)


def test_gaps_table_config_schema():
    config = GapEventsTableConfig(table_id="some-table")
    schema = config.schema

    assert isinstance(schema, list)
    assert all("name" in field for field in schema)


def test_gaps_table_config_schema_regions_mean_position_is_built_from_regions():
    """regions_mean_position's fields aren't hardcoded -- they come from whatever `regions`
    rows are passed in (sourced from pipe-regions' own registry, see `main.fetch_regions_
    registry`), so a newly registered region shows up here without a pipe-gaps code change.
    """
    regions = [
        {"name": "eez", "description": "Exclusive Economic Zones."},
        {"name": "imma", "description": "International Marine Mammal Areas."},
    ]
    config = GapEventsTableConfig(table_id="some-table", regions=regions)

    regions_field = next(f for f in config.schema if f["name"] == "regions_mean_position")

    assert regions_field["fields"] == [
        {
            "name": "eez",
            "type": "STRING",
            "mode": "REPEATED",
            "description": "Exclusive Economic Zones.",
        },
        {
            "name": "imma",
            "type": "STRING",
            "mode": "REPEATED",
            "description": "International Marine Mammal Areas.",
        },
    ]


def test_gaps_table_config_schema_regions_mean_position_defaults_to_no_fields():
    config = GapEventsTableConfig(table_id="some-table")

    regions_field = next(f for f in config.schema if f["name"] == "regions_mean_position")

    assert regions_field["fields"] == []


def test_gaps_table_config_with_description():
    table_id = "my-project.my_dataset.gaps_table"
    config = GapEventsTableConfig(
        table_id=table_id,
        description=GapEventsTableDescription()
    )

    assert isinstance(config.description, GapEventsTableDescription)


def test_intentional_disabling_events_table_description():
    description = IntentionalDisablingEventsTableDescription(version="1.0.0").render()

    assert "INTENTIONAL DISABLING EVENTS" in description
    assert "intentional_disabling = true" in description


def test_intentional_disabling_events_table_config_same_schema_no_clustering():
    regions = [{"name": "eez", "description": "Exclusive Economic Zones."}]
    gap_events = GapEventsTableConfig(table_id="gap-events", regions=regions)
    disabling = IntentionalDisablingEventsTableConfig(
        table_id="intentional-disabling-events", regions=regions
    )

    assert disabling.schema == gap_events.schema
    assert disabling.partition_field == gap_events.partition_field == "event_start"
    assert disabling.partition_type == gap_events.partition_type == "MONTH"
    assert disabling.clustering_fields is None
