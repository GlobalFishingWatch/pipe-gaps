from pipe_gaps.pipelines.events.table_config import (
    GapEventsTableConfig,
    GapEventsTableDescription,
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
        {"region": "eez", "description": "Exclusive Economic Zones."},
        {"region": "imma", "description": "International Marine Mammal Areas."},
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
