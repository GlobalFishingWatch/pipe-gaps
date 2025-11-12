from pipe_gaps.pipelines.raw_gaps_events.table_config import (
    RawGapsEventsTableConfig,
    RawGapsEventsTableDescription,
)


def test_raw_gaps_table_config_schema():
    config = RawGapsEventsTableConfig(table_id="some-table")
    schema = config.schema

    assert isinstance(schema, list)
    assert all("name" in field for field in schema)


def test_raw_gaps_table_config_with_description():
    table_id = "my-project.my_dataset.gaps_table"
    config = RawGapsEventsTableConfig(
        table_id=table_id,
        description=RawGapsEventsTableDescription()
    )

    assert isinstance(config.description, RawGapsEventsTableDescription)
