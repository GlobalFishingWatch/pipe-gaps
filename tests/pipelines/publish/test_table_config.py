from pipe_gaps.pipelines.publish.table_config import (
    GapEventsTableConfig,
    GapEventsTableDescription,
)


def test_gaps_table_config_schema():
    config = GapEventsTableConfig(table_id="some-table")
    schema = config.schema

    assert isinstance(schema, list)
    assert all("name" in field for field in schema)


def test_gaps_table_config_with_description():
    table_id = "my-project.my_dataset.gaps_table"
    config = GapEventsTableConfig(
        table_id=table_id,
        description=GapEventsTableDescription()
    )

    assert isinstance(config.description, GapEventsTableDescription)
