from pipe_gaps.pipelines.detect.table_config import (
    GapsTableConfig,
)


def test_gaps_table_config_schema():
    config = GapsTableConfig(table_id="some-table")
    schema = config.schema

    assert isinstance(schema, list)
    assert all("name" in field for field in schema)


def test_gaps_table_config_view_query():
    table_id = "my-project.my_dataset.gaps_table"
    config = GapsTableConfig(table_id=table_id)

    query = config.view_query()

    assert isinstance(query, str)
    assert table_id in query
    assert "SELECT" in query.upper()
