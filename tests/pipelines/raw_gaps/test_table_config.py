from pipe_gaps.pipelines.raw_gaps.table_config import (
    GapsTableConfig,
    GapsLatestViewConfig,
)


def test_gaps_table_config_schema():
    config = GapsTableConfig(table_id="some-table")
    schema = config.schema

    assert isinstance(schema, list)
    assert all("name" in field for field in schema)


def test_gaps_latest_view_config_view_query():
    table_id = "my-project.my_dataset.gaps_table"
    table_config = GapsTableConfig(table_id=table_id)
    view_config = GapsLatestViewConfig(source=table_config)

    query = view_config.view_query()

    assert isinstance(query, str)
    assert table_id in query
    assert "SELECT" in query.upper()


def test_gaps_latest_view_config_view_id():
    table_id = "my-project.my_dataset.gaps_table"
    table_config = GapsTableConfig(table_id=table_id)
    view_config = GapsLatestViewConfig(source=table_config)

    assert view_config.view_id == f"{table_id}_latest"


def test_gaps_latest_view_config_schema_matches_source():
    table_config = GapsTableConfig(table_id="some-table")
    view_config = GapsLatestViewConfig(source=table_config)

    assert view_config.schema == table_config.schema
