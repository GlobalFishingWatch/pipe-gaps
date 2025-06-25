from pipe_gaps.pipeline.table_config import (
    RawGapsTableConfig,
    SCHEMA_FILE,
    VIEW_SUFFIX,
    PARTITION_TYPE,
    PARTITION_FIELD,
    CLUSTERING_FIELDS,
)


def test_raw_gaps_table_config_properties():
    table_id = "project.dataset.table"
    config = RawGapsTableConfig(table_id=table_id)

    assert config.schema_file == SCHEMA_FILE
    assert config.view_suffix == VIEW_SUFFIX
    assert config.partition_type == PARTITION_TYPE
    assert config.partition_field == PARTITION_FIELD
    assert config.clustering_fields == CLUSTERING_FIELDS
    assert config.description_template.startswith("「 ✦ 𝚁𝙰𝚆 𝙶𝙰𝙿𝚂 ✦ 」")


def test_raw_gaps_table_config_schema():
    config = RawGapsTableConfig(table_id="some-table")
    schema = config.schema

    assert isinstance(schema, list)
    assert all("name" in field for field in schema)


def test_raw_gaps_table_config_view_query():
    table_id = "my-project.my_dataset.gaps_table"
    config = RawGapsTableConfig(table_id=table_id)

    query = config.view_query

    assert isinstance(query, str)
    assert table_id in query
    assert "SELECT" in query.upper()
