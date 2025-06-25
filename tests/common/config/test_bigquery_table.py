import pytest
from pipe_gaps.common.config.bigquery_table import BigQueryTableConfig


class DummyTableConfig(BigQueryTableConfig):
    @property
    def schema(self):
        return [{"name": "id", "type": "STRING"}]


@pytest.fixture
def config():
    return DummyTableConfig(
        table_id="project.dataset.table",
        schema_file="schema.json",
        project="my-project",
        partition_field="timestamp",
        clustering_fields=("vessel_id",),
    )


def test_view_id_property(config):
    assert config.view_id == "project.dataset.table_view"


def test_schema_property(config):
    assert config.schema == [{"name": "id", "type": "STRING"}]


def test_to_dict_with_description(config):
    version = "1.2.3"
    params = {"source": "AIS", "country": "AR"}

    result = config.to_dict(version=version, description_params=params)

    assert result["table"] == config.table_id
    assert result["schema"] == config.schema
    assert result["project"] == config.project
    assert result["partition_type"] == config.partition_type
    assert result["partition_field"] == config.partition_field
    assert result["clustering_fields"] == config.clustering_fields
    assert result["write_disposition"] == config.write_disposition
    assert "description" in result
    assert "AIS" in result["description"]
    assert '"country": "AR"' in result["description"]
    assert "1.2.3" not in result["description"]  # version is not interpolated directly


def test_to_dict_without_description(config):
    result = config.to_dict(version="x", description_params={}, description_enabled=False)
    assert "description" not in result
