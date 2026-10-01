from pipe_gaps.pipelines.raw_gaps.table_config import GapsTableConfig, GapsLastVersionsViewConfig


def test_gaps_table_config_property(base_config):
    config = base_config.table_config

    assert isinstance(config, GapsTableConfig)
    assert config.table_id == base_config.bq_out_gaps


def test_gaps_view_config_property(base_config):
    view_config = base_config.view_config

    assert isinstance(view_config, GapsLastVersionsViewConfig)
    assert view_config.source is base_config.table_config
    assert view_config.min_gap_length == base_config.min_gap_length
    assert view_config.description is not None


def test_bq_out_gaps_description_params(base_config):
    params = base_config.bq_out_gaps_description_params

    assert params["bq_in_messages"] == base_config.bq_in_messages
    assert params["bq_in_segments"] == base_config.bq_in_segments
    assert params["filter_good_seg"] == base_config.filter_good_seg
    assert params["min_gap_length"] == base_config.min_gap_length
    assert params["n_hours_before"] == base_config.n_hours_before
    assert (
        params["filter_not_overlapping_and_short"]
        == base_config.filter_not_overlapping_and_short
    )
