from pipe_gaps.pipelines.detect.table_config import GapsTableConfig


def test_gaps_table_config_property(base_config):
    config = base_config.table_config

    assert isinstance(config, GapsTableConfig)
    assert config.table_id == base_config.bq_output_gaps


def test_bq_output_gaps_description_params(base_config):
    params = base_config.bq_output_gaps_description_params

    assert params["bq_input_messages"] == base_config.bq_input_messages
    assert params["bq_input_segments"] == base_config.bq_input_segments
    assert params["filter_good_seg"] == base_config.filter_good_seg
    assert params["min_gap_length"] == base_config.min_gap_length
    assert params["n_hours_before"] == base_config.n_hours_before
    assert (
        params["filter_not_overlapping_and_short"]
        == base_config.filter_not_overlapping_and_short
    )
