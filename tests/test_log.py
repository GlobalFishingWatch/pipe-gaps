from pipe_gaps.log import setup_logger


def test_setup_logger():
    setup_logger(warning_level=("apache_beam.io.gcp"))
