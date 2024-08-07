import pytest

from pipe_gaps.data import get_sample_messages


@pytest.fixture(scope="module")
def messages():
    return get_sample_messages().copy()
