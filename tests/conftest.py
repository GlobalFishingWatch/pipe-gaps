import pytest

from pipe_gaps.utils import get_sample_messages


@pytest.fixture(scope="module")
def messages():
    return get_sample_messages()
