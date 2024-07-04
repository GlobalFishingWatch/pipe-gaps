import os
import json

import pytest

TEST_DATA_DIR = "tests/data"


def get_messages():
    data_file = os.path.join(TEST_DATA_DIR, "messages.json")
    with open(data_file) as file:
        return json.load(file)


@pytest.fixture(scope="module")
def messages():
    return get_messages()
