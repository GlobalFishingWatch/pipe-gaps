import os
import json

import pytest

TEST_DATA_DIR = "tests/data"


def get_messages():
    DATA_FILE = os.path.join(TEST_DATA_DIR, "messages.json")
    with open(DATA_FILE) as file:
        return json.load(file)


@pytest.fixture(scope="module")
def messages():
    return get_messages()
