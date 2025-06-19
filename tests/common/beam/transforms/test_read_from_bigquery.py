from datetime import datetime

import pytest
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.testing.test_pipeline import TestPipeline as _TestPipeline

from pipe_gaps.common.beam.transforms.read_from_bigquery import (
    ReadFromBigQuery,
    FakeReadFromBigQuery,
)
from pipe_gaps.queries import AISMessagesQuery


@pytest.fixture
def query_params():
    return dict(
        start_date=datetime(2024, 1, 1),
        end_date=datetime(2024, 1, 2)
    )


@pytest.fixture
def query(query_params):
    return AISMessagesQuery(**query_params)


@pytest.fixture
def messages():
    return [
        {"ssvid": 1234, "timestamp": datetime(2024, 1, 1).timestamp(), "distance_from_shore_m": 1},
        {"ssvid": 5678, "timestamp": datetime(2024, 1, 1).timestamp(), "distance_from_shore_m": 2},
    ]


def test_read_from_query_with_schema_true(query, messages):
    query.schema = lambda: dict  # Patch schema so its easier to test.

    tr = ReadFromBigQuery(
        query=query,
        use_schema=True,
        read_from_bigquery_factory=FakeReadFromBigQuery,
        read_from_bigquery_kwargs={"elements": messages}
    )

    with _TestPipeline() as p:
        output = p | tr
        assert_that(output, equal_to(messages))


def test_read_from_query_with_schema_false(query):
    messages = None
    tr = ReadFromBigQuery(
        query=query,
        read_from_bigquery_factory=FakeReadFromBigQuery,
        read_from_bigquery_kwargs={"elements": messages}
    )

    with _TestPipeline() as p:
        output = p | tr
        assert_that(output, equal_to([]))


def test_get_client_factory_returns_fake():
    factory = ReadFromBigQuery.get_client_factory(mocked=True)
    assert factory is FakeReadFromBigQuery


def test_get_client_factory_returns_real():
    factory = ReadFromBigQuery.get_client_factory(mocked=False)
    assert factory.__name__ == "ReadFromBigQuery"
