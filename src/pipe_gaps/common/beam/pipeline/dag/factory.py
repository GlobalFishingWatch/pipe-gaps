from unittest import mock
from functools import partial
from abc import ABC, abstractmethod
from typing import Optional

from google.cloud import bigquery

from apache_beam import PTransform

from gfw.common.beam.pipeline.dag import LinearDag
from gfw.common.beam.transforms import WriteToPartitionedBigQuery
from gfw.common.bigquery_helper import BigQueryHelper

from pipe_gaps.common.beam.transforms.read_from_bigquery import ReadFromBigQuery


class DagFactory(ABC):
    @property
    def read_from_bigquery_factory(self):
        # Returns a factory for ReadFromBigQuery clients, with optional mocking.
        return ReadFromBigQuery.get_client_factory(mocked=self.config.mock_bq_clients)

    @property
    def write_to_bigquery_factory(self):
        # Returns a factory for WriteToPartitionedBigQuery clients, with optional mocking.
        return WriteToPartitionedBigQuery.get_client_factory(mocked=self.config.mock_bq_clients)

    @property
    def bigquery_helper_factory(self):
        # Move this fix to gfw-common.
        def mock_client_factory(*args, **kwargs):
            # Extract project from kwargs or use default None.
            # Otherwise project is not set and is needed.
            project = kwargs.get("project", None)
            client = mock.create_autospec(bigquery.Client, project=project, instance=True)
            return client

        if self.config.mock_bq_clients:
            client_factory = mock_client_factory
        else:
            client_factory = bigquery.client.Client

        return partial(BigQueryHelper, client_factory=client_factory)

    @abstractmethod
    def build_dag(self) -> tuple[PTransform, ...]:
        """Builds the DAG. Implement this in subclass."""


class LinearDagFactory(DagFactory, ABC):
    @property
    @abstractmethod
    def sources(self) -> tuple[PTransform, ...]:
        """Returns sources for the LinearDag."""

    @property
    @abstractmethod
    def core(self) -> PTransform:
        """Returns core PTransform for the LinearDag."""

    @property
    def side_inputs(self) -> Optional[PTransform]:
        """Returns side inputs PTransform for the LinearDag."""

    @property
    @abstractmethod
    def sinks(self) -> tuple[PTransform, ...]:
        """Returns sinks for the LinearDag."""

    def build_dag(self) -> LinearDag:
        """Builds a LinearDag instance from the configured pipeline parts."""
        return LinearDag(
            sources=tuple(self.sources),
            core=self.core,
            side_inputs=self.side_inputs,
            sinks=tuple(self.sinks),
        )
