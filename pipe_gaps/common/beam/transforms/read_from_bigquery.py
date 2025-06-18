"""Module with reusable PTransforms for reading input PCollections."""
from typing import Callable, Any

import apache_beam as beam
from apache_beam import io

from pipe_gaps.common.query import Query


class FakeReadFromBigQuery(io.ReadFromBigQuery):
    """Mocks beam.io.ReadFromBigQuery.

    Args:
        elements: Elements to use as output Pcollection.
    """
    def __init__(self, elements: list[dict] = None, **kwargs):
        self._elements = elements

        if self._elements is None:
            self._elements = []

    def expand(self, pcoll):
        return pcoll | beam.Create(self._elements)


class ReadFromBigQuery(beam.PTransform):
    """Beam transform to read Pcollection from BigQuery table.

    Args:
        query:
            Instance of Query.

        use_schema:
            If true, uses query defined schema as PCollection type. If not, uses dict.

        method:
            The method to use to read from BigQuery. It may be EXPORT or DIRECT_READ.

        use_standard_sql:
            Specifies whether to use BigQuery’s standard SQL dialect for this query.
            Defaults to True.

        read_from_bigquery_factory:
            A factory function used to create a `WriteToBigQuery` instance.
            This is primarily useful for testing, where you may want to inject a custom or fake
            implementation instead of using the real `WriteToBigQuery` transform.
            If not provided, the default `WriteToBigQuery` class will be used.

        **write_to_bigquery_kwargs:
            Any additional keyword arguments to be passed to Beam's `ReadFromBigQuery` class.
            Check official documentation:
            https://beam.apache.org/releases/pydoc/2.64.0/apache_beam.io.gcp.bigquery.html#apache_beam.io.gcp.bigquery.ReadFromBigQuery.
    """

    def __init__(
        self,
        query: Query,
        use_schema: bool = False,
        method: str = beam.io.ReadFromBigQuery.Method.EXPORT,
        use_standard_sql: bool = True,
        label: str = None,
        read_from_bigquery_factory: Callable[..., io.ReadFromBigQuery] = io.ReadFromBigQuery,
        **read_from_bigquery_kwargs: Any,
    ):
        super().__init__(label=label)
        self._query = query
        self._use_schema = use_schema
        self._method = method
        self._label = label
        self._use_standard_sql = use_standard_sql
        self._read_from_bigquery_factory = read_from_bigquery_factory
        self._read_from_bigquery_kwargs = read_from_bigquery_kwargs

    @classmethod
    def get_client_factory(cls, mocked: bool = False) -> Callable:
        """Returns a factory for ReadFromPubSub objects."""
        if mocked:
            return FakeReadFromBigQuery

        return io.ReadFromBigQuery

    def expand(self, pcoll):
        transform = self._read_from_bigquery_factory(
            use_standard_sql=self._use_standard_sql,
            query=self._query.render(),
            method=self._method,
            **self._read_from_bigquery_kwargs
        )

        if self._use_schema:
            transform = transform.with_output_types(self._query.schema())

        return pcoll | transform
