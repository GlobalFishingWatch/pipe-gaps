import pytest
from unittest import mock
from types import SimpleNamespace

from apache_beam import PTransform, Map
from gfw.common.beam.pipeline.dag import LinearDag

from pipe_gaps.common.beam.pipeline.dag.factory import DagFactory, LinearDagFactory


# --- Dummy PTransform for test ---
class DummyTransform(PTransform):
    def expand(self, pcoll):
        return pcoll | "DummyMap" >> Map(lambda x: x)


# --- Test Classes ---
def test_dag_factory_is_abstract():
    with pytest.raises(TypeError):
        DagFactory()


def test_linear_dag_factory_is_abstract():
    # Missing abstract properties `sources`, `core`, and `sinks`
    class IncompleteFactory(LinearDagFactory):
        pass

    with pytest.raises(TypeError):
        IncompleteFactory()


def test_build_linear_dag():
    class CompleteFactory(LinearDagFactory):
        def __init__(self):
            self.config = SimpleNamespace(mock_bq_clients=False)

        @property
        def sources(self):
            return (DummyTransform(),)

        @property
        def core(self):
            return DummyTransform()

        @property
        def sinks(self):
            return (DummyTransform(),)

    factory = CompleteFactory()
    dag = factory.build_dag()

    assert isinstance(dag, LinearDag)
    assert isinstance(dag._core, PTransform)
    assert isinstance(dag._sinks, tuple)
    assert isinstance(dag._sources, tuple)


def test_bigquery_factories_mocked():
    class MockedFactory(DagFactory):
        def __init__(self):
            self.config = SimpleNamespace(mock_bq_clients=True)

        def build_dag(self):
            pass

    factory = MockedFactory()
    read_factory = factory.read_from_bigquery_factory
    write_factory = factory.write_to_bigquery_factory
    bq_helper = factory.bigquery_helper_factory()

    assert callable(read_factory)
    assert callable(write_factory)
    assert isinstance(bq_helper.client, mock.NonCallableMagicMock)


def test_bigquery_factories_real():
    class RealFactory(DagFactory):
        def __init__(self):
            self.config = SimpleNamespace(mock_bq_clients=False)

        def build_dag(self):
            pass

    factory = RealFactory()
    read_factory = factory.read_from_bigquery_factory
    write_factory = factory.write_to_bigquery_factory
    bq_helper_partial = factory.bigquery_helper_factory

    assert callable(read_factory)
    assert callable(write_factory)
    assert isinstance(bq_helper_partial.keywords["client_factory"], type)
