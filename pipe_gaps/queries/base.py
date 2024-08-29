from abc import ABC, abstractmethod


class QueryError(Exception):
    pass


class Query(ABC):
    @abstractmethod
    def render():
        """Renders query."""

    @classmethod
    def subclasses(cls):
        return {x.NAME: x for x in cls.__subclasses__()}


def get_query(query_name: str, query_params: dict) -> Query:
    q = Query.subclasses()

    if query_name not in q:
        raise NotImplementedError(f"Query with name '{query_name}' not implemented.")

    return q[query_name](**query_params)
