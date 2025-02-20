import typing

from datetime import datetime
from abc import ABC, abstractmethod


class QueryError(Exception):
    pass


class Query(ABC):
    @abstractmethod
    def render(self):
        """Renders query."""

    @abstractmethod
    def schema(self):
        """Defines schema for the query."""

    @classmethod
    def subclasses(cls):
        return {x.NAME: x for x in cls.__subclasses__()}

    @classmethod
    def datetime_to_timestamp(cls, field: str):
        return "CAST(UNIX_MICROS({field}) AS FLOAT64) / 1000000  AS {field}".format(field=field)

    @classmethod
    def select_clause(cls):
        fields = typing.get_type_hints(cls.schema())

        clause = ""

        for i, (field, class_) in enumerate(fields.items()):
            if class_ == datetime:
                field = cls.datetime_to_timestamp(field)

            if i == len(fields) - 1:  # last item
                clause += f"{field}"
                continue

            clause += f"{field}, \n"

        return clause

    @classmethod
    def where_clause(cls, filters):
        where_clause = ""
        if len(filters) > 0:
            it = iter(filters)

            where_clause = f"WHERE {next(it)}"

            for filter_ in it:
                where_clause += f" AND {filter_}"

        return where_clause


def get_query(query_name: str, query_params: dict) -> Query:
    q = Query.subclasses()

    if query_name not in q:
        raise NotImplementedError(f"Query with name '{query_name}' not implemented.")

    return q[query_name](**query_params)
