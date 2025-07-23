from __future__ import annotations  # Avoids forward reference problem in type hints
import logging
from typing import Optional, NamedTuple, get_type_hints

from datetime import datetime
from abc import ABC, abstractmethod
from functools import cached_property

import sqlparse
from jinja2 import Environment

from pipe_gaps.common.jinja2 import create_environment


logger = logging.getLogger(__name__)


class Query(ABC):

    _jinja_env: Optional[Environment] = None

    @classmethod
    def subclasses(cls) -> dict[str, type[Query]]:
        """Returns a dictionary of all Query subclasses keyed by their NAME attribute."""
        return {x.NAME: x for x in cls.__subclasses__() if hasattr(x, 'NAME')}

    @classmethod
    def datetime_to_timestamp(cls, field: str) -> str:
        """Converts a datetime field to a Unix timestamp (FLOAT64 in seconds) in BigQuery SQL."""
        return f"CAST(UNIX_MICROS({field}) AS FLOAT64) / 1000000 AS {field}"

    @abstractmethod
    @cached_property
    def output_type(self) -> type[NamedTuple]:
        """Defines the concrete Python type for the elements yielded by this query.

        This abstract property must be implemented by each `Query` subclass to specify
        the exact `typing.NamedTuple` that represents the schema of the data records
        returned by its specific SQL query. The fields of this `NamedTuple` are used
        to dynamically construct the `SELECT` clause of the SQL query.

        Returns:
            A `typing.NamedTuple` subclass that precisely defines the structure
            and types of the output records for this particular query.
        """
        pass

    # @abstractmethod
    @cached_property
    def template_filename(self) -> dict:
        """Returns the filename to the Jinja2 template."""

    # @abstractmethod
    @cached_property
    def template_vars(self) -> dict:
        """Returns the variables to be passed to Jinja2 template."""

    @cached_property
    def jinja_env(self) -> Environment:
        """Returns the jinja environment encapsulated in this instance."""
        if self._jinja_env is None:
            self._jinja_env = create_environment(folder_pattern="**/assets/queries")

        return self._jinja_env

    def with_env(self, env: Environment) -> Query:
        """Setter for the Jinja environment, returns self for chaining."""
        self._jinja_env = env
        return self

    def render(self, formatted: bool = False) -> str:
        """Renders the Query using Jinja2.

        Args:
            formatted:
                If True, formats the query to have proper indentation.
                Defaults to False.

        Returns:
            The rendered (and possibly formatted) query.
        """
        template = self.jinja_env.get_template(self.template_filename)

        query = template.render(self.template_vars)

        formatted_query = self.format(query)

        logger.debug(f"Rendered Query for {self}: ")
        logger.debug(formatted_query)

        if formatted:
            return formatted_query

        return query

    def get_select_fields(self) -> str:
        """Generates the SELECT clause fields based on the schema NamedTuple."""
        fields = get_type_hints(self.output_type)

        clause_parts = []
        for field, class_ in fields.items():
            if class_ == datetime:
                clause_parts.append(self.datetime_to_timestamp(field))
            else:
                clause_parts.append(field)

        return ",".join(clause_parts)

    def get_where_clause(self, filters: list[str]) -> str:
        """Generates a WHERE clause from a list of filter conditions."""
        if not filters:
            return ""

        return "WHERE " + " AND ".join(filters)

    @staticmethod
    def format(query: str) -> str:
        return sqlparse.format(
            query,
            reindent=True,
            use_space_around_operators=True,
            strip_comments=True,
            keyword_case='upper'
        )
