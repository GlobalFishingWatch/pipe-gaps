import json
from functools import cached_property

from typing import Tuple, Optional, Any
from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass
class TableConfig(ABC):
    table_id: str
    schema_file: str
    project: Optional[str] = None
    view_suffix: Optional[str] = "view"
    partition_type: str = "DAY"
    partition_field: Optional[str] = None
    clustering_fields: Optional[Tuple] = None
    description_template: str = ""

    def bigquery_params(self):
        return dict(
            table=self.table_id,
            schema=self.schema(),
            partition_type=self.partition_type,
            clustering_fields=self.clustering_fields,
            project=self.project,
        )

    def description(self, version: str, **params: Any) -> str:
        """Format description template with both specific keys and full params dump."""
        return self.description_template.format(
            version=version,
            **params,
            params_json=json.dumps(params, indent=4)
        )

    @cached_property
    def view_id(self):
        return f"{self.table_id}_{self.view_suffix}"

    @abstractmethod
    def schema(self):
        """Returns the schema of the table."""
