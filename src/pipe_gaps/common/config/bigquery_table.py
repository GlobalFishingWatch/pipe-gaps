import json
from functools import cached_property

from typing import Tuple, Optional
from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass
class BigQueryTableConfig(ABC):
    table_id: str
    schema_file: str
    write_disposition: str = "WRITE_APPEND"
    project: Optional[str] = None
    view_suffix: Optional[str] = "view"
    partition_type: str = "DAY"
    partition_field: Optional[str] = None
    clustering_fields: Optional[Tuple] = None
    description_template: str = "𝗥𝗲𝗹𝗲𝘃𝗮𝗻𝘁 𝗽𝗮𝗿𝗮𝗺𝗲𝘁𝗲𝗿𝘀:\n{params_json}"

    @cached_property
    def view_id(self):
        """Returns the ID of the view for the table."""
        return f"{self.table_id}_{self.view_suffix}"

    @abstractmethod
    @cached_property
    def schema(self):
        """Returns the schema of the table."""

    def to_dict(
        self, version: str, description_params: dict, description_enabled: bool = True
    ) -> dict:
        """Returns a dictionary that can be passed as bigquery parameters."""
        bigquery_params = dict(
            table=self.table_id,
            schema=self.schema,
            project=self.project,
            partition_type=self.partition_type,
            partition_field=self.partition_field,
            clustering_fields=self.clustering_fields,
            write_disposition=self.write_disposition,
        )

        if description_enabled:
            bigquery_params["description"] = self.description_template.format(
                version=version,
                **description_params,
                params_json=json.dumps(description_params, indent=4)
            )

        return bigquery_params
