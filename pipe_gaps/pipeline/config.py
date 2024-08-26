from pathlib import Path
from dataclasses import field
from typing import Optional
from pydantic import BaseModel, ConfigDict


DEFAULT_WORK_DIR = "workdir"


class PipeConfigError(Exception):
    pass


class PipelineConfig(BaseModel):
    """Encapsulates Pipeline configuration.

    Args:
        input_file: Input file to process.
        input_query: Input query parameters. Ignored if input_file exists.
        mock_db_client: If True, mocks the DB client. Useful for development and testing.
        work_dir: Working directory to use for saving outputs.
        save_json: If True, saves the results in JSON file.
        save_stats: If True, computes and saves basic statistics.
        core: Extra arguments for the core process.
        options: Extra arguments for the pipeline.
    """
    model_config = ConfigDict(extra='forbid')

    input_file: Optional[Path] = None
    side_input_file: Optional[Path] = None
    input_query: Optional[dict] = None
    mock_db_client: bool = False
    work_dir: Path = Path(DEFAULT_WORK_DIR)
    save_json: bool = False
    save_stats: bool = False
    core: dict = field(default_factory=dict)
    options: dict = field(default_factory=dict)

    def to_json(self):
        return self.model_dump_json(indent=4)

    def validate(self):
        if self.input_file is None and self.input_query is None:
            raise PipeConfigError("You need to provide input_file OR query parameters.")

        if self.input_file is None:
            self.validate_query_params(**self.input_query)

    @staticmethod
    def validate_query_params(start_date=None, end_date=None, **kwargs):
        if start_date is None or end_date is None:
            raise PipeConfigError("You need to provide both start_date and end_date parameters.")
