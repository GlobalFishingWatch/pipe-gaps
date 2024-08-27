from pathlib import Path
from dataclasses import field
from pydantic import BaseModel, ConfigDict


DEFAULT_WORK_DIR = "workdir"


class PipeConfigError(Exception):
    pass


class PipelineConfig(BaseModel):
    """Encapsulates Pipeline configuration.

    Args:
        inputs: List of main inputs to process.
        side_inputs: List of side inputs to use.
        work_dir: Working directory to use for saving outputs.
        save_json: If True, saves the results in JSON file.
        save_stats: If True, computes and saves basic statistics.
        core: Extra arguments for the core process.
        options: Extra arguments for the pipeline.
    """
    model_config = ConfigDict(extra='forbid')

    inputs: list[dict] = field(default_factory=list)
    side_inputs: list[dict] = field(default_factory=list)
    work_dir: Path = Path(DEFAULT_WORK_DIR)
    save_json: bool = False
    save_stats: bool = False
    core: dict = field(default_factory=dict)
    options: dict = field(default_factory=dict)

    def to_json(self):
        return self.model_dump_json(indent=4)

    def validate(self):
        if not self.inputs:
            raise PipeConfigError("You need to provide inputs for the pipeline!")

        if self.side_inputs is not None and len(self.side_inputs) > 1:
            raise PipeConfigError("More than 1 side input is not currently supported.")
