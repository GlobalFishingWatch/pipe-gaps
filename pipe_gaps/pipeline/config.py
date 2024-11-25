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
        core: Extra arguments for the core process.
        side_inputs: List of side inputs to use.
        outputs: List of outputs.
        save_stats: If True, computes and saves basic statistics.
        work_dir: Working directory to use for saving outputs.
        options: Extra arguments for the pipeline.
    """
    model_config = ConfigDict(extra='forbid')

    inputs: list[dict] = field(default_factory=list)
    side_inputs: list[dict] = field(default_factory=list)
    core: dict = field(default_factory=dict)
    outputs: list[dict] = field(default_factory=list)
    save_stats: bool = False
    work_dir: Path = Path(DEFAULT_WORK_DIR)
    options: dict = field(default_factory=dict)

    def to_json(self):
        return self.model_dump_json(indent=4)

    def validate(self):
        if len(self.inputs) == 0:
            raise PipeConfigError("You need to provide inputs for the pipeline!")

        if len(self.side_inputs) > 1:
            raise PipeConfigError("More than 1 side input is not currently supported.")

        if len(self.core) == 0:
            raise PipeConfigError("You must configure a core process for the pipeline.")
