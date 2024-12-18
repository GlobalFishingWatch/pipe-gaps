"""This module encapsulates the "naive" local pipeline."""
import csv
import logging
import itertools
from pathlib import Path

from pipe_gaps import queries
from pipe_gaps.pipeline import base
from pipe_gaps.pipeline.processes import processes_factory
from pipe_gaps.utils import json_load, json_save
from pipe_gaps.bq_client import BigQueryClient

logger = logging.getLogger(__name__)


def fetch_inputs(name, config):
    if config["kind"] == "json":
        input_file = Path(config["input_file"])
        logger.info("Fetching {} inputs from file: {}".format(name, input_file.resolve()))
        return json_load(config["input_file"], config.get("lines", False))
    else:
        logger.info("Fetching {} inputs from database...".format(name))
        client = BigQueryClient.build(mock_client=config["mock_db_client"])
        query = queries.get_query(config["query_name"], config["query_params"])
        return client.run_query(query)


class NaivePipeline(base.Pipeline):
    """Pure python pipeline without parallelization, useful for development and testing."""

    name = "naive"

    def __init__(self, config: base.PipelineConfig):
        self.config = config
        self._output_path = None
        self._output_path_stats = None

    @classmethod
    def _build(cls, config: base.PipelineConfig):
        # TODO: Use factories to make this method also generic and define transforms by config.
        return cls(config)

    def run(self):
        # TODO: Split this into sources, core, and sinks operations.
        # TODO: Inject them through the constructor.

        # Sources
        inputs = fetch_inputs("main", self.config.inputs[0])

        if len(inputs) == 0:
            raise base.NoInputsFound("No inputs found with filters provided.")

        side_inputs = None
        if len(self.config.side_inputs) > 0:
            side_inputs = fetch_inputs("side", self.config.side_inputs[0])

        # Core process
        core = processes_factory(**self.config.core)
        outputs = core.process(inputs, side_inputs=side_inputs)

        total_n_gaps = len(outputs)
        logger.info("Total amount of gaps detected: {}".format(total_n_gaps))

        # Sinks
        for output_config in self.config.outputs:
            self._save_outputs(outputs, output_config)

        if self.config.save_stats:
            self._save_stats(outputs)

    def _save_outputs(self, outputs, config):
        if config["kind"] == "json":
            output_prefix = config["output_prefix"]
            self._output_path = self.config.work_dir.joinpath(f"{self.name}-{output_prefix}.json")
            json_save(outputs, self._output_path, lines=True)
            logger.info("Output saved in {}".format(self._output_path.resolve()))
            return

        logger.warning(
            "Ignoring output of type '{}' not impemented for naive pipeline."
            .format(config["kind"])
        )

    def _save_stats(self, outputs):
        self._output_path_stats = self.config.work_dir.joinpath(f"{self.name}-stats.csv")
        gaps_by_ssvid = itertools.groupby(outputs, key=lambda x: x["ssvid"])

        stats = []
        for ssvid, gaps in gaps_by_ssvid:
            gaps = list(gaps)
            stats.append({"ssvid": ssvid, "total": len(gaps)})

        with open(self._output_path_stats, "w", newline="") as output_file:
            dict_writer = csv.DictWriter(output_file, ["ssvid", "total"])
            dict_writer.writeheader()
            dict_writer.writerows(stats)

        logger.info("Stats saved in {}".format(self._output_path_stats.resolve()))
