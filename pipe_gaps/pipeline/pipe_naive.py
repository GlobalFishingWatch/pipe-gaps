"""This module encapsulates the "naive" local pipeline."""
import csv
import logging
import itertools
from pathlib import Path

from pipe_gaps import queries
from pipe_gaps.utils import json_load, json_save
from pipe_gaps.bq_client import BigQueryClient
from pipe_gaps.pipeline import base
from pipe_gaps.pipeline.common import DetectGaps

logger = logging.getLogger(__name__)


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
        # TODO: split this into sources, core, and sinks operations built in _build method
        # and injected into the instance.

        # Sources
        inputs = self.config.inputs[0]

        if inputs["kind"] == "json":
            input_file = Path(inputs["input_file"])
            logger.info("Fetching messages from file: {}".format(input_file.resolve()))
            messages = json_load(inputs["input_file"])
        else:
            logger.info("Fetching messages from database...")
            client = BigQueryClient.build(mock_client=inputs["mock_db_client"])
            query = queries.get_query(inputs["query_name"], inputs["query_params"])
            messages = client.run_query(query)

        if len(messages) == 0:
            raise base.NoInputsFound("No messages found with filters provided.")

        side_inputs = None
        if len(self.config.side_inputs) > 0:
            side_inputs = self.config.side_inputs[0]
            side_inputs = json_load(side_inputs["input_file"], lines=side_inputs["lines"])

        # Core process
        core = DetectGaps.build(**self.config.core)

        outputs = core.process(messages, side_inputs=side_inputs)

        total_n_gaps = len(outputs)
        logger.info("Total amount of gaps detected: {}".format(total_n_gaps))

        # Sinks
        if self.config.save_json:
            self._output_path = self.config.work_dir.joinpath(f"{self.name}-gaps.json")
            json_save(outputs, self._output_path, lines=True)
            logger.info("Output saved in {}".format(self._output_path.resolve()))

        if self.config.save_stats:
            self._output_path_stats = self.config.work_dir.joinpath(f"{self.name}-stats.csv")
            gaps_by_ssvid = itertools.groupby(outputs, key=lambda x: x["OFF"]["ssvid"])

            stats = []
            for ssvid, gaps in gaps_by_ssvid:
                gaps = list(gaps)
                stats.append({"ssvid": ssvid, "total": len(gaps)})

            with open(self._output_path_stats, "w", newline="") as output_file:
                dict_writer = csv.DictWriter(output_file, ["ssvid", "total"])
                dict_writer.writeheader()
                dict_writer.writerows(stats)

            logger.info("Stats saved in {}".format(self._output_path_stats.resolve()))
