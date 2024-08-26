"""This module encapsulates the "naive" local pipeline."""
import csv
import logging
import itertools

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
        return cls(config)

    def run(self):
        # TODO: split this into sources, core, and sinks operations.
        # TODO: Generalize this method.

        # Sources
        if self.config.input_file is not None:
            logger.info("Fetching messages from file: {}".format(self.config.input_file.resolve()))
            messages = json_load(self.config.input_file)
            input_id = self.config.input_file.stem
        else:
            logger.info("Fetching messages from database...")
            client = BigQueryClient.build(mock_client=self.config.mock_db_client)
            messages = client.run_query(queries.AISMessagesQuery(**self.config.input_query))
            input_id = "from-query"

        if len(messages) == 0:
            raise base.NoInputsFound("No messages found with filters provided.")

        # Core process
        core = DetectGaps.build(**self.config.core)

        side_inputs = None
        if self.config.side_input_file is not None:
            side_inputs = json_load(self.config.side_input_file, lines=True)

        outputs = core.process(messages, side_inputs=side_inputs)

        total_n_gaps = len(outputs)
        logger.info("Total amount of gaps detected: {}".format(total_n_gaps))

        # Sinks
        if self.config.save_json:
            self._output_path = self.config.work_dir.joinpath(f"{self.name}-gaps-{input_id}.json")
            json_save(outputs, self._output_path, lines=True)
            logger.info("Output saved in {}".format(self._output_path.resolve()))

        if self.config.save_stats:
            self._output_path_stats = self.config.work_dir.joinpath(f"stats-{input_id}.csv")
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
