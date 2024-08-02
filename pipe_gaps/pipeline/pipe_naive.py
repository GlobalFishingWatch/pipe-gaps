"""This module encapsulates the "naive" local pipeline."""
import csv
import logging
import itertools
from datetime import datetime
from dataclasses import dataclass, fields

from pipe_gaps import queries
from pipe_gaps import constants as ct
from pipe_gaps.pipeline import base
from pipe_gaps.core import gap_detector as gd
from pipe_gaps.utils import json_load, json_save

logger = logging.getLogger(__name__)


@dataclass(eq=True, frozen=True)
class ProcessingUnitKey:
    """Encapsulates the key to group by processing units."""

    ssvid: str
    year: str

    @classmethod
    def fields(cls):
        return [x.name for x in fields(cls)]

    @classmethod
    def from_dict(cls, item: dict) -> "ProcessingUnitKey":
        return cls(ssvid=item["ssvid"], year=str(datetime.fromtimestamp(item["timestamp"]).year))


class NaivePipeline(base.Pipeline):
    """Pure python pipeline without parallelization, useful for development and testing."""

    name = "naive"

    def __init__(self, config: base.Config):
        self.config = config

    @classmethod
    def _build(cls, config: base.Config):
        return cls(config)

    def run(self):
        if self.config.input_file is not None:
            logger.info("Fetching messages from file: {}".format(self.config.input_file.resolve()))
            messages = json_load(self.config.input_file)
            output_stem = self.config.input_file.stem
        else:
            logger.info("Fetching messages from database...")
            messages_query = queries.AISMessagesQuery.build(mock_client=self.config.mock_db_client)
            messages = messages_query.run(**self.config.query_params)
            output_stem = "from-query"

        if len(messages) == 0:
            raise base.NoMessagesFound("No messages found with filters provided.")

        logger.info("Total amount of input messages: {}".format(len(messages)))
        logger.info(f"Grouping messages by {ProcessingUnitKey.fields()}...")
        sorted_messages = sorted(messages, key=lambda x: (x["ssvid"], x["timestamp"]))
        grouped_messages = itertools.groupby(sorted_messages, key=ProcessingUnitKey.from_dict)

        logger.info("Detecting gaps...")
        gaps_by_key = {}
        for key, messages in grouped_messages:
            gaps = gd.detect(messages, **self.config.core)
            logger.info("Found {} gaps for key={}".format(len(gaps), key))
            gaps_by_key[key] = gaps

        total_n_gaps = sum(len(g) for g in gaps_by_key.values())
        logger.info("Total amount of gaps detected: {}".format(total_n_gaps))

        output_path = self.config.work_dir.joinpath(f"{ct.OUTPUT_PREFIX}-{output_stem}.json")
        output_path_stats = self.config.work_dir.joinpath(f"stats-{output_stem}")

        if self.config.save_json:
            json_save(list(gaps_by_key.values()), output_path)
            logger.info("Output saved in {}".format(output_path.resolve()))

        if self.config.save_stats:
            stats = []
            for k, v in gaps_by_key.items():
                stats.append({"ssvid": k.ssvid, "total": len(v)})

            with open(f"{output_path_stats}.csv", "w", newline="") as output_file:
                dict_writer = csv.DictWriter(output_file, stats[0].keys())
                dict_writer.writeheader()
                dict_writer.writerows(stats)

            # json_save(stats, f"{output_path_stats}.json")

        return output_path, gaps_by_key
