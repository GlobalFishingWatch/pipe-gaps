"""This module encapsulates the "naive" local pipeline."""
import csv
import logging
import itertools

from datetime import datetime, timedelta

from pipe_gaps import queries
from pipe_gaps.bq_client import BigQueryClient
from pipe_gaps.pipeline import base
from pipe_gaps.core import gap_detector as gd
from pipe_gaps.utils import json_load, json_save
from pipe_gaps.pipeline.common import SsvidAndYearKey

logger = logging.getLogger(__name__)


class NaivePipeline(base.Pipeline):
    """Pure python pipeline without parallelization, useful for development and testing."""

    name = "naive"

    def __init__(self, config: base.PipelineConfig):
        self.config = config
        self._output_path = None

    @classmethod
    def _build(cls, config: base.PipelineConfig):
        return cls(config)

    def run(self):
        # TODO: split this into sources, core, and sinks operations.
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

        eval_last = self.config.core.pop("eval_last", False)

        logger.info("Total amount of input messages: {}".format(len(messages)))
        logger.info(f"Grouping messages by {SsvidAndYearKey.attributes()}...")
        sorted_messages = sorted(messages, key=lambda x: (x["ssvid"], x[gd.KEY_TIMESTAMP]))
        grouped_messages = itertools.groupby(sorted_messages, key=SsvidAndYearKey.from_dict)

        logger.info("Detecting gaps...")
        prev_key, prev_message = (None, None)
        gaps_by_key = {}
        for key, messages in grouped_messages:
            messages = list(messages)

            # Handle border cases. Add last message in next batch.
            last_message = messages[-1].copy()
            if prev_message is not None and prev_key.ssvid == key.ssvid:
                messages.append(prev_message)

            gaps = gd.detect(messages, **self.config.core)
            logger.info("Found {} gaps for key={}".format(len(gaps), key))
            gaps_by_key[key] = gaps

            prev_key, prev_message = key, last_message

        if eval_last:
            groups = itertools.groupby(sorted_messages, key=lambda x: x["ssvid"])
            last_messages = [(k, max(m, key=lambda x: x[gd.KEY_TIMESTAMP])) for k, m in groups]

            for key, last_m in last_messages:
                last_m_date = datetime.fromtimestamp(last_m[gd.KEY_TIMESTAMP]).date()
                next_m_date = last_m_date + timedelta(days=1)
                next_m_datetime = datetime.combine(next_m_date, datetime.min.time())

                next_m = {
                    gd.KEY_TIMESTAMP: next_m_datetime.timestamp(),
                    "distance_from_shore_m": 1,
                    }

                open_gaps = gd.detect([last_m, next_m], **self.config.core)
                assert len(open_gaps) <= 1, "I shouldn't find more than one open gap per vessel."

                for open_gap in open_gaps:
                    logger.info(f"Found 1 open gap for key={key}...")
                    open_gap["ON"] = None
                    gaps_by_key.setdefault(key, []).append(open_gap)

        total_n_gaps = sum(len(g) for g in gaps_by_key.values())
        logger.info("Total amount of gaps detected: {}".format(total_n_gaps))

        if self.config.save_json:
            self._output_path = self.config.work_dir.joinpath(f"{self.name}-gaps-{input_id}.json")
            json_save(list(itertools.chain(*gaps_by_key.values())), self._output_path)
            logger.info("Output saved in {}".format(self._output_path.resolve()))

        if self.config.save_stats:
            output_path_stats = self.config.work_dir.joinpath(f"stats-{input_id}")
            stats = []
            for k, v in gaps_by_key.items():
                stats.append({"ssvid": k.ssvid, "total": len(v)})

            with open(f"{output_path_stats}.csv", "w", newline="") as output_file:
                dict_writer = csv.DictWriter(output_file, stats[0].keys())
                dict_writer.writeheader()
                dict_writer.writerows(stats)

            # json_save(stats, f"{output_path_stats}.json")
