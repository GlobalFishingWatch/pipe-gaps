"""This module encapsulates the "naive" local pipeline."""
import logging
import itertools
from datetime import datetime
from dataclasses import dataclass, fields

from pipe_gaps.core import gap_detector as gd
from pipe_gaps.utils import json_load, json_save
from pipe_gaps import queries
from pipe_gaps import constants as ct

logger = logging.getLogger(__name__)


class NoMessagesFound(Exception):
    pass


@dataclass(eq=True, frozen=True)
class ProcessingUnitKey:
    """Encapsulates the key to group by processing units."""

    ssvid: str
    year: str

    def to_filename(self):
        return f"{self.ssvid}-{self.year}"

    @classmethod
    def fields(cls):
        return [x.name for x in fields(cls)]

    @classmethod
    def from_dict(cls, item: dict) -> "ProcessingUnitKey":
        return cls(
            ssvid=item["ssvid"],
            year=str(datetime.fromtimestamp(item["timestamp"]).year)
        )


def run(
    input_file=None,
    query_params: dict = None,
    mock_db_client: bool = False,
    work_dir: str = ct.WORK_DIR,
    save_json: bool = False,
    pipe_config=None,
    core_config=None,
) -> dict[str, list]:
    """Naive gap detection pipeline."""

    if input_file is not None:
        logger.info("Fetching messages from local file: {}".format(input_file.resolve()))
        messages = json_load(input_file)
        output_stem = input_file.stem
    else:
        logger.info("Fetching messages from database...")
        messages_query = queries.AISMessagesQuery.build(mock_client=mock_db_client)
        messages = messages_query.run(**query_params)
        output_stem = "from-query"

    if len(messages) == 0:
        raise NoMessagesFound("No messages found with filters provided.")

    logger.info("Total amount of input messages: {}".format(len(messages)))
    grouped_messages = itertools.groupby(messages, key=ProcessingUnitKey.from_dict)

    logger.info(f"Detecting gaps in messages grouped by {ProcessingUnitKey.fields()}...")
    gaps_by_key = {}
    for key, messages in grouped_messages:
        gaps = gd.detect(messages, **core_config)
        logger.info("Found {} gaps for key={}".format(len(gaps), key))
        gaps_by_key[key] = gaps

    total_n_gaps = sum(len(g) for g in gaps_by_key.values())
    logger.info("Total amount of gaps detected: {}".format(total_n_gaps))

    if save_json:
        output_path = work_dir.joinpath(f"{ct.OUTPUT_PREFIX}-{output_stem}.json")
        json_save(list(gaps_by_key.values()), output_path)
        logger.info("Output saved in {}".format(output_path.resolve()))

    return gaps_by_key
