"""This module encapsulates the "naive" local pipeline."""
import logging

from pipe_gaps.core import gap_detector as gd
from pipe_gaps.utils import json_load, json_save
from pipe_gaps import queries
from pipe_gaps import constants as ct

logger = logging.getLogger(__name__)


class NoMessagesFound(Exception):
    pass


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
    logger.info("Grouping messages by SSVID...")
    msgs_by_ssvid = {}
    for msg in messages:
        msgs_by_ssvid.setdefault(msg["ssvid"], []).append(msg)

    gaps_by_ssvid = {}
    for ssvid, messages in msgs_by_ssvid.items():
        logger.info("SSVID: {}. Amount of messages: {}".format(ssvid, len(messages)))
        logger.info("Detecting gaps...")
        gaps = gd.detect(messages, **core_config)

        logger.info("SSVID: {}. Amount of gaps detected: {}".format(ssvid, len(gaps)))
        gaps_by_ssvid[ssvid] = gaps

    total_n_gaps = sum(len(g) for g in gaps_by_ssvid.values())
    logger.info("Total amount of gaps detected: {}".format(total_n_gaps))

    if save_json:
        output_path = work_dir.joinpath(f"{ct.OUTPUT_PREFIX}-{output_stem}.json")
        json_save(gaps_by_ssvid, output_path)
        logger.info("Output saved in {}".format(output_path.resolve()))

    return gaps_by_ssvid
