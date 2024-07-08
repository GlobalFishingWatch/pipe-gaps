"""This module encapsulates the "naive" local pipeline."""
import os
import logging

from pathlib import Path

from pipe_gaps.core import gap_detector as gd
from pipe_gaps.utils import json_load, json_save
from pipe_gaps import queries
from pipe_gaps import constants as ct

logger = logging.getLogger(__name__)


class NoMessagesFound(Exception):
    pass


def _validate_query_params(start_date=None, end_date=None, ssvids=None):
    if start_date is None or end_date is None:
        raise ValueError("You need to provide both start-date and end-date parameters.")


def run(
    input_file=None,
    query_params: dict = None,
    mock_db_client: bool = False,
    work_dir: Path = ct.WORK_DIR,
    **kwargs
) -> dict[str, list]:
    """Runs AIS gap detector.

    Args:
        input_file: input file to process.
        query_params: query parameters. Ignored if input_file exists.
        mock_db_client: if true, mocks the database client.
        work_dir: working directory to use.
        **kwargs: extra arguments for the core gap detector.
    """

    if input_file is None and not query_params:
        raise ValueError("You need to provide input_file OR query parameters.")

    if query_params is not None:
        _validate_query_params(**query_params)

    logger.info("Starting pipe-gaps pipeline...")
    os.makedirs(work_dir, exist_ok=True)

    if input_file is not None:
        logger.info("Using messages from local file: {}".format(input_file.resolve()))
        messages = json_load(input_file)
    else:
        logger.info("Fetching messages from database...")
        messages_query = queries.AISMessagesQuery.build(mock_client=mock_db_client)
        messages = messages_query.run(**query_params)

    if len(messages) == 0:
        raise NoMessagesFound("No messages found with filters provided.")

    logger.info("Total amount of messages: {}".format(len(messages)))

    logger.info("Grouping messages by SSVID...")
    msgs_by_ssvid = {}
    for msg in messages:
        msgs_by_ssvid.setdefault(msg["ssvid"], []).append(msg)

    gaps_by_ssvid = {}
    for ssvid, messages in msgs_by_ssvid.items():
        logger.info("SSVID: {}. Amount of messages: {}".format(ssvid, len(messages)))
        gaps = gd.detect(messages, **kwargs)

        logger.info("SSVID: {}. Amount of gaps detected: {}".format(ssvid, len(gaps)))
        gaps_by_ssvid[ssvid] = gaps

    total_n_gaps = sum(len(g) for g in gaps_by_ssvid.values())
    logger.info("Total amount of gaps detected: {}".format(total_n_gaps))

    output_path = work_dir.joinpath("gaps.json")
    json_save(gaps_by_ssvid, output_path)
    logger.info("Output saved in {}".format(output_path.resolve()))

    return gaps_by_ssvid
