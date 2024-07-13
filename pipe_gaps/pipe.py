"""This module encapsulates the "naive" local pipeline."""
import os
import logging
import warnings

from pathlib import Path

from pipe_gaps import constants as ct
from pipe_gaps import pipe_naive

try:
    import apache_beam  # noqa
except ImportError:
    warnings.warn(
        "Apache Beam not found. Install pipe-gaps[beam] to enable beam integration.", stacklevel=1
    )
    is_beam_imported = False
else:
    from pipe_gaps import pipe_beam

    is_beam_imported = True

logger = logging.getLogger(__name__)

OUTPUT_PREFIX = "gaps"


class ApacheBeamNotInstalled(Exception):
    pass


def _validate_query_params(start_date=None, end_date=None, ssvids=None):
    if start_date is None or end_date is None:
        raise ValueError("You need to provide both start-date and end-date parameters.")


def run(
    pipe_type: str = "naive",
    input_file=None,
    query_params: dict = None,
    work_dir: str = ct.WORK_DIR,
    save: bool = False,
    **kwargs,
) -> dict[str, list]:
    """Runs AIS gap detector.

    Args:
        pipe_type: pipeline type to use.
        input_file: input file to process.
        query_params: query parameters. Ignored if input_file exists.
        work_dir: working directory to use.
        save: if True, saves the results in JSON file.
        **kwargs: extra arguments for the core gap detector.
    """

    if input_file is None and not query_params:
        raise ValueError("You need to provide input_file OR query parameters.")

    if input_file is None and query_params is not None:
        _validate_query_params(**query_params)

    work_dir = Path(work_dir)

    logger.info("Creating working directory {}...".format(work_dir.resolve()))
    os.makedirs(work_dir, exist_ok=True)

    if pipe_type == "naive":
        pipe_naive.run(
            input_file=input_file,
            query_params=query_params,
            work_dir=work_dir,
            save=save,
            **kwargs,
        )

    if pipe_type == "beam":
        if not is_beam_imported:
            raise ApacheBeamNotInstalled(
                "Must install package with pipe-gaps[beam] to use beam integration."
            )

        return pipe_beam.run(
            input_file=input_file,
            query_params=query_params,
            work_dir=work_dir,
            save=save,
            **kwargs,
        )
