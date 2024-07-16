"""This module encapsulates the "naive" local pipeline."""
import os
import logging
import warnings
from typing import Union

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
PIPELINE_TYPES = ["naive", "beam"]


class ApacheBeamNotInstalled(Exception):
    pass


def _validate_query_params(start_date=None, end_date=None, ssvids=None):
    if start_date is None or end_date is None:
        raise ValueError("You need to provide both start-date and end-date parameters.")


def run(
    pipe_type: str = "naive",
    input_file: Union[str, Path] = None,
    query_params: dict = None,
    mock_db_client: bool = False,
    work_dir: str = ct.WORK_DIR,
    save_json: bool = False,
    pipe_config: bool = None,
    **core_config,
) -> dict[str, list]:
    """Runs AIS gap detector.

    Args:
        pipe_type: pipeline type to use.
        input_file: input file to process.
        query_params: query parameters. Ignored if input_file exists.
        mock_db_client: If True, mocks the DB client. Useful for development and testing.
        work_dir: working directory to use.
        save_json: if True, saves the results in JSON file.
        **core_config: extra arguments for the core gap detector.
    """
    if pipe_type not in PIPELINE_TYPES:
        raise ValueError("Pipeline type {} not implemented".format(pipe_type))

    if input_file is None and not query_params:
        raise ValueError("You need to provide input_file OR query parameters.")

    if input_file is None and query_params is not None:
        _validate_query_params(**query_params)

    if pipe_config is None:
        pipe_config = {}

    if isinstance(input_file, str):
        input_file = Path(input_file)

    work_dir = Path(work_dir)

    logger.info("Creating working directory {}...".format(work_dir.resolve()))
    os.makedirs(work_dir, exist_ok=True)

    if pipe_type == "naive":
        pipe_naive.run(
            input_file=input_file,
            query_params=query_params,
            work_dir=work_dir,
            mock_db_client=mock_db_client,
            save_json=save_json,
            core_config=core_config,
            pipe_config=pipe_config,
        )

    if pipe_type == "beam":
        if not is_beam_imported:
            raise ApacheBeamNotInstalled(
                "Must install package with pipe-gaps[beam] to use beam integration."
            )

        runner = "DirectRunner"
        # runner = "DataflowRunner"

        pipe_config.update(
            runner=runner,
            project="world-fishing-827",
            # temp_location="gs://pipe-temp-us-central-ttl7/dataflow_temp",
            staging_location="gs://pipe-temp-us-central-ttl7/dataflow_staging",
            region="us-central1",
            max_num_workers=100,
            worker_machine_type="e2-standard-2",
            disk_size_gb=25,
            no_use_public_ips=True,
            network="gfw-internal-network",
            subnetwork="regions/us-central1/subnetworks/gfw-internal-us-central1",
            job_name="tom-test-gaps",
            setup_file="./setup.py",
            # experiments=["use_runner_v2"],
        )

        return pipe_beam.run(
            input_file=input_file,
            query_params=query_params,
            work_dir=work_dir,
            save_json=save_json,
            pipe_config=pipe_config,
            core_config=core_config,
        )
