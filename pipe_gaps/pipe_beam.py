"""This module encapsulates the apache beam integrated pipeline."""
import json
import logging
import typing

from typing import Any

from datetime import datetime

import apache_beam as beam

from apache_beam.io.fileio import default_file_naming
from apache_beam.options.pipeline_options import PipelineOptions

from pipe_gaps.core import gap_detector as gd
from pipe_gaps.utils import json_load  # noqa
from pipe_gaps import constants as ct
from pipe_gaps import queries


logger = logging.getLogger(__name__)


class Message(typing.NamedTuple):
    """Schema for input messages."""

    ssvid: str
    seg_id: str
    msgid: str
    timestamp: float


class Gap(typing.NamedTuple):
    """Schema for output gaps."""

    ssvid: str
    total: int
    gaps: list[dict[str, Message]]


class DetectGapsFn(beam.DoFn):
    def __init__(self, config):
        self.config = config

    def process(self, element) -> list[Any]:
        """Fn to wire AIS core gap detector with apache beam.

        Args:
            element: Description

        Returns:
            TYPE: Description
        """
        (ssvid, year), messages = element

        gaps = gd.detect(messages=messages, **self.config)

        logger.info("Found {} gaps. for ssvid={}, year={}".format(len(gaps), ssvid, year))
        yield {"ssvid": ssvid, "total": len(gaps), "gaps": gaps}


def run(
    input_file=None,
    work_dir=ct.WORK_DIR,
    save=True,
    mock_db_client=False,
    query_params=None,
    **kwargs,
):
    """Beam integrated gap detection pipeline."""

    runner = "DirectRunner"
    # runner = "DataflowRunner"

    options = PipelineOptions(
        flags=[],
        runner=runner,
        project="world-fishing-827",
        # temp_location="gs://pipe-temp-us-central-ttl7/dataflow_temp",
        # staging_location="gs://pipe-temp-us-central-ttl7/dataflow_staging",
        # region="us-central1",
        # max_num_workers=600,
        # worker_machine_type="custom-1-65536-ext",
        # disk_size_gb=50,
        # wait_for_job=True,
        no_use_public_ips=True,
        # network="gfw-internal-network",
        # subnetwork="regions/us-central1/subnetworks/gfw-internal-us-central1",
        # job_name="tom-test-gaps",
        # experiments=["use_runner_v2"],
    )

    def parallelization_unit(msg):
        return (msg["ssvid"], str(datetime.fromtimestamp(msg["timestamp"]).year))

    def convert_to_json(element):
        return json.dumps(element, indent=4)

    def core_fn(config):
        return DetectGapsFn(config)

    with beam.Pipeline(options=options) as p:
        if input_file is not None:
            # TODO: consider lines=True and use JSON Lines format.
            messages = (
                p
                | "ReadInputFile"
                >> beam.io.ReadFromJson(str(input_file), lines=False, convert_dates=False)
                | beam.Map(lambda x: dict(x._asdict())).with_output_types(Message)
            )
            # messages = p | beam.Create(json_load(input_file)).with_output_types(Message)
            output_stem = input_file.stem
        else:
            output_stem = "from-query"
            query = queries.AISMessagesQuery.render_query(**query_params)

            query = f"{query} AND ssvid IN ('243042594')"

            logger.info(query)

            messages = p | beam.io.ReadFromBigQuery(
                query=query,
                use_standard_sql=True,
                gcs_location="gs://pipe-temp-us-central-ttl7/dataflow_temp",
            ).with_output_types(Message)

        # messages | beam.LogElements()

        gaps = (
            messages
            | beam.GroupBy(parallelization_unit)
            | beam.ParDo(core_fn(kwargs)).with_output_types(Gap)
        )

        # gaps | beam.LogElements()

        if save:
            prefix = f"beam-gaps-{output_stem}"
            file_naming = default_file_naming(prefix=prefix, suffix=".json")
            # TODO: consider lines=True and use JSON Lines format.
            gaps | "WriteOutputFile" >> beam.io.WriteToJson(
                work_dir, file_naming=file_naming, lines=False, indent=4
            )
