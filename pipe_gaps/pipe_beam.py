"""This module encapsulates the apache beam integrated pipeline."""
import typing
import logging

from datetime import datetime
from dataclasses import dataclass

import apache_beam as beam

from apache_beam.io.fileio import default_file_naming
from apache_beam.options.pipeline_options import PipelineOptions

from pipe_gaps.core import gap_detector as gd
from pipe_gaps.utils import json_load
from pipe_gaps import constants as ct
from pipe_gaps import queries


logger = logging.getLogger(__name__)


@dataclass(eq=True, frozen=True)
class ProcessingUnitKey:
    """Encapsulates the key to group by processing units."""

    ssvid: str
    year: str

    @classmethod
    def from_dict(cls, item: dict) -> "ProcessingUnitKey":
        return cls(
            ssvid=item["ssvid"],
            year=str(datetime.fromtimestamp(item["timestamp"]).year)
        )


class Message(typing.NamedTuple):
    """Schema for input messages."""

    ssvid: str
    seg_id: str
    msgid: str
    timestamp: float


class Gap(typing.NamedTuple):
    """Schema for gaps."""

    OFF: Message
    ON: Message


class CoreFn(beam.DoFn):
    """Fn to wire core process with apache beam."""

    def __init__(self, config=None):
        self.config = config

        if self.config is None:
            self.config = {}

    def process(self, element: tuple) -> list[tuple[ProcessingUnitKey, Gap]]:
        """Process elements of a keyed p-collection.

        Args:
            element: Processing unit (key, items).

        Returns:
            Processed items.
        """
        key, messages = element

        gaps = gd.detect(messages=messages, **self.config)
        logger.info("Found {} gaps for key={}".format(len(gaps), key))

        yield key, gaps

    @staticmethod
    def type():
        return Gap

    @staticmethod
    def parallelization_unit(item):
        return ProcessingUnitKey.from_dict(item)


def run(
    input_file=None,
    query_params=None,
    mock_db_client=False,
    work_dir=ct.WORK_DIR,
    save_json=True,
    pipe_config=None,
    core_config=None,
):
    """Beam integrated gap detection pipeline."""

    options = PipelineOptions(flags=[], **pipe_config)

    with beam.Pipeline(options=options) as p:
        if input_file is not None:
            # ReadFromJson returns BeamSchema objects, and then we need to convert to dict...
            # TODO: consider using JSON Lines format.
            # inputs = (
            #     p
            #     | "ReadInputFile"
            #     >> beam.io.ReadFromJson(str(input_file), lines=False, convert_dates=False)
            #     | beam.Map(lambda x: dict(x._asdict())).with_output_types(Message)
            # )
            inputs = p | beam.Create(json_load(input_file)).with_output_types(Message)
            output_stem = input_file.stem
        else:
            output_stem = "from-query"
            query = queries.AISMessagesQuery.render_query(**query_params)

            inputs = p | beam.io.ReadFromBigQuery(
                query=query,
                use_standard_sql=True,
                gcs_location="gs://pipe-temp-us-central-ttl7/dataflow_temp",
            ).with_output_types(Message)

        # inputs | beam.LogElements()

        outputs = (
            inputs
            | beam.GroupBy(CoreFn.parallelization_unit)
            | beam.ParDo(CoreFn(core_config))
            | beam.FlatMapTuple(lambda k, v: v).with_output_types(CoreFn.type())
        )

        # outputs | beam.LogElements()

        prefix = f"beam-{ct.OUTPUT_PREFIX}-{output_stem}"
        if save_json:
            # TODO: consider using JSON Lines format.
            file_naming = default_file_naming(prefix=prefix, suffix=".json")
            outputs | "WriteOutputJson" >> beam.io.WriteToJson(
                work_dir, file_naming=file_naming, lines=False, indent=4
            )
