"""This module encapsulates the apache beam integrated pipeline."""
import json
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from pipe_gaps import queries
from pipe_gaps import constants as ct
from pipe_gaps.pipeline import base
from pipe_gaps.pipeline.schemas import Message
from pipe_gaps.pipeline.beam.transforms import ReadFromJson, ReadFromQuery, WriteJson, Core
from pipe_gaps.pipeline.beam.fns import DetectGapsFn

logger = logging.getLogger(__name__)


class BeamPipeline(base.Pipeline):
    """Beam integrated pipeline."""

    name = "beam"

    def __init__(self, read_inputs, core_transform, sinks, **options):
        self._read_inputs = read_inputs
        self._core_transform = core_transform
        self._sinks = sinks

        beam_options = self.default_options()
        beam_options.update(**options)

        self._options = PipelineOptions(flags=[], **beam_options)

    @classmethod
    def _build(cls, config: base.Config):
        # This is the only method of the class that has concrete class implementations for Gaps.
        # Like AISMessagesQuery, DetectGapsFn, Message.
        # The rest of the class is generic.

        if config.input_file is not None:
            input_id = config.input_file.stem
            read_inputs = ReadFromJson(config.input_file, schema=Message)
        else:
            input_id = "from-query"
            query = queries.AISMessagesQuery.render_query(**config.query_params)

            read_inputs = ReadFromQuery(
                mock_db_client=config.mock_db_client,
                query=query,
                schema=Message,
                use_standard_sql=True,
                gcs_location="gs://pipe-temp-us-central-ttl7/dataflow_temp",
            )

        core = Core(core_fn=DetectGapsFn(**config.core))

        sinks = []

        if config.save_json:
            output_prefix = f"{cls.name}-{ct.OUTPUT_PREFIX}-{input_id}"
            sinks.append(WriteJson(config.work_dir, output_prefix=output_prefix))

        return cls(read_inputs, core, sinks, **config.options)

    def run(self):
        with beam.Pipeline(options=self._options) as p:
            inputs = p | self._read_inputs

            outputs = inputs | self._core_transform

            for sink_transform in self._sinks:
                outputs | sink_transform

            self._debug_n_elements(inputs, n=1, message="Sample Input")
            self._debug_n_elements(outputs, n=1, message="Sample Output")

    def _debug_n_elements(self, elements, n=1, message=""):
        def debug(elem):
            for e in elem:
                logger.debug(f"{message}: {json.dumps(e, indent=4)}")

        elements | message >> (beam.combiners.Sample.FixedSizeGlobally(n) | beam.Map(debug))

    @staticmethod
    def default_options():
        return dict(
            runner="DirectRunner",
            max_num_workers=100,
            worker_machine_type="e2-standard-2",
            disk_size_gb=25,
            no_use_public_ips=True,
            job_name="tom-test-gaps",
            project="world-fishing-827",
            # temp_location="gs://pipe-temp-us-central-ttl7/dataflow_temp",
            staging_location="gs://pipe-temp-us-central-ttl7/dataflow_staging",
            region="us-central1",
            network="gfw-internal-network",
            subnetwork="regions/us-central1/subnetworks/gfw-internal-us-central1",
            # experiments=["use_runner_v2"],
            setup_file="./setup.py",
        )
