"""This module encapsulates the apache beam integrated pipeline."""
import json
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam import PTransform

from pipe_gaps import queries
from pipe_gaps.pipeline import base
from pipe_gaps.pipeline.schemas import Message
from pipe_gaps.pipeline.beam.fns import DetectGapsFn
from pipe_gaps.pipeline.beam.transforms import ReadFromJson, ReadFromQuery, WriteJson, Core

logger = logging.getLogger(__name__)


class BeamPipeline(base.Pipeline):
    """Beam integrated pipeline.

    Args:
        sources: list of read transforms.
        core_transform: the core transform.
        sinks: list of sinks transforms.
        **options: extra arguments for PipelineOptions.

    This pipeline will:
        1. Apply the list of sources transforms and merge results into a single input p-collection.
        2. Apply the core transform o the p-collection obtained in 1.
        3. Apply the list of sinks transforms to save the outputs obtained in 2.
    """

    name = "beam"

    def __init__(
        self, sources: list[PTransform], core: PTransform, sinks: list[PTransform], **options
    ):
        self._sources = sources
        self._core = core
        self._sinks = sinks

        beam_options = self.default_options()
        beam_options.update(**options)

        self._options = PipelineOptions(flags=[], **beam_options)

    def run(self):
        with beam.Pipeline(options=self._options) as p:
            inputs = [p | s for s in self._sources] | beam.Flatten()

            outputs = inputs | self._core

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
            temp_location="gs://pipe-temp-us-central-ttl7/dataflow_temp",
            staging_location="gs://pipe-temp-us-central-ttl7/dataflow_staging",
            region="us-central1",
            network="gfw-internal-network",
            subnetwork="regions/us-central1/subnetworks/gfw-internal-us-central1",
            # experiments=["use_runner_v2"],
            setup_file="./setup.py",
        )

    @classmethod
    def _build(cls, config: base.Config):
        # This is the only method of the class that uses concrete implementations for Gaps.
        # AISMessagesQuery, DetectGapsFn, Message, output_prefix
        # The rest of the class is generic.
        # TODO: put this in a concrete subclass GapsBeamPipeline.

        sources = []
        if config.input_file is not None:
            input_id = config.input_file.stem
            sources.append(ReadFromJson(config.input_file, schema=Message))
        else:
            input_id = "from-query"
            query = queries.AISMessagesQuery.render_query(**config.query_params)

            sources.append(
                ReadFromQuery(
                    query=query,
                    schema=Message,
                    mock_db_client=config.mock_db_client,
                    use_standard_sql=True,
                    # gcs_location="gs://pipe-temp-us-central-ttl7/dataflow_temp",
                )
            )

        core = Core(core_fn=DetectGapsFn(**config.core))

        sinks = []
        if config.save_json:
            output_prefix = f"{cls.name}-gaps-{input_id}"
            sinks.append(WriteJson(config.work_dir, output_prefix=output_prefix))

        return cls(sources, core, sinks, **config.options)
