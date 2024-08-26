"""This module encapsulates the apache beam integrated pipeline."""
import json
import logging
from pathlib import Path


import apache_beam as beam
from apache_beam import PTransform
from apache_beam.options.pipeline_options import PipelineOptions

from pipe_gaps import queries
from pipe_gaps.pipeline import base
from pipe_gaps.pipeline.schemas import Message, Gap
from pipe_gaps.pipeline.common import DetectGaps
from pipe_gaps.pipeline.beam.transforms import ReadFromJson, ReadFromQuery, WriteJson, Core

logger = logging.getLogger(__name__)


class BeamPipeline(base.Pipeline):
    """Beam integrated pipeline.

    Args:
        sources: list of sources transforms to read pipeline inputs.
        core: the core transform.
        sinks: list of sinks transforms to write pipeline outputs.
        **options: extra arguments for PipelineOptions.

    This pipeline will:
        1. Apply the list of sources transforms and merge results into a single input p-collection.
        2. Apply the core transform o the p-collection obtained in 1.
        3. Apply the list of sinks transforms to save the outputs obtained in 2.
    """

    name = "beam"

    def __init__(
        self,
        sources: list[PTransform],
        core: PTransform,
        sinks: list[PTransform],
        side_inputs: PTransform = None,
        output_path: Path = None,
        **options
    ):
        self._sources = sources
        self._core = core
        self._sinks = sinks
        self._output_path = output_path
        self._side_inputs = side_inputs

        beam_options = self.default_options()
        beam_options.update(**options)

        self._options = PipelineOptions(flags=[], **beam_options)

    def run(self):
        with beam.Pipeline(options=self._options) as p:
            inputs = [p | s for s in self._sources]

            if self._side_inputs is not None:
                side_inputs = p | "ReadSideInputs" >> self._side_inputs
                self._core.set_side_inputs(side_inputs)

            if len(inputs) > 1:
                inputs = inputs | "JoinSources" >> beam.Flatten()
            else:
                inputs = inputs[0]

            outputs = inputs | self._core

            for sink_transform in self._sinks:
                outputs | sink_transform

            if logger.level == logging.DEBUG:
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
            worker_machine_type="e2-standard-2",  # 2 cores - 8GB
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
    def _build(cls, config: base.PipelineConfig):
        # This is the only method of the class that uses concrete implementations for Gaps.
        # AISMessagesQuery, DetectGapsFn, Message, output_prefix
        # The rest of the class is generic.
        # TODO: Use factories to make this method also generic and define transforms by config.

        sources = []
        if config.input_file is not None:
            input_id = config.input_file.stem
            sources.append(ReadFromJson(config.input_file, schema=Message))
        else:
            input_id = "from-query"
            sources.append(
                ReadFromQuery(
                    query=queries.AISMessagesQuery(**config.input_query).render(),
                    schema=Message,
                    mock_db_client=config.mock_db_client,
                    use_standard_sql=True
                )
            )

        side_inputs = None
        if config.side_input_file is not None:
            side_inputs = ReadFromJson(config.side_input_file, schema=Gap, lines=True)

        core = Core(core_process=DetectGaps.build(**config.core))

        sinks = []
        output_path = None
        if config.save_json:
            output_prefix = f"{cls.name}-gaps-{input_id}"
            write_json_sink = WriteJson(config.work_dir, output_prefix=output_prefix)
            output_path = write_json_sink.path
            sinks.append(write_json_sink)

        return cls(
            sources,
            core,
            sinks,
            side_inputs=side_inputs,
            output_path=output_path,
            **config.options
        )
