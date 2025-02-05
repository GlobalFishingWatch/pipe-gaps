"""This module encapsulates the apache beam integrated pipeline."""
import json
import logging

import googlecloudprofiler

import apache_beam as beam
from apache_beam import PTransform
from apache_beam.options.pipeline_options import PipelineOptions

from pipe_gaps.pipeline import base
from pipe_gaps.pipeline.processes import processes_factory
from pipe_gaps.pipeline.beam.transforms import sources_factory, sinks_factory, Core
from pipe_gaps.version import __version__

logger = logging.getLogger(__name__)

DATAFLOW_SDK_CONTAINER_IMAGE = "sdk_container_image"
DATAFLOW_SERVICE_OPTIONS = "dataflow_service_options"
DATAFLOW_ENABLE_PROFILER = "enable_google_cloud_profiler"


class BeamPipeline(base.Pipeline):
    """Beam integrated pipeline.

    Args:
        sources: list of p-transforms to read main inputs.
        core: the core p-transform.
        sinks: list of transforms to write outputs.
        side_inputs: list of p-transforms to read side inputs.
        **options: extra arguments for PipelineOptions.

    This pipeline will:
        1. Apply sources p-transforms and merge results into a single input p-collection.
        2. Apply side inputs p-transforms and inject the results into the core p-transform.
        3. Apply core p-transform to the p-collection obtained in 1.
        4. Apply sinks p-transforms to save the outputs obtained in 3.
    """

    name = "beam"

    def __init__(
        self,
        sources: list[PTransform],
        core: PTransform,
        sinks: list[PTransform],
        side_inputs: list[PTransform] = None,
        **options
    ):
        self._sources = sources
        self._core = core
        self._sinks = sinks
        self._side_inputs = side_inputs
        self._output_path = self._resolve_output_path()

        self._options = PipelineOptions.from_dictionary(self._resolve_beam_options(options))

    def run(self):
        if self._is_profiler_enabled():
            logger.info("Stating Google Cloud Profiler...")
            self._start_profiler()

        with beam.Pipeline(options=self._options) as p:
            if self._side_inputs is not None and len(self._side_inputs) > 0:
                side_inputs = p | "ReadSideInputs" >> self._side_inputs[0]
                self._core.set_side_inputs(side_inputs)

            inputs = [p | f"ReadInputs{i}" >> s for i, s in enumerate(self._sources, 1)]

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

        if self._output_path is not None:
            logger.info("Output JSON saved in {}".format(self._output_path.resolve()))

    def _debug_n_elements(self, elements, n=1, message=""):
        def debug(elem):
            for e in elem:
                logger.debug(f"{message}: {json.dumps(e, indent=4)}")

        elements | message >> (
            beam.combiners.Sample.FixedSizeGlobally(n).without_defaults() | beam.Map(debug))
        #  Defaults are not supported if you are not using a Global Window

    @classmethod
    def _build(cls, config: base.PipelineConfig):
        sources = [sources_factory(**p) for p in config.inputs]
        side_inputs = [sources_factory(**p) for p in config.side_inputs]

        core = Core(core_process=processes_factory(**config.core))
        sinks = [sinks_factory(**p) for p in config.outputs]

        return cls(
            sources,
            core,
            sinks,
            side_inputs=side_inputs,
            **config.options
        )

    def _resolve_output_path(self):
        # Maybe we could change the list of outputs for a dict of outputs,
        # and that way we can access the JSON output directly with a unique key.
        path = None
        for s in self._sinks:
            path = getattr(s, 'path', None)

        return path

    def _resolve_beam_options(self, options):
        args_list = options.pop("unparsed", [])

        # We let apache beam parse its options: they have the CLI argparse definition.
        # They can cast properly each value.
        unparsed_options = {}
        if len(args_list) > 0:
            unparsed_options = PipelineOptions(args_list).get_all_options(drop_default=True)

        logger.info("Options parsed by beam: {}".format(unparsed_options))

        options.update(unparsed_options)  # CLI args takes precedence.

        beam_options = self.default_options()
        beam_options.update(**options)

        if DATAFLOW_SDK_CONTAINER_IMAGE not in beam_options:
            beam_options["setup_file"] = "./setup.py"

        return beam_options

    def _is_profiler_enabled(self):
        if DATAFLOW_ENABLE_PROFILER in self._options.display_data().get(
            DATAFLOW_SERVICE_OPTIONS, []
        ):
            return True

        return False

    def _start_profiler(self):
        try:
            googlecloudprofiler.start(
                service="pipe-gaps",
                service_version=__version__,
                # verbose is the logging level. 0-error, 1-warning, 2-info,
                # 3-debug. It defaults to 0 (error) if not set.
                verbose=3,
            )
        except (ValueError, NotImplementedError) as e:
            logger.warning(f"Profiler couldn't start: {e}")

    @staticmethod
    def default_options():
        return dict(
            runner="DirectRunner",
            max_num_workers=100,
            machine_type="e2-standard-2",  # 2 cores - 8GB RAM.
            disk_size_gb=25,
            use_public_ips=False,
            project="world-fishing-827",
            temp_location="gs://pipe-temp-us-east-ttl7/dataflow_temp",
            staging_location="gs://pipe-temp-us-east-ttl7/dataflow_staging",
            region="us-east1",
            network="gfw-internal-network",
            subnetwork="regions/us-east1/subnetworks/gfw-internal-us-east1"
        )
