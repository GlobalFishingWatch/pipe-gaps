import apache_beam as beam


class Core(beam.PTransform):
    def __init__(self, core_fn: beam.DoFn):
        self._core_fn = core_fn

    def expand(self, pcoll):
        return (
            pcoll
            | beam.GroupBy(self._core_fn.parallelization_unit)
            | beam.ParDo(self._core_fn)
            | beam.FlatMapTuple(lambda k, v: v).with_output_types(self._core_fn.type())
        )
