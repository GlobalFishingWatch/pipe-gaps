from datetime import timedelta, datetime

from pipe_gaps import pipeline
from pipe_gaps.utils import setup_logger

setup_logger()

config = dict(
    input_query=dict(
        source_messages="pipe_ais_v3_published.messages",
        source_segments="pipe_ais_v3_published.segs_activity",
        start_date=datetime(2019, 2, 1).date(),
        end_date=datetime(2019, 2, 2).date(),
        ssvids=[412331104, 477334300],
    ),
    core=dict(
        threshold=timedelta(hours=0.1),
        show_progress=True,
    ),
    save_json=True
)

pipe = pipeline.create(pipe_type="beam", **config)
pipe.run()
