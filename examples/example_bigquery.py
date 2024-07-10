from datetime import datetime, timedelta

from pipe_gaps import pipe
from pipe_gaps.utils import setup_logger

setup_logger()

query_params = {
    "start_date": datetime(2024, 1, 1).date(),
    "end_date": datetime(2024, 1, 5).date(),
    "ssvids": ["243042594"]
}

gaps_by_ssvid = pipe.run(
    query_params=query_params,
    show_progress=True,
    threshold=timedelta(hours=0, minutes=5)
)
