from pprint import pprint
from datetime import timedelta, datetime

from pipe_gaps.core import gap_detector as gd

messages = [
    {
        "ssvid": "226013750",
        "msgid": "295fa26f-cee9-1d86-8d28-d5ed96c32835",
        "timestamp": datetime(2024, 1, 1, 0).timestamp(),
        "distance_from_shore_m": 1
    },
    {
        "ssvid": "226013750",
        "msgid": "295fa26f-cee9-1d86-8d28-d5ed96c32835",
        "timestamp": datetime(2024, 1, 1, 1).timestamp(),
        "distance_from_shore_m": 1
    }
]

gaps = gd.detect(messages, threshold=timedelta(hours=0, minutes=50), show_progress=True)
pprint(gaps)
