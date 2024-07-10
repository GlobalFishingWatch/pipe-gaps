from pipe_gaps.core import gap_detector as gd
from datetime import timedelta

messages = [{
    "ssvid": "226013750",
    "msgid": "295fa26f-cee9-1d86-8d28-d5ed96c32835",
    "timestamp": "2024-01-04 20:48:40.000000 UTC",
}]

gaps = gd.detect(messages, threshold=timedelta(hours=1, minutes=20), show_progress=True)
