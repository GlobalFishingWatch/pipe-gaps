"""This module allows to perform simple benchmarks."""
import sys
import logging
from random import shuffle
from datetime import timedelta

from pipe_gaps.utils import timing
from pipe_gaps.log import setup_logger
from pipe_gaps.core import gap_detector as gd

from tests import conftest

logger = logging.getLogger()


def build_input_messages(n: int = 1000) -> list[dict]:
    """Constructs a list of n AIS position messages."""
    logger.info("Constructing messages...")
    test_messages = conftest.get_messages().copy()
    q, mod = divmod(n, len(test_messages))
    messages = test_messages * q + test_messages[:mod]

    logger.info("Shuffling messages...")
    shuffle(messages)

    return messages


@timing
def bench_core_gap_detector(messages: list[dict]) -> None:
    """Benchmark for core gap detector."""
    gd.detect(messages, threshold=timedelta(hours=1, minutes=20))


def main(n: int = 1000) -> None:
    """Runs benchmarks with n position messages."""
    setup_logger()

    if isinstance(n, str):
        n = int(float(n))

    messages = build_input_messages(n)
    bench_core_gap_detector(messages)


if __name__ == '__main__':
    main(*sys.argv[1:])
