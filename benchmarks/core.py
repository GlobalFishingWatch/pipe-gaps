"""This module allows to perform simple benchmarks."""
import os
import sys
import math
import json
import pathlib
import logging
import argparse
import statistics
from dataclasses import dataclass

from random import shuffle
from datetime import timedelta

from pipe_gaps.utils import timing
from pipe_gaps.log import setup_logger
from pipe_gaps.core import gap_detector as gd

from tests import conftest

setup_logger()
logger = logging.getLogger('Benchmark')

NAME = "bench"
DESCRIPTION = "Run benchmarks for core algorithm."

HELP_RUN = "runs benchmark."
HELP_STATS = "recomputes stats with an existing measurement file."

HELP_SIZE = "size of the input."
HELP_REPS = "number of repetitions."
HELP_FILE = "filepath to measurements file."

STATS_FILENAME = "measurement-size-{size}-reps-{reps}.json"
BENCHMARKS_DATA_DIR = "benchmarks/data"


@dataclass
class Stats:
    mean: float
    std: float
    median: float
    minimum: float


class Measurement:
    def __init__(self, measurements: list[float], input_size: int, path: str = None):
        self.measurements = measurements
        self.input_size = input_size
        self.reps = len(self.measurements)

        self.path = path
        if path is None:
            self.path = pathlib.Path(BENCHMARKS_DATA_DIR).joinpath(
                STATS_FILENAME.format(size=self.input_size, reps=self.reps))

        self.stats = self.compute_stats()

    @classmethod
    def from_path(cls, path):
        with open(path, 'r') as f:
            measurement = json.load(f)

        return cls(
            measurements=measurement["measurements"],
            input_size=measurement["input_size"],
            path=path
        )

    def to_dict(self):
        return dict(
            measurements=self.measurements,
            statistics=self.stats.__dict__,
            input_size=self.input_size
        )

    def compute_stats(self):
        stats = Stats(
            mean=statistics.mean(self.measurements),
            std=statistics.stdev(self.measurements),
            median=statistics.median(self.measurements),
            minimum=min(self.measurements)
        )
        stats.sem = stats.std / math.sqrt(len(self.measurements))

        return stats

    def log_stats(self):
        logger.info("=========== STATISTICS ===========")
        logger.info("Minimum: {} sec".format(round(self.stats.minimum, 3)))
        logger.info("Median: {} sec".format(round(self.stats.median, 3)))
        logger.info("Mean: {} sec".format(round(self.stats.mean, 3)))
        logger.info("Standard Dev: {} sec".format(round(self.stats.std, 3)))
        logger.info("SEM: {} sec".format(round(self.stats.sem, 3)))

    def save(self):
        with open(self.path, 'w') as f:
            json.dump(self.to_dict(), f, indent=4)


def _build_input_messages(n: int = 1000) -> list[dict]:
    logger.debug("Constructing messages...")
    test_messages = conftest.get_messages().copy()
    q, mod = divmod(n, len(test_messages))
    messages = test_messages * q + test_messages[:mod]

    logger.debug("Shuffling messages...")
    shuffle(messages)

    return messages


def _core_gap_detector(messages: list[dict]) -> None:
    """Benchmark for core gap detector."""
    gd.detect(messages, threshold=timedelta(hours=1, minutes=20))


def stats(path: pathlib.Path):
    measurement = Measurement.from_path(path)
    measurement.save()


def run_benchmark(input_size: int = 1000, reps: int = 10) -> Measurement:
    """Runs benchmark and writes output with measurements and statistics.

    Args:
        input_size: amount of input messages.
        reps: number of repetitions.

    Returns:
        the measuremnt.
    """
    logger.info("Size of the input: {}".format(input_size))
    logger.info("Number of repetitions: {}".format(reps))

    logger.info("========== MEASUREMENTS ==========")
    times = []
    for rep in range(1, reps + 1):
        messages = _build_input_messages(input_size)
        _, elapsed = timing(_core_gap_detector, quiet=True)(messages)
        logger.info("Repetition {}; duration: {} sec".format(rep, round(elapsed, 3)))
        times.append(elapsed)

    measurement = Measurement(times, input_size=input_size)
    measurement.log_stats()
    measurement.save()

    return measurement


def main(args):
    """CLI for benchmark tool."""
    os.makedirs(BENCHMARKS_DATA_DIR, exist_ok=True)
    parser = argparse.ArgumentParser(prog=NAME, description=DESCRIPTION)
    subparsers = parser.add_subparsers(dest='command', help='available commands')

    p = subparsers.add_parser("run", help=HELP_RUN)
    p.add_argument('-n', '--input_size', type=float, required=True, metavar='\b', help=HELP_SIZE)
    p.add_argument('-r', '--reps', type=int, default=10, metavar='\b', help=HELP_REPS)

    p = subparsers.add_parser("stats", help=HELP_STATS)
    p.add_argument('-f', '--path', type=pathlib.Path, required=True, metavar='\b', help=HELP_SIZE)

    args = parser.parse_args(args=args or ['--help'])

    command = args.command
    del args.command

    if command == "run":
        args.input_size = int(args.input_size)
        run_benchmark(**vars(args))

    if command == "stats":
        stats(**vars(args))


if __name__ == '__main__':
    main(sys.argv[1:])
