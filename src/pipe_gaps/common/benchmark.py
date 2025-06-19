"""This module allows to perform simple benchmarks."""
import os
import sys
import math
import json
import logging
import argparse
import statistics

from pathlib import Path
from random import shuffle
from datetime import timedelta
from dataclasses import dataclass

import cpuinfo

from gfw.common.logging import LoggerConfig
from gfw.common.decorators import timing

from pipe_gaps.core import GapDetector
from pipe_gaps.assets import get_sample_messages


logger = logging.getLogger("Benchmark")

NAME = "bench"
DESCRIPTION = "Run benchmarks with simple statistics."

HELP_RUN = "runs benchmark."
HELP_STATS = "recomputes stats with an existing measurement file."

HELP_SIZE = "size of the input."
HELP_REPS = "number of repetitions (default: %(default)s)."
HELP_OUTPUT_DIR = "directory in which to save the outputs (default: %(default)s)."
HELP_FILE = "filepath of measurements file."

HELP_SKIP_CPU_INFO = "if true, doesn't retrieve CPU info (takes 1 sec)."

STATS_FILENAME = "measurement-size-{size}-reps-{reps}.json"
OUTPUT_DIR = "workdir/"


@dataclass
class Stats:
    mean: float
    std: float
    median: float
    minimum: float


class Measurement:
    """Encapsulates a benchmark measurement."""

    def __init__(self, measurements: list[float], input_size: int, cpu_info: str = None):
        self.measurements = measurements
        self.input_size = input_size
        self.reps = len(self.measurements)
        self.cpu_info = cpu_info

        self.stats = self.compute_stats()

    @classmethod
    def from_path(cls, path, **kwargs):
        with open(path, "r") as f:
            measurement = json.load(f)

        return cls(
            measurements=measurement["measurements"],
            input_size=measurement["input_size"],
            cpu_info=measurement["cpu_info"],
        )

    def to_dict(self):
        return dict(
            measurements=self.measurements,
            statistics=self.stats.__dict__,
            input_size=self.input_size,
            cpu_info=self.cpu_info,
        )

    def compute_stats(self):
        stats = Stats(
            mean=statistics.mean(self.measurements),
            std=statistics.stdev(self.measurements) if len(self.measurements) > 1 else 0,
            median=statistics.median(self.measurements),
            minimum=min(self.measurements),
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

    def save(self, path):
        with open(path, "w") as f:
            json.dump(self.to_dict(), f, indent=4)


# @profile  # noqa  # Uncomment to run memory profiler
def _build_input_messages(n: int = 1000) -> list[dict]:
    logger.debug("Constructing messages...")
    test_messages = get_sample_messages().copy()
    q, mod = divmod(n, len(test_messages))
    messages = test_messages * q + test_messages[:mod]

    logger.debug("Shuffling messages...")
    shuffle(messages)

    return messages


# @profile  # noqa  # Uncomment to run memory profiler
def _run_process(input_size: int = 1000) -> None:
    """Benchmark for core gap detector."""
    gd = GapDetector(
        threshold=timedelta(hours=1, minutes=20)
    )

    messages = _build_input_messages(input_size)
    _, elapsed = timing(gd.detect, quiet=True)(messages)
    return elapsed


def stats(path: Path):
    """Recomputes stats with existing measurement file."""
    measurement = Measurement.from_path(path)
    measurement.save(path)


def run_benchmark(
    input_size: int = 1000,
    reps: int = 10,
    output_dir: Path = Path(OUTPUT_DIR),
    skip_cpu_info: str = True,
) -> Measurement:
    """Runs benchmark and writes output with measurements and statistics.

    Args:
        input_size: amount of input messages.
        reps: number of repetitions.
        output_dir: directory in which to save the outputs.
        skip_cpu_info: if true, doesn't retrieve CPU info (takes 1 sec).

    TODO: pass generic _run_process to as parameter.

    Returns:
        the measurement.
    """
    input_size = int(input_size)
    logger.info("Size of the input: {}".format(input_size))
    logger.info("Number of repetitions: {}".format(reps))

    logger.info("========== MEASUREMENTS ==========")
    times = []
    for rep in range(1, reps + 1):
        elapsed = _run_process(input_size)
        logger.info("Repetition {}; duration: {} sec".format(rep, round(elapsed, 3)))
        times.append(elapsed)

    filename = STATS_FILENAME.format(size=input_size, reps=reps)
    path = output_dir.joinpath(filename)

    cpu_info = None
    if not skip_cpu_info:
        cpu_info = cpuinfo.get_cpu_info()["brand_raw"]

    measurement = Measurement(times, input_size, cpu_info=cpu_info)
    measurement.log_stats()
    measurement.save(path)

    return measurement


def main(args):
    """CLI for benchmark tool."""
    LoggerConfig().setup()

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    parser = argparse.ArgumentParser(prog=NAME, description=DESCRIPTION)
    subparsers = parser.add_subparsers(dest="command", help="available commands")

    p = subparsers.add_parser("run", help=HELP_RUN)
    p.set_defaults(func=run_benchmark)
    p.add_argument("-n", "--input_size", type=float, required=True, metavar="\b", help=HELP_SIZE)
    p.add_argument("-r", "--reps", type=int, default=10, metavar="\b", help=HELP_REPS)
    p.add_argument(
        "-o", "--output_dir", type=Path, default=Path(OUTPUT_DIR), metavar="\b", help=HELP_REPS
    )

    p.add_argument("--skip-cpu-info", action="store_true", help=HELP_SKIP_CPU_INFO)

    s = subparsers.add_parser("stats", help=HELP_STATS)
    s.set_defaults(func=stats)
    s.add_argument("-f", "--path", type=Path, required=True, metavar="\b", help=HELP_SIZE)

    args = parser.parse_args(args=args or ["--help"])

    # command = args.command
    command = args.func
    del args.command
    del args.func

    return command(**vars(args))


if __name__ == "__main__":
    main(sys.argv[1:])
