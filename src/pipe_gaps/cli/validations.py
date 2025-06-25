import argparse

ERROR_DATE_RANGE = "Must be a comma-separated string with two dates in ISO format."


def date_range(date_str: str) -> tuple[str, str]:
    parts = date_str.split(",")
    if len(parts) != 2:
        raise argparse.ArgumentTypeError(ERROR_DATE_RANGE)

    return tuple(parts)


def ssvids(ssvids_str: str) -> tuple[str, ...]:
    return tuple(ssvids_str.split(","))
