import argparse

from datetime import date

ERROR_DATE = "Must be a string with a date in ISO format."
ERROR_DATE_RANGE = "Must be a comma-separated string with two dates in ISO format."


def validate_date(date_str: str) -> None:
    try:
        date.fromisoformat(date_str)
    except Exception as e:
        raise argparse.ArgumentTypeError(f"{ERROR_DATE} \n {e}")


def date_range(date_str: str) -> tuple[str, str]:
    parts = date_str.split(",")
    if len(parts) != 2:
        raise argparse.ArgumentTypeError(ERROR_DATE_RANGE)

    for d in parts:
        validate_date(d)

    return tuple(parts)


def ssvids(ssvids_str: str) -> tuple[str, ...]:
    return tuple(ssvids_str.split(","))
