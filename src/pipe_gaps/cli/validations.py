import argparse

from datetime import date

ERROR_DATE = "Must be a string with a date in ISO format."
ERROR_DATE_RANGE = "Must be a comma-separated string with two dates in ISO format."


def validate_date(date_str):
    try:
        date.fromisoformat(date_str)
    except Exception as e:
        raise argparse.ArgumentTypeError(f"{ERROR_DATE} \n {e}")


def date_range(date_str):
    date_range = date_str.split(",")
    if len(date_range) != 2:
        raise argparse.ArgumentTypeError(ERROR_DATE_RANGE)

    for d in date_range:
        validate_date(d)

    return date_range


def ssvids(ssvids_str):
    return ssvids_str.split(",")
