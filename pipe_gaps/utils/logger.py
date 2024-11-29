import logging

from rich.logging import RichHandler


_TIME_ENTRY = "%(asctime)s - "
_DEFAULT_LOG_FORMAT = f"{_TIME_ENTRY}%(name)s - %(message)s"


def setup_logger(
    level: str = logging.INFO,
    format_: str = _DEFAULT_LOG_FORMAT,
    warning_level: tuple = (),
    error_level: tuple = (),
    force: bool = False,
    rich: bool = True,
) -> None:
    """Configures the root logger.

    Args:
        level: logger level.
        format_: logger format.
        warning_level: list of packages/modules for which to set the log level as WARNING.
        error_level: list of packages/modules for which to set the log level as ERROR.
        force: If true, forces the root logger config replacing the one done on other places.
    """

    handlers = []

    if rich:
        handlers.append(RichHandler())
        format_ = format_.replace(_TIME_ENTRY, "")
    else:
        handlers.append(logging.StreamHandler())

    logging.basicConfig(level=level, format=format_, handlers=handlers, force=force)

    for module in warning_level:
        logging.getLogger(module).setLevel(logging.WARNING)

    for module in error_level:
        logging.getLogger(module).setLevel(logging.ERROR)
