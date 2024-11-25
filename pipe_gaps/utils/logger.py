import logging

from rich.logging import RichHandler


_DEFAULT_LOG_FORMAT = "%(name)s - %(message)s"


def setup_logger(
    level: str = logging.INFO,
    format_: str = _DEFAULT_LOG_FORMAT,
    warning_level: tuple = (),
    error_level: tuple = (),
    force: bool = False,
) -> None:
    """Configures the root logger.

    Args:
        level: logger level.
        format_: logger format.
        warning_level: list of packages/modules for which to set the log level as WARNING.
        error_level: list of packages/modules for which to set the log level as ERROR.
        force: If true, forces the root logger config replacing the one done on other places.
    """

    logging.basicConfig(
        level=level, format=format_, handlers=[RichHandler(level="NOTSET")], force=force
    )

    for module in warning_level:
        logging.getLogger(module).setLevel(logging.WARNING)

    for module in error_level:
        logging.getLogger(module).setLevel(logging.ERROR)
