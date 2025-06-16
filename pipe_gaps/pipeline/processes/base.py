import logging

from abc import ABC

logger = logging.getLogger(__name__)


class CoreProcess(ABC):
    """Base class to define the core step of the pipeline.

    This class provides different abstract methods that help to parallelize the process
        and must be defined in subclasses.
    """
