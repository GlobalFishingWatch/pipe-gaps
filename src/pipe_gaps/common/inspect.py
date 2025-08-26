import importlib


def top_level_package(module: str) -> str:
    """Return the top-level package name of a module.

    Args:
        module:
            The dotted path of the module to inspect, e.g. "mypackage.subpackage.module".

    Returns:
        The name of the top-level package, e.g. "mypackage".
    """
    module = importlib.import_module(module)
    package = module.__package__.split(".")[0]

    return package
