from .detect_gaps import DetectGaps


def processes_factory(kind, **kwargs):
    PROCESSES_MAP = {
        "detect_gaps": DetectGaps,
    }

    if kind not in PROCESSES_MAP:
        raise NotImplementedError(f"Process {kind} not implemented.")

    return PROCESSES_MAP[kind].build(**kwargs)
