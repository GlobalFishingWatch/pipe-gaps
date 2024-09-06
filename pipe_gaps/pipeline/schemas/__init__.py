from pipe_gaps.queries import Message, AISGap

__all__ = [Message]

SCHEMAS = {
    "messages": Message,
    "ais_gaps": AISGap
}


def get_schema(name):
    if name not in SCHEMAS:
        raise NotImplementedError(
            f"Schema with name '{name}' not implemented!. "
            f"Available schemas: {list(SCHEMAS.keys())}.")

    return SCHEMAS[name]
