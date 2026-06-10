"""Canonical message ordering helpers.

Position messages are sorted and selected (first-at-or-after, min-in-range,
boundary ordering) in several places across the detect pipeline. Sorting by
``timestamp`` alone is not a total order: a vessel can emit multiple messages
with the same timestamp (e.g. through different receivers or segments), and
for tied timestamps Python's stable sort preserves the INPUT order -- which,
for messages read from BigQuery, is non-deterministic across runs.

That non-determinism propagates into which message becomes a gap's OFF/ON
endpoint, and from there into ``gap_id`` (a hash over the OFF message's
ssvid/timestamp/lat/lon) and every ``end_*`` output field -- producing
run-to-run diffs on identical input data.

``message_sort_key`` defines the canonical total order ``(timestamp, msgid)``.
``msgid`` is unique per message and present on every pipeline path (it is a
mandatory field of the messages query schema), so ties resolve identically
on every run and on every code path that picks from the same candidates.
"""


def message_sort_key(message: dict, timestamp_key: str = "timestamp") -> tuple:
    """Total-order sort key for position messages: ``(timestamp, msgid)``.

    Args:
        message:
            Position message dictionary.

        timestamp_key:
            Key under which the message stores its unix timestamp.

    Returns:
        Tuple usable as a ``key`` for ``sort``/``sorted``/``min``/``max``.
        Messages missing ``msgid`` (not expected on pipeline paths) sort
        by timestamp with an empty-string tiebreaker.
    """
    return (message[timestamp_key], message.get("msgid") or "")
