from datetime import date

from pipe_gaps.queries import GapsDeleteQuery


def test_gaps_delete_query_backticks_table_identifier():
    """The rendered DELETE must backtick the table identifier.

    Without backticks, BigQuery rejects identifiers that contain hyphens
    (e.g. project ``world-fishing-827``) or that start with a digit, raising
    a SQL parse error. With gfw-common < 0.10 the error was silently
    swallowed by ``BigQueryHelper.run_query``'s fire-and-forget pattern; on
    0.10+ ``run_query`` blocks for the result and the error surfaces.
    """
    query = GapsDeleteQuery(
        source_gaps="world-fishing-827.dataset.raw_gaps",
        start_date=date(2024, 1, 1),
    )
    rendered = query.render()
    assert "DELETE FROM `world-fishing-827.dataset.raw_gaps`" in rendered
