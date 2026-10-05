"""Sync selected Hacker News topics into cognee and search the result.

Setup (Hacker News needs no credentials):

    export LLM_API_KEY="..."
    uv run python examples/example.py

Re-run the script to perform an incremental sync. Only new/edited documents and
deletion tombstones are emitted after the first run.
"""

import asyncio

import cognee

from cognee_community_connector_hacker_news import hacker_news_source

DATASET_NAME = "hacker_news"


async def main() -> None:
    source = hacker_news_source(
        topics=["AI agents", "knowledge graphs"],
        # Select any combination of story, comment, and thread.
        content_types=["story", "comment", "thread"],
        max_thread_comments=100,
        thread_depth=5,
    )

    print("Syncing Hacker News (no authentication required) ...")
    await cognee.remember(
        source,
        dataset_name=DATASET_NAME,
        primary_key="id",
        # Required for cursor batches + hard-delete tombstones.
        write_disposition="merge",
    )

    answer = await cognee.search(
        query_text="What are Hacker News users saying about memory for AI agents?",
        query_type=cognee.SearchType.GRAPH_COMPLETION,
        datasets=[DATASET_NAME],
    )
    print("\nSearch result:\n", answer)


if __name__ == "__main__":
    asyncio.run(main())
