# cognee-community-connector-hacker-news

Track Hacker News topics in [cognee](https://github.com/topoteretes/cognee) and
search matching stories, comments, and discussion threads as memory.

The connector has no authentication step. It uses the public
[Algolia HN Search API](https://hn.algolia.com/api) for topic discovery and the
official [Hacker News Firebase API](https://github.com/HackerNews/API) to detect
edits and upstream deletions. Rows are tagged as document sources, so they pass
through cognee's normal cognify entity-extraction pipeline.

## Install

```bash
uv pip install cognee-community-connector-hacker-news

# From this monorepo:
cd packages/connector/hacker-news
uv sync
```

Set the LLM key required by your cognee configuration. Hacker News itself needs
no key or token.

```bash
export LLM_API_KEY="..."
```

## Usage

```python
import cognee
from cognee_community_connector_hacker_news import hacker_news_source

source = hacker_news_source(
    topics=["AI agents", "knowledge graphs"],
    content_types=["story", "comment", "thread"],
)

await cognee.remember(
    source,
    dataset_name="hacker_news",
    primary_key="id",
    write_disposition="merge",
)

results = await cognee.search(
    query_text="What are Hacker News commenters saying about agent memory?",
    query_type=cognee.SearchType.GRAPH_COMPLETION,
    datasets=["hacker_news"],
)
```

Use a dedicated dataset for a stable set of topics and content types. See
[`examples/example.py`](examples/example.py) for a runnable program.

## Selecting content

`topics` is required and accepts one phrase or an iterable of phrases.
`content_types` accepts:

- `story`: one searchable document per matching HN story;
- `comment`: one searchable document per matching HN comment, with thread context;
- `thread`: one aggregate document containing a story and its nested discussion.

Plural spellings (`stories`, `comments`, `threads`) are also accepted. Thread
size is bounded with `max_thread_comments` and `thread_depth`.

The initial sync searches the last 30 days by default. For a different backfill
boundary, pass a unix timestamp, datetime, or ISO-8601 value:

```python
hacker_news_source(
    "vector databases",
    initial_since="2026-01-01T00:00:00Z",
    max_pages=50,
)
```

If a topic has more pages than `max_pages`, the run fails without advancing its
cursor. Increase the cap or narrow the topic; the connector never silently skips
a page.

## Incremental sync

Each topic keeps an independent `created_at_i` high-water cursor in dlt state.
Later runs call Algolia `search_by_date` only from that cursor, with a one-hour
overlap to tolerate delayed indexing. Stable fingerprints suppress boundary
replays, so only new or edited documents are yielded to dlt and cognee.

Hacker News does not expose a durable deletion cursor. To meet forget-on-delete,
the connector revalidates the IDs it already tracks through the lightweight
Firebase Item endpoint. Unchanged items are not emitted. Edited items are merged;
`deleted`, `dead`, 404, and null items emit dlt hard-delete tombstones. Thread
documents are refreshed so deleting a comment removes it from the aggregate on
the same sync. Cognee's existing `orphan_cleanup` then removes deleted documents
from graph and vector storage after fresh rows commit.

Important: cognee's ingestion call defaults to `write_disposition="replace"`.
Always pass `primary_key="id"` and `write_disposition="merge"` as shown above;
otherwise an incremental batch would replace the retained corpus.

## Testing

```bash
cd packages/connector/hacker-news
uv run pytest tests/ -v
```

Tests use fake Algolia/Firebase responses, mocked LLM/embedding calls, and
temporary SQLite, dlt, and graph stores. No live API, account, or token is used.
