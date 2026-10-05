"""Tests for Hacker News discovery, cursors, document rows, and deletion sync."""

import asyncio
import importlib
from types import SimpleNamespace
from urllib.parse import urlparse
from uuid import NAMESPACE_OID, uuid5

import pytest
from cognee.tasks.ingestion.resolve_dlt_sources import _build_document_data_item

from cognee_community_connector_hacker_news.hacker_news import (
    ALGOLIA_SEARCH_URL,
    FIREBASE_ITEM_URL,
    HN_SOURCE_NAME,
    HN_TABLE_NAME,
    _clean_html,
    _item_to_row,
    _iter_topic_hits,
    _parse_since,
    _thread_to_row,
    _validate_content_types,
    _validate_topics,
    hacker_news_source,
    sync_hacker_news,
)


def _story(
    item_id,
    *,
    title="Rust reaches 1.0",
    text="Story body",
    timestamp=100,
    url=None,
    kids=None,
):
    return {
        "id": int(item_id),
        "type": "story",
        "by": "alice",
        "time": timestamp,
        "title": title,
        "text": text,
        "url": url or f"https://example.com/{item_id}",
        "kids": kids or [],
    }


def _comment(item_id, *, parent=1, text="Useful reply", timestamp=101):
    return {
        "id": int(item_id),
        "type": "comment",
        "by": "bob",
        "time": timestamp,
        "parent": int(parent),
        "text": text,
    }


def _story_hit(item_id, *, created=100, title="Rust reaches 1.0"):
    return {
        "objectID": str(item_id),
        "created_at_i": created,
        "title": title,
        "_tags": ["story"],
    }


def _comment_hit(item_id, *, story_id=1, created=101, story_title="Rust reaches 1.0"):
    return {
        "objectID": str(item_id),
        "created_at_i": created,
        "story_id": story_id,
        "story_title": story_title,
        "comment_text": "Useful reply",
        "_tags": ["comment"],
    }


def _thread(story_id=1, comments=None):
    return {
        "id": story_id,
        "title": "Rust reaches 1.0",
        "author": "alice",
        "created_at_i": 100,
        "url": "https://example.com/1",
        "text": "Story body",
        "children": comments
        if comments is not None
        else [
            {
                "id": 2,
                "author": "bob",
                "created_at_i": 101,
                "text": "<p>Useful reply</p>",
                "children": [],
            }
        ],
    }


class FakeHttp:
    """In-memory Algolia/Firebase transport with inspectable calls."""

    def __init__(self, *, hits=None, pages=None, items=None):
        self.hits = hits or {}
        self.pages = pages or {}
        self.items = items or {}
        self.calls = []

    def get_json(self, url, params=None):
        params = dict(params or {})
        self.calls.append((url, params))
        if url == ALGOLIA_SEARCH_URL:
            topic = params["query"]
            page = int(params["page"])
            if (topic, page) in self.pages:
                return self.pages[(topic, page)]
            topic_hits = self.hits.get(topic, [])
            return {"hits": topic_hits if page == 0 else [], "nbPages": 1}
        if url.startswith(f"{FIREBASE_ITEM_URL}/"):
            item_id = url.rsplit("/", 1)[-1].removesuffix(".json")
            return self.items.get(item_id)
        raise AssertionError(f"Unexpected URL: {urlparse(url)}")

    def search_calls(self):
        return [(url, params) for url, params in self.calls if url == ALGOLIA_SEARCH_URL]


def _sync(http, state, *, types=("story", "comment", "thread"), topics=("rust",), **kwargs):
    options = {
        "topics": list(topics),
        "content_types": set(types),
        "initial_since": 0,
        "cursor_overlap_seconds": 10,
        "hits_per_page": 100,
        "max_pages": 10,
        "max_thread_comments": 100,
        "thread_depth": 5,
    }
    options.update(kwargs)
    return list(sync_hacker_news(http, state, **options))


# ---------------------------------------------------------------------------
# Configuration and rendering
# ---------------------------------------------------------------------------


def test_topics_and_content_selection_are_normalized():
    assert _validate_topics(" rust ") == ["rust"]
    assert _validate_topics(["rust", "rust", " AI "]) == ["rust", "AI"]
    assert _validate_content_types(["stories", "comment", "threads"]) == {
        "story",
        "comment",
        "thread",
    }


@pytest.mark.parametrize("topics", [[], [""], None])
def test_topics_are_required(topics):
    with pytest.raises(ValueError):
        _validate_topics(topics)


def test_invalid_content_type_is_rejected():
    with pytest.raises(ValueError, match="Invalid values"):
        _validate_content_types(["job"])


def test_parse_since_accepts_supported_cursor_types():
    assert _parse_since(100.9) == 100
    assert _parse_since("1970-01-01T00:01:40Z") == 100
    assert _parse_since(None) is None
    with pytest.raises(ValueError):
        _parse_since("not-a-date")


def test_clean_html_preserves_paragraph_breaks_and_unescapes():
    assert _clean_html("<p>Hello &amp; goodbye</p><p>Next<br>line</p>") == (
        "Hello & goodbye\nNext\nline"
    )


def test_story_and_comment_rows_are_searchable_documents():
    story = _item_to_row(_story(1), story_id="1", story_title=None)
    comment = _item_to_row(
        _comment(2, text="<p>Hello &amp; goodbye</p>"),
        story_id="1",
        story_title="Rust reaches 1.0",
    )

    assert story == {
        "id": "hn-story-1",
        "url": "https://news.ycombinator.com/item?id=1",
        "title": "Rust reaches 1.0",
        "content": (
            "Hacker News story by alice, published 1970-01-01T00:01:40Z.\n"
            "Original article: https://example.com/1\n\nStory body"
        ),
    }
    assert comment["id"] == "hn-comment-2"
    assert comment["title"] == "Comment on Rust reaches 1.0"
    assert "Thread: https://news.ycombinator.com/item?id=1" in comment["content"]
    assert "Hello & goodbye" in comment["content"]


def test_thread_row_renders_nested_discussion_with_limits():
    nested = {
        "id": 3,
        "author": "carol",
        "created_at_i": 102,
        "text": "Nested",
        "children": [],
    }
    root = _thread(comments=[{**_thread()["children"][0], "children": [nested]}])

    row = _thread_to_row(root, max_comments=10, max_depth=2)
    shallow = _thread_to_row(root, max_comments=10, max_depth=1)

    assert row["id"] == "hn-thread-1"
    assert "Useful reply" in row["content"]
    assert "  carol" in row["content"]
    assert "Nested" not in shallow["content"]


# ---------------------------------------------------------------------------
# Algolia paging and cursor safety
# ---------------------------------------------------------------------------


def test_search_by_date_uses_topic_tags_and_cursor():
    http = FakeHttp(hits={"rust": [_story_hit(1)]})

    hits = list(
        _iter_topic_hits(
            http,
            "rust",
            tags="(story,comment)",
            since_i=90,
            hits_per_page=50,
            max_pages=2,
        )
    )

    assert [hit["objectID"] for hit in hits] == ["1"]
    params = http.search_calls()[0][1]
    assert params == {
        "query": "rust",
        "tags": "(story,comment)",
        "numericFilters": "created_at_i>=90",
        "hitsPerPage": 50,
        "page": 0,
    }


def test_pagination_reads_every_page():
    http = FakeHttp(
        pages={
            ("rust", 0): {"hits": [_story_hit(1)], "nbPages": 2},
            ("rust", 1): {"hits": [_story_hit(2)], "nbPages": 2},
        }
    )
    hits = list(
        _iter_topic_hits(http, "rust", tags="story", since_i=0, hits_per_page=1, max_pages=2)
    )
    assert [hit["objectID"] for hit in hits] == ["1", "2"]


def test_page_cap_fails_instead_of_advancing_past_unseen_hits():
    http = FakeHttp(pages={("rust", 0): {"hits": [_story_hit(1)], "nbPages": 2}})
    with pytest.raises(RuntimeError, match="cursor was not advanced"):
        list(_iter_topic_hits(http, "rust", tags="story", since_i=0, hits_per_page=1, max_pages=1))


# ---------------------------------------------------------------------------
# Incremental state machine
# ---------------------------------------------------------------------------


def test_first_sync_emits_selected_stories_comments_and_threads():
    state = {}
    http = FakeHttp(
        hits={"rust": [_story_hit(1), _comment_hit(2)]},
        items={"1": _story(1, kids=[2]), "2": _comment(2)},
    )

    rows = _sync(http, state)

    assert {row["id"] for row in rows} == {"hn-story-1", "hn-comment-2", "hn-thread-1"}
    assert state["topic_cursors"] == {"rust": 101}
    assert set(state["known_items"]) == {"1", "2"}
    assert set(state["known_threads"]) == {"1"}


def test_unchanged_resync_emits_no_rows_and_reuses_overlap_cursor():
    state = {}
    first = FakeHttp(
        hits={"rust": [_story_hit(1, created=100)]},
        items={"1": _story(1, kids=[2]), "2": _comment(2)},
    )
    _sync(first, state)

    second = FakeHttp(
        hits={"rust": [_story_hit(1, created=100)]},
        items={"1": _story(1, kids=[2]), "2": _comment(2)},
    )
    rows = _sync(second, state)

    assert rows == []
    assert second.search_calls()[0][1]["numericFilters"] == "created_at_i>=90"
    assert state["topic_cursors"] == {"rust": 100}


def test_incremental_sync_emits_only_new_item_and_changed_thread():
    state = {}
    _sync(
        FakeHttp(
            hits={"rust": [_story_hit(1)]},
            items={"1": _story(1)},
        ),
        state,
    )

    rows = _sync(
        FakeHttp(
            hits={"rust": [_story_hit(1), _comment_hit(2, created=110)]},
            items={"1": _story(1, kids=[2]), "2": _comment(2)},
        ),
        state,
    )

    assert {row["id"] for row in rows} == {"hn-comment-2", "hn-thread-1"}
    assert state["topic_cursors"] == {"rust": 110}


def test_edit_to_old_item_is_picked_up_by_revalidation():
    state = {}
    _sync(
        FakeHttp(hits={"rust": [_story_hit(1)]}, items={"1": _story(1)}),
        state,
        types=("story",),
    )

    rows = _sync(
        FakeHttp(hits={"rust": []}, items={"1": _story(1, text="Edited body")}),
        state,
        types=("story",),
    )

    assert [row["id"] for row in rows] == ["hn-story-1"]
    assert "Edited body" in rows[0]["content"]


@pytest.mark.parametrize("gone", [None, {"id": 1, "deleted": True}, {"id": 1, "dead": True}])
def test_upstream_delete_emits_hard_delete_tombstone(gone):
    state = {}
    _sync(
        FakeHttp(hits={"rust": [_story_hit(1)]}, items={"1": _story(1)}),
        state,
        types=("story",),
    )

    rows = _sync(
        FakeHttp(hits={"rust": []}, items={"1": gone}),
        state,
        types=("story",),
    )

    assert rows == [{"id": "hn-story-1", "_deleted": True}]
    assert state["known_items"] == {}


def test_deleted_comment_updates_thread_and_deletes_comment_document():
    state = {}
    _sync(
        FakeHttp(
            hits={"rust": [_comment_hit(2)]},
            items={"1": _story(1, kids=[2]), "2": _comment(2)},
        ),
        state,
        types=("comment", "thread"),
    )

    rows = _sync(
        FakeHttp(
            hits={"rust": []},
            items={"1": _story(1, kids=[2]), "2": None},
        ),
        state,
        types=("comment", "thread"),
    )

    assert {row["id"] for row in rows} == {"hn-comment-2", "hn-thread-1"}
    tombstone = next(row for row in rows if row["id"] == "hn-comment-2")
    thread = next(row for row in rows if row["id"] == "hn-thread-1")
    assert tombstone["_deleted"] is True
    assert "Useful reply" not in thread["content"]


def test_deselecting_threads_emits_one_tombstone_per_retained_thread():
    state = {}
    _sync(
        FakeHttp(hits={"rust": [_story_hit(1)]}, items={"1": _story(1)}),
        state,
        types=("story", "thread"),
    )

    rows = _sync(
        FakeHttp(hits={"rust": []}, items={"1": _story(1)}),
        state,
        types=("story",),
    )

    assert rows == [{"id": "hn-thread-1", "_deleted": True}]
    assert state["known_threads"] == {}


def test_failure_does_not_publish_cursor_state():
    class BrokenHttp(FakeHttp):
        def get_json(self, url, params=None):
            raise ValueError("bad response")

    state = {"topic_cursors": {"rust": 100}}
    with pytest.raises(ValueError, match="bad response"):
        _sync(BrokenHttp(), state)
    assert state == {"topic_cursors": {"rust": 100}}


# ---------------------------------------------------------------------------
# cognee marker and real dlt merge
# ---------------------------------------------------------------------------


def test_document_marker_and_data_item_mapping():
    from cognee.tasks.ingestion.dlt_utils import document_source_tag

    source = hacker_news_source(
        ["rust"],
        content_types=["story"],
        initial_since=0,
        http_client=FakeHttp(),
    )
    assert document_source_tag(source) == HN_SOURCE_NAME

    row = SimpleNamespace(
        row_data={
            "id": "hn-story-1",
            "url": "https://news.ycombinator.com/item?id=1",
            "title": "Rust reaches 1.0",
            "content": "Story body",
        },
        content_hash="abc",
        table_name=HN_TABLE_NAME,
    )
    data_id = uuid5(NAMESPACE_OID, "hn-story-1")
    item = _build_document_data_item(row, data_id, HN_SOURCE_NAME)
    metadata = getattr(item, "system_metadata", None) or item.external_metadata
    assert metadata["source"] == HN_SOURCE_NAME
    assert metadata["external_id"] == "hn-story-1"
    assert item.data.startswith("# Rust reaches 1.0")


def test_resource_uses_merge_primary_key_and_hard_delete():
    source = hacker_news_source(
        ["rust"],
        content_types=["story"],
        initial_since=0,
        http_client=FakeHttp(),
    )
    schema = source.resources[HN_TABLE_NAME].compute_table_schema()
    disposition = schema["write_disposition"]
    if isinstance(disposition, dict):
        disposition = disposition.get("disposition")
    assert disposition == "merge"
    assert schema["columns"]["id"].get("primary_key") is True
    assert schema["columns"]["_deleted"].get("hard_delete") is True


@pytest.fixture
def dlt_mod():
    return pytest.importorskip("dlt")


def _pipeline(dlt_mod, tmp_path):
    db_path = (tmp_path / "hacker_news.db").as_posix()
    return dlt_mod.pipeline(
        pipeline_name="hacker_news_test",
        destination=dlt_mod.destinations.sqlalchemy(f"sqlite:///{db_path}"),
        dataset_name="hacker_news_ds",
        pipelines_dir=str(tmp_path / "state"),
    )


def _read_ids(pipeline):
    with (
        pipeline.sql_client() as client,
        client.execute_query(f"SELECT id FROM {HN_TABLE_NAME}") as cursor,
    ):
        return {row[0] for row in cursor.fetchall()}


def test_real_dlt_incremental_cursor_and_hard_delete(dlt_mod, tmp_path):
    pipeline = _pipeline(dlt_mod, tmp_path)
    first = FakeHttp(hits={"rust": [_story_hit(1)]}, items={"1": _story(1)})
    pipeline.run(
        hacker_news_source(
            ["rust"],
            content_types=["story"],
            initial_since=0,
            cursor_overlap_seconds=10,
            http_client=first,
        )
    )
    assert _read_ids(pipeline) == {"hn-story-1"}

    second = FakeHttp(hits={"rust": []}, items={"1": None})
    pipeline.run(
        hacker_news_source(
            ["rust"],
            content_types=["story"],
            initial_since=0,
            cursor_overlap_seconds=10,
            http_client=second,
        )
    )

    assert _read_ids(pipeline) == set()
    assert second.search_calls()[0][1]["numericFilters"] == "created_at_i>=90"


def test_cognee_graph_ingest_and_forget_on_delete(tmp_path, monkeypatch):
    """Prove document-mode ingest and deletion reach cognee's graph layer."""
    import cognee
    from cognee.infrastructure.databases.graph import get_graph_engine
    from cognee.infrastructure.databases.vector.embeddings.LiteLLMEmbeddingEngine import (
        LiteLLMEmbeddingEngine,
    )
    from cognee.infrastructure.llm import LLMGateway

    pytest.importorskip("ladybug")
    add_data_points = importlib.import_module("cognee.tasks.storage.add_data_points")
    alpha = "HnAlphacorp"
    bravo = "HnBravocorp"
    dataset = "hacker_news_graph_forget_test"

    async def mock_structured_output(
        text_input=None, system_prompt=None, response_model=str, **_kwargs
    ):
        from cognee.shared.data_models import KnowledgeGraph, SummarizedContent
        from cognee.shared.data_models import Node as KGNode

        if response_model is str:
            return "Mocked answer."
        if response_model == SummarizedContent:
            return SummarizedContent(summary="Mock summary", description="Mock summary")
        if response_model == KnowledgeGraph:
            name = next(
                (token for token in (alpha, bravo) if text_input and token in text_input),
                None,
            )
            nodes = (
                [KGNode(id=name, name=name, type="Company", description=f"{name} entity")]
                if name
                else []
            )
            return KnowledgeGraph(nodes=nodes, edges=[])
        return response_model()

    async def noop_index(*_args, **_kwargs):
        return None

    async def mock_embed_text(self, text):
        return [[0.0] * self.get_vector_size() for _ in text]

    async def graph_has(token):
        nodes, _ = await (await get_graph_engine()).get_graph_data()
        lowered = token.lower()
        return any(
            lowered in str(value).lower()
            for _node_id, properties in nodes
            for value in (properties or {}).values()
        )

    async def sync(http):
        await cognee.add(
            hacker_news_source(
                ["graph memory"],
                content_types=["story"],
                initial_since=0,
                http_client=http,
            ),
            dataset_name=dataset,
            primary_key="id",
            write_disposition="merge",
            max_rows_per_table=0,
        )
        await cognee.cognify(datasets=[dataset])

    async def scenario():
        monkeypatch.setenv("COGNEE_SKIP_CONNECTION_TEST", "true")
        cognee.config.data_root_directory(str(tmp_path / "data"))
        cognee.config.system_root_directory(str(tmp_path / "system"))
        cognee.config.set_relational_db_config({"db_provider": "sqlite"})
        monkeypatch.setattr(add_data_points, "index_data_points", noop_index)
        monkeypatch.setattr(add_data_points, "index_graph_edges", noop_index)
        monkeypatch.setattr(LLMGateway, "acreate_structured_output", mock_structured_output)
        monkeypatch.setattr(LiteLLMEmbeddingEngine, "embed_text", mock_embed_text)

        await cognee.prune.prune_data()
        await cognee.prune.prune_system(metadata=True)
        try:
            first = FakeHttp(
                hits={"graph memory": [_story_hit(1, created=100), _story_hit(2, created=101)]},
                items={
                    "1": _story(1, title=alpha, text=f"{alpha} builds graph memory."),
                    "2": _story(2, title=bravo, text=f"{bravo} builds unrelated storage."),
                },
            )
            await sync(first)
            assert await graph_has(alpha), "first selected story must be searchable in the graph"
            assert await graph_has(bravo), "second selected story must be searchable in the graph"

            second = FakeHttp(
                hits={"graph memory": []},
                items={
                    "1": _story(1, title=alpha, text=f"{alpha} builds graph memory."),
                    "2": None,
                },
            )
            await sync(second)
            assert await graph_has(alpha), "surviving story must remain in the graph"
            assert not await graph_has(bravo), "deleted upstream story must leave the graph"
        finally:
            await cognee.prune.prune_data()
            await cognee.prune.prune_system(metadata=True)

    asyncio.run(scenario())
