"""Incremental Hacker News data-source connector for cognee.

Algolia's HN Search API discovers stories and comments matching configured
topics. The official Firebase Item API revalidates already-tracked items so
edits and upstream deletions become merge rows or hard-delete tombstones, and
hydrates optional thread documents from authoritative current items.

Rows use cognee's document-source marker and therefore flow through normal
cognify entity extraction.  The dlt resource uses ``merge`` by stable row id;
only new/edited rows and deletion tombstones are yielded after the first run.
"""

from __future__ import annotations

import hashlib
import html
import json
import re
import time
from collections.abc import Iterable, Iterator
from datetime import UTC, datetime, timedelta
from typing import Any

from cognee.shared.logging_utils import get_logger
from cognee.tasks.ingestion.dlt_utils import DOCUMENT_SOURCE_ATTR

logger = get_logger("hacker_news_connector")

HN_TABLE_NAME = "hacker_news_items"
HN_SOURCE_NAME = "hacker_news"

ALGOLIA_SEARCH_URL = "https://hn.algolia.com/api/v1/search_by_date"
FIREBASE_ITEM_URL = "https://hacker-news.firebaseio.com/v0/item"
HN_ITEM_URL = "https://news.ycombinator.com/item"

DEFAULT_CONTENT_TYPES = ("story", "comment", "thread")
_VALID_CONTENT_TYPES = frozenset(DEFAULT_CONTENT_TYPES)
_MAX_RETRIES = 5
_HTTP_TIMEOUT = 30

_EXTRA_HINT = (
    "The Hacker News connector requires dlt and requests: pip install "
    '"cognee-community-connector-hacker-news".'
)

_BREAK_RE = re.compile(r"(?i)<\s*(?:br|/p|/div|/li|/h[1-6])\s*/?>")
_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"[ \t]+")


def hacker_news_source(
    topics: Iterable[str],
    *,
    content_types: Iterable[str] = DEFAULT_CONTENT_TYPES,
    initial_since: datetime | int | float | str | None = None,
    cursor_overlap_seconds: int = 3600,
    hits_per_page: int = 100,
    max_pages: int = 20,
    max_thread_comments: int = 100,
    thread_depth: int = 5,
    http_client: Any = None,
):
    """Return a document-mode dlt source for topic-filtered Hacker News data.

    Args:
        topics: Search phrases to track. At least one non-empty topic is required.
        content_types: Any of ``story``, ``comment`` and ``thread``. Stories and
            comments are individual documents; a thread is an aggregate story +
            discussion document.
        initial_since: Lower time bound used only before a topic has a stored
            cursor. Accepts UTC unix seconds, a datetime, or an ISO-8601 string.
            Defaults to 30 days before source construction.
        cursor_overlap_seconds: Re-query this much history on incremental runs
            to tolerate delayed Algolia indexing. Stable fingerprints suppress
            unchanged rows.
        hits_per_page: Algolia page size (1-1000).
        max_pages: Safety cap per topic. The sync fails instead of advancing the
            cursor when the cap would truncate results.
        max_thread_comments: Maximum comments rendered in one thread document.
        thread_depth: Maximum nested comment depth rendered in a thread.
        http_client: Optional object with ``get_json(url, params=None)`` for tests
            or custom transports. No authentication is required.

    Returns:
        A dlt source for ``cognee.remember`` / ``cognee.add``.
    """
    try:
        import dlt
    except ImportError as exc:
        raise ImportError(_EXTRA_HINT) from exc

    clean_topics = _validate_topics(topics)
    selected_types = _validate_content_types(content_types)
    since_i = _parse_since(initial_since)
    if since_i is None:
        since_i = int((datetime.now(tz=UTC) - timedelta(days=30)).timestamp())
    if cursor_overlap_seconds < 0:
        raise ValueError("cursor_overlap_seconds must be >= 0.")
    if not 1 <= hits_per_page <= 1000:
        raise ValueError("hits_per_page must be between 1 and 1000.")
    if max_pages < 1:
        raise ValueError("max_pages must be >= 1.")
    if max_thread_comments < 0:
        raise ValueError("max_thread_comments must be >= 0.")
    if thread_depth < 0:
        raise ValueError("thread_depth must be >= 0.")

    http = http_client if http_client is not None else _RequestsClient()

    @dlt.resource(
        name=HN_TABLE_NAME,
        primary_key="id",
        write_disposition="merge",
        columns={"_deleted": {"data_type": "bool", "hard_delete": True}},
    )
    def hacker_news_items():
        state = dlt.current.resource_state()
        yield from sync_hacker_news(
            http,
            state,
            topics=clean_topics,
            content_types=selected_types,
            initial_since=since_i,
            cursor_overlap_seconds=cursor_overlap_seconds,
            hits_per_page=hits_per_page,
            max_pages=max_pages,
            max_thread_comments=max_thread_comments,
            thread_depth=thread_depth,
        )

    @dlt.source(name=HN_SOURCE_NAME)
    def _hacker_news():
        return hacker_news_items

    source = _hacker_news()
    setattr(source, DOCUMENT_SOURCE_ATTR, HN_SOURCE_NAME)
    return source


def sync_hacker_news(
    http: Any,
    state: dict[str, Any],
    *,
    topics: list[str],
    content_types: set[str],
    initial_since: int,
    cursor_overlap_seconds: int,
    hits_per_page: int,
    max_pages: int,
    max_thread_comments: int,
    thread_depth: int,
) -> Iterator[dict[str, Any]]:
    """Yield only changed rows/tombstones and advance state after a safe run.

    The plain-dict state boundary keeps the sync state machine testable without
    dlt. State is copied up front and published only after every API operation
    succeeds, so a failed run cannot advance a cursor past unseen data.
    """
    topic_cursors = {
        str(topic): int(value) for topic, value in (state.get("topic_cursors") or {}).items()
    }
    known_items = {
        str(item_id): dict(metadata)
        for item_id, metadata in (state.get("known_items") or {}).items()
    }
    known_threads = {
        str(story_id): dict(metadata)
        for story_id, metadata in (state.get("known_threads") or {}).items()
    }

    item_cache: dict[str, dict | None] = {}
    emitted_rows: set[str] = set()
    changed = 0
    deleted = 0

    def get_item(item_id: str) -> dict | None:
        if item_id not in item_cache:
            item_cache[item_id] = _fetch_firebase_item(http, item_id)
        return item_cache[item_id]

    # Revalidate every retained individual document. This lightweight id lookup
    # is the deletion/edit feed HN does not otherwise provide; unchanged items
    # are not yielded to dlt or cognee.
    for item_id in sorted(known_items, key=_numeric_sort_key):
        metadata = known_items[item_id]
        row_id = str(metadata["row_id"])
        item = get_item(item_id)
        if item is None:
            yield _deleted_row(row_id)
            emitted_rows.add(row_id)
            del known_items[item_id]
            deleted += 1
            continue

        row = _item_to_row(
            item,
            story_id=metadata.get("story_id"),
            story_title=metadata.get("story_title"),
        )
        if row is None:
            yield _deleted_row(row_id)
            emitted_rows.add(row_id)
            del known_items[item_id]
            deleted += 1
            continue

        fingerprint = _row_fingerprint(row)
        if fingerprint != metadata.get("fingerprint"):
            yield row
            emitted_rows.add(row_id)
            changed += 1
        metadata.update(
            fingerprint=fingerprint,
            story_id=_story_id_for(item, metadata.get("story_id")),
            story_title=metadata.get("story_title") or row["title"],
        )

    touched_threads: set[str] = set()
    discovery_tags = _discovery_tags(content_types)

    # Discover new topic matches with a per-topic cursor. The overlap makes
    # late-indexed hits visible; fingerprints make replaying the boundary cheap.
    for topic in topics:
        cursor = topic_cursors.get(topic, initial_since)
        lower_bound = max(initial_since, cursor - cursor_overlap_seconds)
        newest = cursor
        for hit in _iter_topic_hits(
            http,
            topic,
            tags=discovery_tags,
            since_i=lower_bound,
            hits_per_page=hits_per_page,
            max_pages=max_pages,
        ):
            created_i = _as_int(hit.get("created_at_i"), default=0)
            newest = max(newest, created_i)
            item_id = str(hit.get("objectID") or "").strip()
            if not item_id:
                raise ValueError(f"Hacker News: Algolia returned a hit without objectID: {hit!r}")

            item = get_item(item_id)
            if item is None:
                continue
            item_type = item.get("type")
            story_id = _hit_story_id(hit, item, get_item)
            story_title = str(hit.get("story_title") or hit.get("title") or "").strip()

            if "thread" in content_types and story_id:
                touched_threads.add(story_id)

            if item_type not in content_types or item_type not in {"story", "comment"}:
                continue

            row = _item_to_row(item, story_id=story_id, story_title=story_title)
            if row is None:
                continue
            row_id = row["id"]
            fingerprint = _row_fingerprint(row)
            previous = known_items.get(item_id)
            if (
                previous is None or previous.get("fingerprint") != fingerprint
            ) and row_id not in emitted_rows:
                yield row
                emitted_rows.add(row_id)
                changed += 1
            known_items[item_id] = {
                "row_id": row_id,
                "kind": item_type,
                "story_id": story_id,
                "story_title": story_title or row["title"],
                "fingerprint": fingerprint,
            }

        topic_cursors[topic] = newest

    if "thread" not in content_types:
        for metadata in known_threads.values():
            row_id = str(metadata["row_id"])
            if row_id not in emitted_rows:
                yield _deleted_row(row_id)
                emitted_rows.add(row_id)
                deleted += 1
        known_threads.clear()
    else:
        # Refresh retained threads as well as newly touched ones. A removed
        # comment changes the aggregate fingerprint; a removed root emits a
        # thread tombstone.
        thread_ids = set(known_threads) | touched_threads
        for story_id in sorted(thread_ids, key=_numeric_sort_key):
            previous = known_threads.get(story_id)
            row_id = f"hn-thread-{story_id}"
            root = get_item(story_id)
            thread = _hydrate_firebase_thread(
                root,
                get_item,
                max_comments=max_thread_comments,
                max_depth=thread_depth,
            )
            row = _thread_to_row(
                thread,
                max_comments=max_thread_comments,
                max_depth=thread_depth,
            )
            if row is None:
                if previous is not None and row_id not in emitted_rows:
                    yield _deleted_row(row_id)
                    emitted_rows.add(row_id)
                    deleted += 1
                known_threads.pop(story_id, None)
                continue

            fingerprint = _row_fingerprint(row)
            if (
                previous is None or previous.get("fingerprint") != fingerprint
            ) and row_id not in emitted_rows:
                yield row
                emitted_rows.add(row_id)
                changed += 1
            known_threads[story_id] = {"row_id": row_id, "fingerprint": fingerprint}

    # Publish state only after the full discovery + reconciliation pass. dlt
    # commits resource state transactionally with the successful load.
    state["topic_cursors"] = topic_cursors
    state["known_items"] = known_items
    state["known_threads"] = known_threads
    logger.info(
        "Hacker News: %d changed/new document(s), %d deletion(s), %d retained item(s).",
        changed,
        deleted,
        len(known_items) + len(known_threads),
    )


def _validate_topics(topics: Iterable[str]) -> list[str]:
    if isinstance(topics, str):
        topics = [topics]
    try:
        clean = [topic.strip() for topic in topics if isinstance(topic, str) and topic.strip()]
    except TypeError as exc:
        raise ValueError("topics must be a string or iterable of strings.") from exc
    # Preserve input order while avoiding duplicate API searches.
    clean = list(dict.fromkeys(clean))
    if not clean:
        raise ValueError("hacker_news_source requires at least one non-empty topic.")
    return clean


def _validate_content_types(content_types: Iterable[str]) -> set[str]:
    if isinstance(content_types, str):
        content_types = [content_types]
    aliases = {"stories": "story", "comments": "comment", "threads": "thread"}
    try:
        selected = {
            aliases.get(value.strip().lower(), value.strip().lower())
            for value in content_types
            if isinstance(value, str) and value.strip()
        }
    except TypeError as exc:
        raise ValueError("content_types must be an iterable of strings.") from exc
    invalid = selected - _VALID_CONTENT_TYPES
    if invalid or not selected:
        allowed = ", ".join(sorted(_VALID_CONTENT_TYPES))
        detail = f" Invalid values: {sorted(invalid)!r}." if invalid else ""
        raise ValueError(f"Select at least one content type from: {allowed}.{detail}")
    return selected


def _parse_since(value: datetime | int | float | str | None) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError("initial_since must be a unix timestamp, datetime, or ISO-8601 string.")
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, datetime):
        parsed = value if value.tzinfo else value.replace(tzinfo=UTC)
        return int(parsed.timestamp())
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError(f"Could not parse initial_since={value!r}.") from exc
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return int(parsed.timestamp())
    raise ValueError("initial_since must be a unix timestamp, datetime, or ISO-8601 string.")


def _discovery_tags(content_types: set[str]) -> str:
    if "thread" in content_types or {"story", "comment"} <= content_types:
        return "(story,comment)"
    return "comment" if "comment" in content_types else "story"


def _iter_topic_hits(
    http: Any,
    topic: str,
    *,
    tags: str,
    since_i: int,
    hits_per_page: int,
    max_pages: int,
) -> Iterator[dict[str, Any]]:
    """Yield every Algolia page or fail rather than silently skipping a gap."""
    for page in range(max_pages):
        payload = _with_retry(
            lambda page=page: http.get_json(
                ALGOLIA_SEARCH_URL,
                params={
                    "query": topic,
                    "tags": tags,
                    "numericFilters": f"created_at_i>={since_i}",
                    "hitsPerPage": hits_per_page,
                    "page": page,
                },
            ),
            what=f"Algolia search for topic {topic!r}",
        )
        if not isinstance(payload, dict) or not isinstance(payload.get("hits"), list):
            raise ValueError(f"Hacker News: invalid Algolia response for topic {topic!r}.")
        hits = payload["hits"]
        yield from hits
        nb_pages = _as_int(payload.get("nbPages"), default=page + 1)
        if page + 1 >= nb_pages:
            return
    raise RuntimeError(
        f"Hacker News: topic {topic!r} has more than max_pages={max_pages} pages. "
        "Increase max_pages or narrow the topic; the cursor was not advanced."
    )


def _fetch_firebase_item(http: Any, item_id: str) -> dict | None:
    payload = _with_retry(
        lambda: http.get_json(f"{FIREBASE_ITEM_URL}/{item_id}.json"),
        what=f"Firebase item {item_id}",
    )
    if not isinstance(payload, dict) or payload.get("deleted") or payload.get("dead"):
        return None
    if str(payload.get("id") or "") != item_id:
        raise ValueError(f"Hacker News: Firebase returned the wrong item for id {item_id}.")
    return payload


def _hydrate_firebase_thread(
    root: dict[str, Any] | None,
    get_item,
    *,
    max_comments: int,
    max_depth: int,
) -> dict[str, Any] | None:
    """Build an Algolia-shaped thread from authoritative Firebase items."""
    if root is None or root.get("type") != "story":
        return None

    remaining = max_comments

    def hydrate_children(parent: dict[str, Any], depth: int) -> list[dict[str, Any]]:
        nonlocal remaining
        if remaining <= 0 or depth >= max_depth:
            return []
        children: list[dict[str, Any]] = []
        for child_id in parent.get("kids") or []:
            if remaining <= 0:
                break
            child = get_item(str(child_id))
            if child is None or child.get("type") != "comment":
                continue
            remaining -= 1
            children.append(
                {
                    "id": child.get("id"),
                    "author": child.get("by"),
                    "time": child.get("time"),
                    "text": child.get("text"),
                    "children": hydrate_children(child, depth + 1),
                }
            )
        return children

    return {
        "id": root.get("id"),
        "title": root.get("title"),
        "author": root.get("by"),
        "time": root.get("time"),
        "url": root.get("url"),
        "text": root.get("text"),
        "children": hydrate_children(root, 0),
    }


def _hit_story_id(hit: dict, item: dict, get_item) -> str | None:
    if item.get("type") == "story":
        return str(item["id"])
    story_id = str(hit.get("story_id") or "").strip()
    if story_id:
        return story_id
    return _walk_to_story(item, get_item)


def _walk_to_story(item: dict, get_item) -> str | None:
    parent_id = item.get("parent")
    visited: set[str] = set()
    while parent_id is not None and len(visited) < 100:
        parent_key = str(parent_id)
        if parent_key in visited:
            return None
        visited.add(parent_key)
        parent = get_item(parent_key)
        if parent is None:
            return None
        if parent.get("type") == "story":
            return str(parent["id"])
        parent_id = parent.get("parent")
    return None


def _story_id_for(item: dict, fallback: Any) -> str | None:
    if item.get("type") == "story":
        return str(item["id"])
    return str(fallback) if fallback is not None else None


def _item_to_row(
    item: dict[str, Any],
    *,
    story_id: Any = None,
    story_title: Any = None,
) -> dict[str, Any] | None:
    item_id = str(item.get("id") or "").strip()
    item_type = item.get("type")
    if not item_id or item_type not in {"story", "comment"}:
        return None

    author = str(item.get("by") or "unknown")
    published = _format_datetime(item.get("time"))
    body = _clean_html(item.get("text") or "")
    hn_url = f"{HN_ITEM_URL}?id={item_id}"

    if item_type == "story":
        title = _clean_html(item.get("title") or "") or f"Hacker News story {item_id}"
        lines = [f"Hacker News story by {author}, published {published}."]
        if item.get("url"):
            lines.append(f"Original article: {item['url']}")
        if body:
            lines.extend(["", body])
        return {
            "id": f"hn-story-{item_id}",
            "url": hn_url,
            "title": title,
            "content": "\n".join(lines).strip(),
        }

    root_id = str(story_id or "").strip()
    thread_title = str(story_title or "").strip()
    title = f"Comment on {thread_title}" if thread_title else f"Hacker News comment {item_id}"
    lines = [f"Hacker News comment by {author}, published {published}."]
    if root_id:
        lines.append(f"Thread: {HN_ITEM_URL}?id={root_id}")
    if item.get("parent"):
        lines.append(f"Replying to item {item['parent']}.")
    if body:
        lines.extend(["", body])
    return {
        "id": f"hn-comment-{item_id}",
        "url": hn_url,
        "title": title,
        "content": "\n".join(lines).strip(),
    }


def _thread_to_row(
    thread: dict[str, Any] | None,
    *,
    max_comments: int,
    max_depth: int,
) -> dict[str, Any] | None:
    if not thread:
        return None
    story_id = str(thread.get("id") or "").strip()
    if not story_id:
        return None
    title = _clean_html(thread.get("title") or "") or f"Hacker News thread {story_id}"
    published = _format_datetime(
        thread.get("created_at_i") or thread.get("time") or thread.get("created_at")
    )
    lines = [
        f"Discussion started by {thread.get('author') or 'unknown'}, published {published}.",
    ]
    if thread.get("url"):
        lines.append(f"Original article: {thread['url']}")
    story_text = _clean_html(thread.get("text") or "")
    if story_text:
        lines.extend(["", story_text])

    comments = list(
        _iter_thread_comments(
            thread.get("children") or [],
            max_comments=max_comments,
            max_depth=max_depth,
        )
    )
    if comments:
        lines.extend(["", "Discussion:"])
        for comment, depth in comments:
            indent = "  " * depth
            author = comment.get("author") or "unknown"
            date = _format_datetime(
                comment.get("created_at_i") or comment.get("time") or comment.get("created_at")
            )
            lines.extend(["", f"{indent}{author} ({date}):"])
            text = _clean_html(comment.get("text") or "")
            lines.extend(f"{indent}{line}" for line in text.splitlines())

    return {
        "id": f"hn-thread-{story_id}",
        "url": f"{HN_ITEM_URL}?id={story_id}",
        "title": f"HN thread: {title}",
        "content": "\n".join(lines).strip(),
    }


def _iter_thread_comments(
    children: list[dict[str, Any]],
    *,
    max_comments: int,
    max_depth: int,
) -> Iterator[tuple[dict[str, Any], int]]:
    if max_comments == 0 or max_depth == 0:
        return
    stack = [(child, 0) for child in reversed(children)]
    yielded = 0
    while stack and yielded < max_comments:
        comment, depth = stack.pop()
        if not isinstance(comment, dict):
            continue
        text = _clean_html(comment.get("text") or "")
        if text and not comment.get("deleted") and not comment.get("dead"):
            yield comment, depth
            yielded += 1
        if depth + 1 < max_depth:
            stack.extend((child, depth + 1) for child in reversed(comment.get("children") or []))


def _deleted_row(row_id: str) -> dict[str, Any]:
    return {"id": row_id, "_deleted": True}


def _row_fingerprint(row: dict[str, Any]) -> str:
    payload = {key: value for key, value in row.items() if key != "_deleted"}
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _clean_html(value: Any) -> str:
    if value is None:
        return ""
    text = str(value)
    text = _BREAK_RE.sub("\n", text)
    text = _TAG_RE.sub("", text)
    text = html.unescape(text)
    text = _WS_RE.sub(" ", text)
    lines = [line.strip() for line in text.splitlines()]
    collapsed: list[str] = []
    for line in lines:
        if line or (collapsed and collapsed[-1]):
            collapsed.append(line)
    return "\n".join(collapsed).strip()


def _format_datetime(timestamp: Any) -> str:
    if isinstance(timestamp, str) and not timestamp.strip().lstrip("-").isdigit():
        try:
            parsed = datetime.fromisoformat(timestamp.strip().replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=UTC)
            return parsed.astimezone(UTC).isoformat().replace("+00:00", "Z")
        except ValueError:
            return "unknown"
    try:
        return datetime.fromtimestamp(int(timestamp), tz=UTC).isoformat().replace("+00:00", "Z")
    except (TypeError, ValueError, OverflowError, OSError):
        return "unknown"


def _as_int(value: Any, *, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _numeric_sort_key(value: str) -> tuple[int, int | str]:
    try:
        return 0, int(value)
    except ValueError:
        return 1, value


class _RequestsClient:
    """Small retry-compatible HTTP transport used by the public factory."""

    def __init__(self) -> None:
        try:
            import requests
        except ImportError as exc:
            raise ImportError(_EXTRA_HINT) from exc
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": "cognee-community-connector-hacker-news/0.1.0"})

    def get_json(self, url: str, params: dict | None = None) -> Any:
        response = self._session.get(url, params=params, timeout=_HTTP_TIMEOUT)
        if response.status_code in (404, 410):
            return None
        response.raise_for_status()
        return response.json()


def _with_retry(operation, *, what: str):
    for attempt in range(_MAX_RETRIES):
        try:
            return operation()
        except Exception as exc:
            if attempt == _MAX_RETRIES - 1 or not _is_transient(exc):
                raise
            delay = _retry_delay(exc, attempt)
            logger.warning(
                "Hacker News: %s; retrying %s in %.1fs (%d/%d).",
                exc,
                what,
                delay,
                attempt + 1,
                _MAX_RETRIES,
            )
            time.sleep(delay)
    raise AssertionError("unreachable")  # pragma: no cover


def _is_transient(exc: Exception) -> bool:
    try:
        import requests
    except ImportError:
        return False
    if isinstance(exc, (requests.Timeout, requests.ConnectionError)):
        return True
    if isinstance(exc, requests.HTTPError):
        status = exc.response.status_code if exc.response is not None else None
        return status in (429, 500, 502, 503, 504)
    return False


def _retry_delay(exc: Exception, attempt: int) -> float:
    headers = getattr(getattr(exc, "response", None), "headers", None) or {}
    header = headers.get("retry-after") or headers.get("Retry-After")
    try:
        return float(header)
    except (TypeError, ValueError):
        return float(2**attempt)
