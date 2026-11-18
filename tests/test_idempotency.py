import time
import pytest
from fastapi.testclient import TestClient

from src.main import app
from src.routers.comments import controller

client = TestClient(app)


@pytest.fixture(autouse=True)
def reset_controller_state():
    """Clears in-memory store and secondary indexes before each test for clean isolation."""
    controller._store.clear()
    controller._post_index.clear()
    controller._user_index.clear()
    controller._reply_index.clear()
    yield


def test_health_endpoint():
    """Verifies load balancer health check endpoint."""
    res = client.get("/health")
    assert res.status_code == 200
    data = res.json()
    assert data["status"] == "healthy"
    assert "node_id" in data
    assert "region" in data


def test_post_creation_is_not_idempotent():
    """Proves POST /posts/{id}/comments mints a new resource on every retry (201 Created)."""
    payload = {"user_id": 101, "content": "Hello distributed world!"}

    r1 = client.post("/api/v1/posts/10/comments", json=payload)
    r2 = client.post("/api/v1/posts/10/comments", json=payload)

    assert r1.status_code == 201
    assert r2.status_code == 201

    c1, c2 = r1.json(), r2.json()
    assert c1["comment_id"] != c2["comment_id"]
    assert c2["comment_id"] > c1["comment_id"]
    assert len(client.get("/api/v1/posts/10/comments").json()) == 2


def test_put_full_replacement_is_idempotent_and_updates_user_index():
    """Proves repeating PUT 10 times produces identical state and updates secondary user indexes."""
    create_res = client.post(
        "/api/v1/posts/20/comments",
        json={"user_id": 1, "content": "Initial draft"},
    )
    cid = create_res.json()["comment_id"]
    original_ts = create_res.json()["created_at_ms"]

    replace_payload = {
        "user_id": 2,  # Reassign ownership from user 1 to user 2
        "content": "Final replaced content",
        "is_pinned": True,
    }

    responses = [
        client.put(f"/api/v1/comments/{cid}", json=replace_payload)
        for _ in range(10)
    ]

    for res in responses:
        assert res.status_code == 200
        body = res.json()
        assert body["comment_id"] == cid
        assert body["post_id"] == 20
        assert body["created_at_ms"] == original_ts
        assert body["user_id"] == 2
        assert body["content"] == "Final replaced content"
        assert body["is_pinned"] is True

    # Verify secondary _user_index moved comment from user 1 to user 2
    assert client.get("/api/v1/comments?user_id=1").json() == []
    user2_comments = client.get("/api/v1/comments?user_id=2").json()
    assert len(user2_comments) == 1
    assert user2_comments[0]["comment_id"] == cid


def test_patch_partial_update_is_idempotent():
    """Proves PATCH modifies only sent fields without erasing omitted attributes."""
    create_res = client.post(
        "/api/v1/posts/30/comments",
        json={"user_id": 5, "content": "Preserve this text"},
    )
    cid = create_res.json()["comment_id"]

    # Toggle only is_pinned five times
    for _ in range(5):
        patch_res = client.patch(f"/api/v1/comments/{cid}", json={"is_pinned": True})
        assert patch_res.status_code == 200
        assert patch_res.json()["is_pinned"] is True
        assert patch_res.json()["content"] == "Preserve this text"


def test_tombstone_delete_and_404_semantics():
    """Proves O(1) tombstone delete returns 204 once, then 404 on subsequent reads/mutations."""
    create_res = client.post(
        "/api/v1/posts/40/comments",
        json={"user_id": 9, "content": "To be deleted"},
    )
    cid = create_res.json()["comment_id"]

    # 1st delete -> 204 No Content
    del_1 = client.delete(f"/api/v1/comments/{cid}")
    assert del_1.status_code == 204

    # 2nd delete -> 404 Not Found
    del_2 = client.delete(f"/api/v1/comments/{cid}")
    assert del_2.status_code == 404

    # Direct GET, PUT, PATCH, and replies on deleted tombstone all return 404
    assert client.get(f"/api/v1/comments/{cid}").status_code == 404
    assert client.put(
        f"/api/v1/comments/{cid}",
        json={"user_id": 9, "content": "x", "is_pinned": False},
    ).status_code == 404
    assert client.patch(f"/api/v1/comments/{cid}", json={"is_pinned": True}).status_code == 404
    assert client.post(
        f"/api/v1/comments/{cid}/replies",
        json={"user_id": 9, "content": "Reply to ghost"},
    ).status_code == 404

    # Paginated post index automatically skips the tombstone ID
    assert client.get("/api/v1/posts/40/comments").json() == []


def test_nested_replies_and_query_filtering():
    """Verifies reply thread inheritance and multi-attribute query planner filtering."""
    parent = client.post(
        "/api/v1/posts/50/comments",
        json={"user_id": 10, "content": "Parent comment"},
    ).json()
    parent_id = parent["comment_id"]

    reply = client.post(
        f"/api/v1/comments/{parent_id}/replies",
        json={"user_id": 20, "content": "Nested reply"},
    )
    assert reply.status_code == 201
    reply_body = reply.json()
    assert reply_body["post_id"] == 50
    assert reply_body["parent_comment_id"] == parent_id

    replies_list = client.get(f"/api/v1/comments/{parent_id}/replies").json()
    assert len(replies_list) == 1
    assert replies_list[0]["comment_id"] == reply_body["comment_id"]

    # Test query planner with both post_id and user_id
    filtered = client.get("/api/v1/comments?post_id=50&user_id=10").json()
    assert len(filtered) == 1
    assert filtered[0]["comment_id"] == parent_id


def test_pydantic_and_query_validation_422_guards():
    """Verifies invalid JSON payloads and out-of-range query limits are rejected with 422."""
    # Empty content string violates Field(min_length=1)
    bad_post = client.post("/api/v1/posts/1/comments", json={"user_id": 1, "content": ""})
    assert bad_post.status_code == 422

    # limit > 100 violates Query(le=100)
    bad_limit = client.get("/api/v1/posts/1/comments?limit=5000")
    assert bad_limit.status_code == 422


def test_one_million_comments_cursor_benchmark():
    """
    Seeds 1,000,000 comments onto a single post and benchmarks O(log N) bisect cursor
    pagination jumping straight to comment #500,000 in both ASC and DESC orders.
    """
    post_id = 999_999
    base_id = 500_000_000_000

    # Fast batch-seed of 1,000,000 chronological records
    store = controller._store
    post_idx = controller._post_index[post_id]
    for i in range(1_000_000):
        cid = base_id + i
        store[cid] = {
            "comment_id": cid,
            "post_id": post_id,
            "parent_comment_id": None,
            "user_id": 1,
            "content": "1M scale benchmark",
            "is_pinned": False,
            "created_at_ms": 1704067200000 + i,
        }
        post_idx.append(cid)

    mid_cursor = base_id + 500_000

    # 1. Benchmark ASC cursor seek at depth 500,000
    t0 = time.perf_counter()
    asc_res = client.get(
        f"/api/v1/posts/{post_id}/comments?cursor_id={mid_cursor}&sort=asc&limit=20"
    )
    asc_ms = (time.perf_counter() - t0) * 1000

    assert asc_res.status_code == 200
    asc_page = asc_res.json()
    assert len(asc_page) == 20
    assert asc_page[0]["comment_id"] == mid_cursor + 1
    assert asc_page[-1]["comment_id"] == mid_cursor + 20

    # 2. Benchmark DESC cursor seek at depth 500,000
    t1 = time.perf_counter()
    desc_res = client.get(
        f"/api/v1/posts/{post_id}/comments?cursor_id={mid_cursor}&sort=desc&limit=20"
    )
    desc_ms = (time.perf_counter() - t1) * 1000

    assert desc_res.status_code == 200
    desc_page = desc_res.json()
    assert len(desc_page) == 20
    assert desc_page[0]["comment_id"] == mid_cursor - 1
    assert desc_page[-1]["comment_id"] == mid_cursor - 20

    print(
        f"\n[1M BENCHMARK] Depth=500,000 | ASC: {asc_ms:.3f} ms | DESC: {desc_ms:.3f} ms"
    )
    assert asc_ms < 15.0
    assert desc_ms < 15.0