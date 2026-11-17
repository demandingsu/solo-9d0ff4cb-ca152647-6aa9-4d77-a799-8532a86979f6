"""拼图状态变更事件 API 的端到端自测。

覆盖：事件生成（新出现/净变化/下游波及）、同批多项修改只记最终结果、
失败与完全重传零事件、游标分页不漏不重、断线与重启续领、历史事件不改写、
并发提交与翻页安全。

运行：pytest -q tests/test_events.py
"""

from __future__ import annotations

import os
import tempfile
import threading

import pytest
from fastapi.testclient import TestClient

from app.main import create_app


def _item(product_id, revision, summary, parent=None):
    payload = {"product_id": product_id, "revision": revision, "summary": summary}
    if parent is not None:
        payload["parent_product_id"] = parent[0]
        payload["parent_revision"] = parent[1]
    return payload


@pytest.fixture()
def client(tmp_path):
    app = create_app(str(tmp_path / "test.db"))
    with TestClient(app) as c:
        yield c


def _post(client, items):
    return client.post("/api/batches", json={"items": items})


def _fetch_all(client, limit=100):
    """从游标 0 翻页领完全部事件，返回 (事件列表, 逐页游标轨迹)。"""
    events, cursor, trail = [], 0, []
    while True:
        page = client.get(
            "/api/mosaic-events", params={"cursor": cursor, "limit": limit}
        ).json()
        events.extend(page["events"])
        cursor = page["next_cursor"]
        trail.append(cursor)
        if not page["has_more"]:
            return events, trail


# ---------------------------------------------------------------------------
# 1. 事件生成：新出现与净变化
# ---------------------------------------------------------------------------

def test_new_mosaic_produces_appeared_event(client):
    _post(client, [_item("M-1", 1, "拼图", parent=("R-1", 1))])
    events, _ = _fetch_all(client)
    assert len(events) == 1
    e = events[0]
    assert e["event_type"] == "appeared"
    assert e["product_id"] == "M-1" and e["revision"] == 1
    assert e["deliverable"] is False
    assert e["blockers"][0]["reason"] == "missing"
    assert e["blockers"][0]["path"] == ["M-1@1", "R-1@1"]
    assert e["batch_id"] == 1 and e["occurred_at"]


def test_tile_submission_produces_no_event(client):
    _post(client, [_item("R-1", 1, "原始瓦片")])
    events, _ = _fetch_all(client)
    assert events == []


def test_late_parent_produces_changed_event(client):
    _post(client, [_item("M-1", 1, "拼图", parent=("R-1", 1))])
    _post(client, [_item("R-1", 1, "晚到的瓦片")])
    events, _ = _fetch_all(client)
    assert [e["event_type"] for e in events] == ["appeared", "changed"]
    assert events[1]["deliverable"] is True and events[1]["blockers"] == []


def test_upstream_revision_notifies_downstream_mosaic(client):
    # M 不在本批提交清单里，但其结论被 R 的新修订波及 → 也记事件
    _post(client, [_item("R", 1, "瓦片v1"), _item("M", 1, "拼图", parent=("R", 1))])
    _post(client, [_item("R", 2, "瓦片v2")])
    events, _ = _fetch_all(client)
    assert len(events) == 2  # 第一批 1 条（M 出现），第二批 1 条（M 受阻）
    e = events[1]
    assert e["event_type"] == "changed" and e["product_id"] == "M"
    assert e["deliverable"] is False
    assert e["blockers"][0]["reason"] == "superseded"
    assert e["blockers"][0]["path"] == ["M@1", "R@1"]


def test_no_net_change_produces_no_event(client):
    _post(client, [_item("R", 1, "瓦片"), _item("M", 1, "拼图", parent=("R", 1))])
    # 结论不变的修订（仍可交付）与无关成果提交都不产生事件
    _post(client, [_item("M", 2, "拼图v2同结论", parent=("R", 1))])
    _post(client, [_item("OTHER", 1, "无关瓦片")])
    events, _ = _fetch_all(client)
    assert len(events) == 1 and events[0]["event_type"] == "appeared"


# ---------------------------------------------------------------------------
# 2. 同批多项修改只记最终结果
# ---------------------------------------------------------------------------

def test_same_batch_multiple_changes_record_final_state_only(client):
    # 同批：瓦片连升两级 + 拼图引用旧级 → 拼图只记一条最终状态事件
    _post(client, [
        _item("R", 1, "瓦片v1"),
        _item("R", 2, "瓦片v2"),
        _item("M", 1, "拼图", parent=("R", 1)),
    ])
    events, _ = _fetch_all(client)
    assert len(events) == 1
    e = events[0]
    assert e["product_id"] == "M" and e["event_type"] == "appeared"
    assert e["deliverable"] is False
    assert e["blockers"][0]["reason"] == "superseded"
    assert "R@2" in e["blockers"][0]["detail"]  # 最终状态参照现役 R@2


def test_same_batch_multiple_revisions_of_same_mosaic_single_event(client):
    _post(client, [_item("R", 1, "瓦片")])
    _post(client, [
        _item("M", 1, "拼图v1", parent=("R", 1)),
        _item("M", 2, "拼图v2", parent=("R", 1)),
    ])
    events, _ = _fetch_all(client)
    assert len(events) == 1
    assert events[0]["revision"] == 2 and events[0]["deliverable"] is True


# ---------------------------------------------------------------------------
# 3. 失败与完全重传：零事件，不留部分数据
# ---------------------------------------------------------------------------

def test_rejected_batch_writes_no_events_and_no_revisions(client):
    _post(client, [_item("R", 1, "原始摘要"), _item("M", 1, "拼图", parent=("R", 1))])
    before, _ = _fetch_all(client)
    assert len(before) == 1

    r = _post(client, [
        _item("R", 1, "被篡改的摘要"),      # 与库内冲突 → 整批拒收
        _item("M2", 1, "本不应入库", parent=("R", 1)),
    ])
    assert r.json()["status"] == "rejected"

    after, _ = _fetch_all(client)
    assert after == before  # 事件零新增
    assert client.get("/api/products/M2/status").status_code == 404  # 修订零写入


def test_duplicate_batch_writes_no_events(client):
    payload = [_item("R", 1, "瓦片"), _item("M", 1, "拼图", parent=("R", 1))]
    _post(client, payload)
    before, _ = _fetch_all(client)
    r = _post(client, payload)
    assert r.json()["status"] == "duplicate"
    after, _ = _fetch_all(client)
    assert after == before


def test_partially_identical_batch_events_only_for_real_changes(client):
    # 同批中：R@1 与库内完全一致（幂等跳过），M@1 是新条目 → 只为 M 记事件
    _post(client, [_item("R", 1, "瓦片")])
    r = _post(client, [
        _item("R", 1, "瓦片"),
        _item("M", 1, "拼图", parent=("R", 1)),
    ])
    body = r.json()
    assert body["status"] == "accepted" and body["inserted_count"] == 1
    events, _ = _fetch_all(client)
    assert len(events) == 1
    assert events[0]["product_id"] == "M" and events[0]["event_type"] == "appeared"


# ---------------------------------------------------------------------------
# 4. 游标分页：不漏领、不重领
# ---------------------------------------------------------------------------

def test_cursor_pagination_no_gap_no_duplicate(client):
    # 制造 6 个事件：M1 出现 → M1 恢复 → M2 出现 → R 升级使 M1、M2 同时受阻 → M3 出现
    _post(client, [_item("M1", 1, "拼图1", parent=("R", 1))])
    _post(client, [_item("R", 1, "瓦片")])
    _post(client, [_item("M2", 1, "拼图2", parent=("R", 1))])
    _post(client, [_item("R", 2, "瓦片v2")])
    _post(client, [_item("M3", 1, "拼图3", parent=("GHOST", 1))])

    collected, cursor, pages = [], 0, 0
    while True:
        page = client.get(
            "/api/mosaic-events", params={"cursor": cursor, "limit": 2}
        ).json()
        collected.extend(page["events"])
        cursor = page["next_cursor"]
        pages += 1
        if not page["has_more"]:
            break
        assert pages < 10  # 防止死循环

    ids = [e["event_id"] for e in collected]
    assert ids == [1, 2, 3, 4, 5, 6]  # 恰好一次、严格递增、无空洞重复
    assert [e["product_id"] for e in collected] == ["M1", "M1", "M2", "M1", "M2", "M3"]
    assert pages == 3  # 2+2+2

    # 游标越界后为空页：游标保持不变，has_more=false（客户端可原地轮询）
    page = client.get(
        "/api/mosaic-events", params={"cursor": cursor, "limit": 2}
    ).json()
    assert page["events"] == [] and page["next_cursor"] == cursor
    assert page["has_more"] is False


def test_cursor_and_limit_validation(client):
    assert client.get("/api/mosaic-events", params={"cursor": -1}).status_code == 422
    assert client.get("/api/mosaic-events", params={"limit": 0}).status_code == 422


# ---------------------------------------------------------------------------
# 5. 断线与服务重启：从原游标续领
# ---------------------------------------------------------------------------

def test_resume_from_saved_cursor_after_restart(tmp_path):
    db_path = str(tmp_path / "restart.db")
    app1 = create_app(db_path)
    with TestClient(app1) as c1:
        _post(c1, [_item("M", 1, "拼图", parent=("R", 1))])
        page1 = c1.get("/api/mosaic-events", params={"limit": 10}).json()
        assert len(page1["events"]) == 1
        saved_cursor = page1["next_cursor"]  # 客户端持久化游标后“断线”

    # 模拟服务重启：同一数据库文件新建应用实例
    app2 = create_app(db_path)
    with TestClient(app2) as c2:
        _post(c2, [_item("R", 1, "瓦片")])  # 重启后新提交产生新事件
        page2 = c2.get(
            "/api/mosaic-events", params={"cursor": saved_cursor, "limit": 10}
        ).json()
        # 从原游标续领：只拿到新事件，不重领重启前的
        assert [e["event_type"] for e in page2["events"]] == ["changed"]
        assert page2["events"][0]["deliverable"] is True
        assert page2["events"][0]["event_id"] > saved_cursor


# ---------------------------------------------------------------------------
# 6. 历史事件不改写；原状态查询仍返回当前结论
# ---------------------------------------------------------------------------

def test_historical_events_are_immutable(client):
    _post(client, [_item("M", 1, "拼图", parent=("R", 1))])
    first, _ = _fetch_all(client)
    assert first[0]["deliverable"] is False

    _post(client, [_item("R", 1, "瓦片")])       # 谱系变化：M 转为可交付
    _post(client, [_item("R", 2, "瓦片v2")])     # 再次变化：M 重新受阻

    replay, _ = _fetch_all(client)
    # 历史事件逐条保持当时状态，不随最新谱系改写
    assert replay[0] == first[0]
    assert replay[0]["deliverable"] is False
    assert replay[1]["deliverable"] is True
    assert replay[2]["deliverable"] is False
    assert replay[2]["blockers"][0]["reason"] == "superseded"

    # 原状态查询仍返回当前结论
    current = client.get("/api/products/M/status").json()
    assert current["deliverable"] is False
    assert current["blockers"][0]["reason"] == "superseded"


# ---------------------------------------------------------------------------
# 7. 并发提交与翻页：不漏领、不重领
# ---------------------------------------------------------------------------

def test_concurrent_submits_and_pagination(tmp_path):
    db_path = str(tmp_path / "events-conc.db")
    app = create_app(db_path)
    n = 12
    errors: list[Exception] = []

    def worker(i: int):
        try:
            with TestClient(app) as c:
                r = c.post("/api/batches", json={"items": [
                    _item(f"M{i}", 1, f"拼图{i}", parent=("GHOST", 1))
                ]})
                assert r.status_code == 201 and r.json()["status"] == "accepted"
        except Exception as exc:  # pragma: no cover
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()

    # 提交进行的同时开始翻页（游标式读取与并发写入交错）
    collected: list[int] = []
    cursor = 0
    with TestClient(app) as c:
        for t in threads:
            t.join()
        assert errors == []
        # 全部提交完成后继续翻页至末尾
        while True:
            page = c.get(
                "/api/mosaic-events", params={"cursor": cursor, "limit": 5}
            ).json()
            collected.extend(e["event_id"] for e in page["events"])
            cursor = page["next_cursor"]
            if not page["has_more"]:
                break

    # 12 个并发批次各产生 1 条事件：收齐且无重复、严格递增
    assert len(collected) == n
    assert len(set(collected)) == n
    assert collected == sorted(collected)


def test_event_ids_survive_rollback_without_reuse(client):
    # 被拒批次不产生事件；后续事件 id 单调递增即可（空洞不影响续领）
    _post(client, [_item("M", 1, "拼图", parent=("R", 1))])
    _post(client, [_item("M", 1, "冲突摘要", parent=("R", 1)), _item("X", 1, "X")])
    _post(client, [_item("R", 1, "瓦片")])
    events, _ = _fetch_all(client)
    ids = [e["event_id"] for e in events]
    assert ids == sorted(ids) and len(set(ids)) == len(ids)
    assert [e["event_type"] for e in events] == ["appeared", "changed"]
