"""拼图状态变更事件。

每批清单提交时，在**同一写事务**内比较受影响的现役拼图在批前/批后的
可交付结论与阻断路径，把净变化（含新出现）落库为持久事件：

- 事件与清单一同原子提交：任一步失败整体回滚，不留部分事件或部分修订；
  冲突拒收、完全重传在事件逻辑之前返回，天然零事件。
- 同批对同一拼图的多项修改只记最终结果：受影响集合按成果号去重，
  批后统一评估一次，每个拼图每批至多一条事件。
- 事件落库后不改写：blockers 为当时状态的 JSON 快照，后续谱系变化
  只产生新事件，不回改历史。

游标即 event_id（AUTOINCREMENT，单调递增且重启不回退）：
客户端记录已领取的最大 event_id，断线/重启后用它续领，
`event_id > cursor` 严格大于 + 升序分页保证不漏领、不重领。
"""

from __future__ import annotations

import json
import sqlite3
from typing import Any, Optional

from . import database as db
from . import lineage

EVENT_APPEARED = "appeared"  # 新出现的现役拼图
EVENT_CHANGED = "changed"    # 结论或阻断路径发生净变化


# ---------------------------------------------------------------------------
# 受影响集合与状态快照（须在写事务/快照事务内调用）
# ---------------------------------------------------------------------------

def collect_affected_product_ids(
    conn: sqlite3.Connection, new_items: list[dict[str, Any]]
) -> set[str]:
    """本批新条目可能波及的全部成果号。

    = 新条目自身的成果号 ∪ 沿固定父引用**反向**依赖这些成果号的全部成果号
    （传递闭包，例如瓦片升级会波及引用它的各级拼图）。

    闭包基于插入前的 revisions 表计算是完备的：本批新引入的引用边只能来自
    本批新条目，其起点成果号已包含在提交集合内。
    """
    affected = {item["product_id"] for item in new_items}
    frontier = set(affected)
    while frontier:
        marks = ",".join("?" for _ in frontier)
        rows = conn.execute(
            f"SELECT DISTINCT product_id FROM revisions "
            f"WHERE parent_product_id IN ({marks})",
            tuple(frontier),
        ).fetchall()
        new_ids = {row["product_id"] for row in rows} - affected
        affected |= new_ids
        frontier = new_ids
    return affected


def _active_mosaic_state(
    conn: sqlite3.Connection, product_id: str
) -> Optional[dict[str, Any]]:
    """该成果号当前现役修订是拼图时，返回其规范化状态；否则 None。"""
    row = db.get_active_revision(conn, product_id)
    if row is None or row["parent_product_id"] is None:
        return None  # 不存在或现役为原始瓦片：不属于拼图状态事件范畴
    result = lineage.evaluate(conn, row, start_is_active=True)
    return {
        "revision": result["revision"],
        "deliverable": result["deliverable"],
        "blockers": result["blockers"],
    }


def snapshot_states(
    conn: sqlite3.Connection, product_ids: set[str]
) -> dict[str, dict[str, Any]]:
    """批前状态快照：{成果号: 状态}，仅含现役为拼图的成果号。"""
    states: dict[str, dict[str, Any]] = {}
    for pid in product_ids:
        state = _active_mosaic_state(conn, pid)
        if state is not None:
            states[pid] = state
    return states


def _same_conclusion(a: dict[str, Any], b: dict[str, Any]) -> bool:
    """净变化判定：可交付结论 + 阻断路径（含原因与说明）完全一致视为无变化。

    修订号本身的变化不算净变化——下游缓存的是结论，结论未变无需刷新。
    """
    return a["deliverable"] == b["deliverable"] and a["blockers"] == b["blockers"]


def record_events(
    conn: sqlite3.Connection,
    batch_id: int,
    affected_ids: set[str],
    before_states: dict[str, dict[str, Any]],
    occurred_at: str,
) -> int:
    """批后逐成果比较并写入事件，返回事件数。须在写事务内调用。"""
    written = 0
    for pid in sorted(affected_ids):
        after = _active_mosaic_state(conn, pid)
        if after is None:
            continue  # 批后现役非拼图（瓦片/不存在）：不产生事件
        before = before_states.get(pid)
        if before is not None and _same_conclusion(before, after):
            continue  # 结论与阻断路径无净变化
        event_type = EVENT_APPEARED if before is None else EVENT_CHANGED
        conn.execute(
            "INSERT INTO mosaic_events (batch_id, product_id, revision, event_type, "
            "deliverable, blockers, occurred_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                batch_id,
                pid,
                after["revision"],
                event_type,
                1 if after["deliverable"] else 0,
                json.dumps(after["blockers"], ensure_ascii=False),
                occurred_at,
            ),
        )
        written += 1
    return written


# ---------------------------------------------------------------------------
# 游标分页领取（须在快照事务内调用）
# ---------------------------------------------------------------------------

def list_events(
    conn: sqlite3.Connection, cursor: int, limit: int
) -> tuple[list[sqlite3.Row], bool]:
    """领取 event_id > cursor 的最早 limit 条；多取一条探测是否还有后续。"""
    rows = list(
        conn.execute(
            "SELECT * FROM mosaic_events WHERE event_id > ? "
            "ORDER BY event_id LIMIT ?",
            (cursor, limit + 1),
        )
    )
    has_more = len(rows) > limit
    return rows[:limit], has_more


def row_to_event_dict(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "event_id": row["event_id"],
        "batch_id": row["batch_id"],
        "product_id": row["product_id"],
        "revision": row["revision"],
        "event_type": row["event_type"],
        "deliverable": bool(row["deliverable"]),
        "blockers": json.loads(row["blockers"]),
        "occurred_at": row["occurred_at"],
    }
