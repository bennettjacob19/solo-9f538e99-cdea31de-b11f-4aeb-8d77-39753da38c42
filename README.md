# PostgreSQL WAL 保留边界演练 API

用 FastAPI + SQLite 模拟 PostgreSQL 的 WAL 段回收决策：给定当前 WAL 段、物理备库回放
位置、备份任务固定起点和复制槽状态，计算**可安全回收的最高完整 WAL 段**，并以乐观
并发控制把边界持久化（边界只能前进）。

## 领域规则

一个 WAL 段是固定大小（默认 16 MiB）的连续 LSN 区间。只有**整段**都低于所有保护点
的段才能回收。保护点：

| 保护点 | 含义 |
| --- | --- |
| 当前正在写的段 | 截止 LSN 不得超过 `current_segment_start_lsn` |
| 物理备库 `replayed_lsn` | 取所有备库中**最小**（最慢/滞后备库决定上限） |
| 备份任务 `backup_start_lsn` | 备份固定 pin，回放到该点之前的 WAL 都要保留 |
| 活跃复制槽 `restart_lsn` | 槽保留 WAL 的最老位置（NULL 表示不 pin） |
| **失效（`active=false`）槽** | 列为 `blockers`，整份快照不可回收，**不能忽略** |

截止 LSN = 所有保护点 LSN 的最小值；最高可回收完整段 =
`(cutoff // segment_size - 1) * segment_size`（截止点恰好落在段边界时，该段仍被
占用，需再退一段；没有完整段时返回 `reclaimable=false`）。

## 并发与持久化语义

- 同一快照（规范化后做 SHA-256，元素顺序无关、整数与 `X/Y` 文本 LSN 等价）重试
  始终返回**同一个 plan**，`snapshot_hash` 上有 UNIQUE 约束兜底。
- 提交带 `expected_version`：与当前版本不符返回 `409 stale_version`，状态不变。
- 边界**只能前进**：过期/回退计划提交时为 no-op，版本号不增加。
- 版本检查、单调性检查与更新在单个 `BEGIN IMMEDIATE` 事务内原子完成；写入失败
  一律 ROLLBACK，旧边界保留。
- 计划、当前边界、边界推进日志全部落在 SQLite；进程重启后原样恢复（已自测）。

## API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/reclaim` | 提交保护点快照，返回计划（201；相同快照幂等返回同一计划） |
| POST | `/plans/{plan_id}/commit` | 带 `expected_version` 提交；409=版本过期 |
| GET | `/plans` | 查询历史计划（含是否已提交、提交版本） |
| GET | `/plans/{plan_id}` | 查询单个计划 |
| GET | `/boundary` | 当前已提交边界与版本 |
| GET | `/health` | 健康检查 |

LSN 可传 uint64 整数或 PostgreSQL 风格十六进制字符串（如 `"0/03000000"`），响应中
两种形式都会给出，并附段文件名（如 `000000010000000000000002`）。

### 请求示例

```json
POST /reclaim
{
  "current_segment_start_lsn": 50331648,
  "current_segment_end_lsn": 67108864,
  "backup_start_lsn": 67108864,
  "segment_size": 16777216,
  "standbys": [
    {"name": "fast", "replayed_lsn": 83886080},
    {"name": "laggard", "replayed_lsn": 16781312}
  ],
  "slots": [
    {"name": "slot_a", "active": true, "restart_lsn": 50331648},
    {"name": "dead_slot", "active": false, "restart_lsn": 16777216}
  ]
}
```

存在失效槽时响应：

```json
{
  "reclaimable": false,
  "highest_reclaimable_segment_start_lsn": null,
  "blockers": [
    {"type": "invalid_slot", "name": "dead_slot", "detail": "..."}
  ]
}
```

提交：`POST /plans/{id}/commit`，body `{"expected_version": 0}`。

## 本地运行

```bash
pip install -r requirements.txt
WAL_REHEARSAL_DB=./data/wal_retention.db uvicorn app.main:app --reload
# 自测（无需运行服务，使用 FastAPI TestClient + 临时库）
python -m pytest tests/ -q
# 对已运行服务的端到端冒烟（仅用标准库）
python scripts/smoke_test.py http://127.0.0.1:8000
```

## Docker Compose

```bash
docker compose up --build -d
curl http://127.0.0.1:8000/health
docker compose logs -f
```

SQLite 数据保存在命名卷 `wal_retention_data`（容器内 `/data`），`docker compose
down` 不会删除卷；如需清空：`docker compose down -v`。

## 目录结构

```
app/
  lsn.py         # LSN 解析/格式化、段对齐与最高完整段计算（纯函数）
  retention.py   # 保护点汇总、失效槽阻塞、快照规范化哈希（纯函数）
  schemas.py     # Pydantic 请求/响应模型
  storage.py     # SQLite：plans 表、边界 meta、boundary_log、原子提交
  main.py        # FastAPI 路由
tests/           # 29 个 pytest 用例（纯逻辑 + API + 重启/写失败/并发）
scripts/smoke_test.py  # 对真实 HTTP 服务的标准库冒烟脚本
```
