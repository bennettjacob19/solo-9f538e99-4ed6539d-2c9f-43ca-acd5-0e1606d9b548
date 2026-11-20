# PostgreSQL 演练台

值班演练台，两个基于**真实数据库会话**的演练：

1. **行锁演练（`/`）**：当一个 `UPDATE` 被 PostgreSQL 行锁挂住时，从网页上找出
   真实的持锁者，并只解除本演练造成的这一次阻塞。
2. **同步提交演练（`/sync`）**：在一主一物理备库上，暂停备库 WAL 回放，用
   `synchronous_commit = remote_apply` 提交部署单；确认事务正在等待同步复制后
   取消等待，再恢复回放按单号核对两库实况。

## 同步提交演练

### 操作流程

1. **暂停备库 WAL 回放**：页面调用备库的 `pg_wal_replay_pause()`，并轮询
   `pg_is_wal_replay_paused()` 确认暂停真正生效后才开放提交。
2. **提交部署单**：独立会话执行
   `SET synchronous_commit TO remote_apply` → `INSERT INTO deploy_order ...` →
   `COMMIT`。回放暂停时，COMMIT 会阻塞在同步复制等待
   （`pg_stat_activity.wait_event_type = IPC`、`wait_event = SyncRep`）。
3. **确认后才能取消**：只有页面从数据库确认该会话处于 IPC/SyncRep 等待时，
   「取消等待」按钮才可用。取消由一条原子 SQL 完成——同时校验
   `pid + backend_start + application_name` 且仍处于 SyncRep 等待，才执行
   `pg_terminate_backend`，PID 复用或状态变化都不会误杀。
4. **取消后的事实判定**：提交记录已在主库持久化，因此
   - 主库可查到该单号，备库在回放暂停期间查不到；
   - 恢复回放并追平后，两边均可查到同一单号；
   - 本次提交结果始终显示「取消等待」，不会因为后来追平而改变。
5. **正常提交**：另一笔单在回放运行时提交，remote_apply 等备库回放完成后正常
   返回，结果显示「已同步确认」。
6. **重启可核对**：每轮演练（单号、令牌、提交结果、两库核对结果）落盘到
   `SYNC_STATE_FILE`。Web 重启后仍可按单号实时查询主库与备库，并显示原提交结果。
7. **无法确认项明确标出**：任一数据库查询失败、或未配置/无法连接备库时，
   对应核对项标记「无法确认」并附带错误；所有写操作都需要页面令牌，
   缺少或伪造凭据返回 403。
8. 等待期间行锁演练（`/`）可并行使用，互不影响。

### 安全与清理

- 启动时清理上一进程遗留的 `lockdrill:sync:commit:*` 会话，避免无人值守的
  remote_apply 永久等待；同时自动恢复可能遗留的回放暂停。
- 新一轮演练、服务关闭都会恢复备库 WAL 回放，绝不把备库留在暂停状态。
- 复制槽 `standby1_slot` 保证备库停机期间主库保留 WAL。

## 行锁演练

- **真实数据库会话**：每轮演练在 PostgreSQL 中创建两个连接
  - 持锁会话：`BEGIN` 后执行 `SELECT ... FOR UPDATE` 锁住演练行（`idle in transaction`）；
  - 受阻更新：另一连接执行 `UPDATE lock_drill_row …`，被行锁挂住。
- **页面展示**：两端 PID、`backend_start`、事务开始时间、当前状态、
  `wait_event_type/wait_event`、`pg_blocking_pids()` 给出的实际阻塞者、
  双方持有的锁及阻塞关系明细表。
- **安全解除**：仅在「当前演练状态为 blocked，且数据库刚确认
  `holder_pid ∈ pg_blocking_pids(waiter_pid)`」时开放按钮；提交时用单条 SQL
  原子复核会话身份与阻塞关系，复核通过才 `pg_terminate_backend`。
- **以真实结果判定**：解锁后等待受阻连接真实执行 `COMMIT`；判定只看
  `UPDATE ... RETURNING` + `COMMIT` 的结果，并用独立连接复核行值。
- **清理**：结束本轮演练、开始新一轮、服务关闭都会按身份三元组终止本演练的会话。

## 运行（Docker Compose）

```bash
docker compose up --build
# 打开 http://localhost:8000        行锁演练
#      http://localhost:8000/sync   同步提交演练
```

`docker compose` 启动三个容器：

| 服务 | 说明 |
| --- | --- |
| `db-primary` | PostgreSQL 16 主库，创建复制角色 `replicator` 与物理复制槽 `standby1_slot`，`synchronous_standby_names = ANY 1 (standby1)` |
| `db-standby` | 首次启动用 `pg_basebackup` 自动搭建成物理流备库（幂等，卷复用直接启动） |
| `web` | FastAPI 应用，固定单 worker（演练会话保存在进程内，演练结果落盘到 `/srv/var`） |

> 备库重建（删除 `pgstandby` 卷）期间主库的同步事务会等待，这是同步复制的
> 预期行为；备库重新接入后自动恢复。

## 本地运行

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
export DATABASE_URL=postgresql://drill:drill@localhost:5432/drill
export STANDBY_URL=postgresql://drill:drill@localhost:5433/drill   # 可选；不配置则同步页显示不可用
uvicorn app.main:app --port 8000
```

## 配置

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `DATABASE_URL` | `postgresql://drill:drill@localhost:5432/drill` | 主库连接串 |
| `STANDBY_URL` | 空 | 物理备库连接串；为空时同步提交演练不可用，备库核对项标记无法确认 |
| `SYNC_STATE_FILE` | `var/sync_drills.json` | 同步演练结果落盘文件（重启保留判定） |
| `REPLICATION_APP_NAME` | `standby1` | 备库在 `pg_stat_replication` 中的 application_name |
| `REPLICATION_SLOT` | `standby1_slot` | 物理复制槽名 |
| `DRILL_SESSION_PREFIX` | `lockdrill` | 会话 `application_name` 前缀 |
| `BLOCK_CONFIRM_SECONDS` | `5` | 行锁演练等待阻塞关系确认的时间 |
| `RELEASE_WAIT_SECONDS` | `10` | 终止后等待受阻更新真实结束的时间 |
| `SYNC_WAIT_CONFIRM_SECONDS` | `10` | 等待回放暂停生效 / SyncRep 等待出现的时间 |
| `SYNC_CATCHUP_SECONDS` | `30` | 恢复回放后等待备库追平的时间 |
| `SYNC_DRILL_TTL_SECONDS` | `7200` | 同步演练超时自动过期（并恢复回放）的时长 |
| `DB_CONNECT_TIMEOUT` | `5` | 连接超时（秒） |

> 注意：演练使用 `pg_terminate_backend`、`pg_wal_replay_pause/resume` 与
> 复制权限，请使用专用数据库/角色与一次性实例。

## 测试

测试需要真实的一主一备（会创建/清理 `deploy_order` 与 `lock_drill_row` 数据，
并会暂停/恢复备库回放，请指向一次性实例）：

```bash
TEST_DATABASE_URL=postgresql://drill@127.0.0.1:55432/drill \
TEST_STANDBY_URL=postgresql://drill@127.0.0.1:55433/drill \
pytest -q
```

同步演练覆盖：暂停生效确认、remote_apply 的 SyncRep 等待确认、取消后
「主库有/备库无」、恢复回放追平且判定保持「取消等待」、正常提交「已同步确认」、
未确认等待时拒绝取消、凭据缺失/伪造 403、查库失败标记无法确认、
Web 重启后按单号核对两库且原结果保留、等待期间行锁演练并行可用、
新一轮自动恢复回放、复制状态上报。

行锁演练覆盖：真实阻塞关系、正常解锁恢复、服务重启后的旧页面、结束演练后的旧页面、
会话身份被篡改（拒绝终止且不释放锁）、终止成功但无提交结果（判未恢复）、
旧会话已消失但更新已提交（按真实结果判恢复）、重复解锁幂等。

## 接口

行锁演练：

- `GET /`、`POST /drill/start`、`GET /drill/{id}`
- `POST /drill/{id}/release`、`POST /drill/{id}/end`（表单，含 `token`）
- `GET /api/state`、`GET /api/drills/{id}`、`POST /api/drills/{id}/release|end`

同步提交演练：

- `GET /sync`、`POST /sync/start`、`GET /sync/{id}`
- `POST /sync/{id}/pause|resume|submit|cancel|verify`（表单，含 `token`）
- `GET /api/sync/state`、`GET /api/sync/{id}`
- `POST /api/sync/{id}/pause|resume|submit|cancel|verify`（JSON 表单）

健康检查：`GET /healthz`（同时报告主库与备库连通性）。
