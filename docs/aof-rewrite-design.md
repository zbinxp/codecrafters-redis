# AOF Rewrite — staged plan（可选扩展）

> CodeCrafters 的 AOF 组（`#75–84`）没有 rewrite 的 stage。这里把 rewrite 拆成
> 9 个循序渐进的小 stage，按顺序实现；每个 stage 独立可测、可单独提交。
> **不参与过关**，纯扩展。
>
> 前置：当前实现已有 incr 文件、manifest、append 写、启动 replay。
> 参考：`docs/dn4-pubsub-design.md`。

---

## 总览

| # | Stage | Slug | 依赖 | 说明 |
|---|-------|------|------|------|
| 1 | SET supports PXAT/EXAT | `ar1` | — | TTL 绝对化的前提 |
| 2 | Manifest as a list + ordered load | `ar2` | ar1 | base→incr 的基础设施 |
| 3 | Base serializer: strings | `ar3` | ar2 | 快照生成器（string） |
| 4 | Base serializer: collections | `ar4` | ar3 | list / zset / stream |
| 5 | BGREWRITEAOF v1 (sync) | `ar5` | ar4 | 真正的 rewrite |
| 6 | Background rewrite | `ar6` | ar5 | 不阻塞客户端 |
| 7 | Crash safety & orphan cleanup | `ar7` | ar5 | 可选 |
| 8 | Truncated tail repair | `ar8` | — | 可选、独立 |
| 9 | INFO persistence & auto-trigger | `ar9` | ar6 | 可选 |

推荐提交节奏：`ar1` → `ar2` → `ar3+ar4` → `ar5` → `ar6` → 其余按需。

---

## ar1 — SET supports PXAT/EXAT

**Goal**：`SET` 支持**绝对**过期时间，这是 AOF/rewrite 保留 TTL 的唯一正确方式。

**Behavior**
- `SET key value PXAT <unix-milliseconds>`：绝对毫秒。
- `SET key value EXAT <unix-seconds>`：绝对秒。
- 时间戳已过去 → 不插入（`GET` → `$-1`）。
- 非整数 → `ERR value is not an integer or out of range`。
- 与现有 `PX` 共存；`PX` 仍相对。

**Changes**
- `commands/string.rs::parse_set`：把「选项 + 值」解析成 `expires_at: Option<Instant>`。

**关键：Unix 时间戳不能直接变成 `Instant`**

`Instant` 是单调时钟，和 Unix epoch 无关，**没有**从时间戳构造的 API。正确做法是
「先拿当前 Unix 时间，算剩余时长，再加到 `Instant::now()` 上」：

```rust
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64).unwrap_or(0)
}

/// 绝对 Unix 毫秒 -> Instant 截止点；已经过去则返回 None。
fn unix_ms_to_instant(target_ms: u64) -> Option<Instant> {
    let now = now_ms();
    if target_ms <= now { return None; }
    Instant::now().checked_add(Duration::from_millis(target_ms - now))
}
```

`PXAT <unix-ms>` 直接用；`EXAT <unix-s>` 先 `*1000`。已经过期（`None`）时，为了**覆盖旧值**，
应插入一个「立刻过期」的 entry（`expires_at: Some(Instant::now())`），这样 `live_entry`
下次访问会删掉它；不要直接跳过插入，否则旧值会残留。

**Tests**
- `SET k v PXAT <future>` → `GET` 得值；`PXAT <past>` → `$-1`。
- `SET k v EXAT <future>` → 值在；过期后消失。
- `SET k v PXAT abc` → 错误。
- 回归：`PX`/无选项仍正常。

**Pitfall**：RDB 里的过期也是绝对 Unix 时间；`ar1` 之后 RDB 加载与 AOF 重放可以用同一种
「绝对时间 → `Instant`」换算。

---

## ar2 — Manifest as a list + ordered load

**Goal**：把 manifest 从「只找第一条 `type i`」升级成「解析全部条目，按 base→incr 顺序加载」。

**Behavior**
- 解析每行 `file <name> seq <n> type <b|i>` 为 `(name, seq, kind)`。
- 启动加载顺序：**最后一个 base**（seq 最大）→ 按 seq 升序的全部 incr。
- 单 base/单 incr 的老 manifest 行为不变。
- 加载完，以 append 打开**最后一个** incr。

**Changes**
- 用 `parse_manifest(text) -> Vec<ManifestEntry>` 替换 `active_incr_file`。
- `lib.rs::run` 的 AOF 段：先加载 base（`.aof` → `replay_aof`；`.rdb` → `rdb::load`），
  再逐条 replay incr。
- replay 期间 `store.aof` 保持 `None`（现有实现已保证）。

**Tests**
- manifest 有 base+incr：base 的值能读到。
- 两个 incr（seq 1、seq 2）：按顺序应用（同一 key 后者覆盖）。
- 没有 base、只有 incr：老行为。
- manifest 指向不存在的文件：不 panic，跳过/报错。

**Pitfall**：incr 必须按 `seq` 排序，不能按 manifest 行序；base 只取 seq 最大的那个。

---

## ar3 — Base serializer: strings

**Goal**：`fn serialize_dataset(store: &Store) -> Vec<u8>`，先支持 `Value::String`。

**Behavior**
- 遍历 `store.data`，跳过已过期 key。
- string → `SET <key> <value>`（RESP 数组，二进制安全）。
- 有 TTL → 追加 `PXAT <abs-ms>`（`abs = now_ms + remaining`）。
- 输出直接是 RESP 字节流，可被 `Decoder` replay。

**Changes**
- 新建 `src/aof.rs`（与 `rdb.rs` 对称）：`serialize_dataset`、`serialize_entry`。
- 复用 `Frame::array([...]).encode(&mut BytesMut)`。

**Tests**
- 单测：空库 → 空字节；`{foo: bar}` → `*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n`。
- 二进制值（含 `\0`、`\r\n`）能原样往返。
- 带 TTL：输出含 `PXAT <绝对毫秒>`。

**Pitfall**：TTL 一定写绝对 `PXAT`，不要写相对 `PX`。

---

## ar4 — Base serializer: collections

**Goal**：补齐 `List` / `ZSet` / `Stream` 的序列化。

**Behavior**

| Value | 输出 |
|---|---|
| `List(q)` | 一条 `RPUSH key e1 e2 ...` |
| `ZSet(t)` | 每个成员一条 `ZADD key <score> <member>`（当前 `cmd_zadd` 只收单成员） |
| `Stream(s)` | 每个 entry 一条 `XADD key <id> f v ...`（**显式 id**） |

- 跳过过期 key；TTL 仍用 `PXAT`（对 `RPUSH`/`ZADD`/`XADD` 同样适用）。

**Changes**
- `aof.rs` 增加各类型的 `serialize_*`。
- 注意 score 的格式化：`Score(f64).0.to_string()`。

**Tests**
- round-trip：构造含四种类型的 `Store` → `serialize_dataset` → replay 到空 `Store`
  → 逐 key 比较（类型 + 值 + 顺序 + score）。
- 空 list/zset/stream 不输出（或输出合法空命令）。

**Pitfall**：ZSet 的 `BTreeSet` 顺序是 score+member，replay 用单条 `ZADD` 不会改变集合语义；
Stream 必须显式 id，`*` 会导致重放时生成新 id。

---

## ar5 — BGREWRITEAOF v1 (sync, Multi-Part)

**Goal**：实现真正的 rewrite：用「当前数据集快照」替换历史日志。

**Behavior**
- 新命令 `BGREWRITEAOF` → `+Background append only file rewriting started\r\n`。
- 生成新文件：`<appendfilename>.<seq>.base.aof` + `<appendfilename>.<seq>.incr.aof`
  （`seq = manifest 最大 seq + 1`）。
- 写新 manifest（base + 新 incr），原子切换，删除旧文件。

**Algorithm（顺序不能错）**
```
lock:
  a. 创建新 incr（append 打开）
  b. store.aof = 新 incr            // 之后的写入只进新 incr
  c. snapshot = store.data.clone()  // 或持锁直接序列化
unlock
2. write snapshot -> base.tmp
3. fsync(base.tmp); rename -> base
4. write manifest.tmp (base + new incr); fsync; rename -> manifest
5. delete old base/incr
```

**Changes**
- `commands/server.rs` 或新建 `commands/aof.rs`：`cmd_bgrewriteaof`。
- `aof.rs`：`rewrite(store) -> Result<()>`（v1 同步）。
- `dispatch.rs`：`BGREWRITEAOF` 不当作写命令（不 append、不传播）。

**Tests**
- 同一 key `SET` 100 次 → `BGREWRITEAOF` → 新 base 里只有 1 条 `SET`（或重放后值正确）。
- rewrite 后重启 → `GET` 仍是最终值。
- 旧 base/incr 被删除，manifest 只引用新文件。
- 空库 rewrite：不 panic，重启后空库。
- 二进制值、TTL 在 rewrite 后仍正确。

**Pitfall**：必须**先切新 incr 再拍快照**；manifest 最后 rename；旧文件最后删。

---

## ar6 — Background rewrite

**Goal**：把 `ar5` 的写盘部分移到后台，不阻塞客户端。

**Behavior**
- 锁内只做：创建新 incr → 切换 `store.aof` → clone 快照。
- 锁外 `tokio::spawn`：写 base.tmp → fsync/rename → write manifest.tmp → rename → 删旧。
- 并发保护：rewrite 进行中再次 `BGREWRITEAOF` 直接返回（不重复启动）。
- rewrite 期间的新写命令进新 incr；重启后 base + 新 incr 能恢复。

**Changes**
- `Db` 或 `lib.rs` 静态：`aof_rewrite_in_progress: Arc<AtomicBool>`（或 `Mutex<Option<JoinHandle>>`）。
- `aof::rewrite_in_background(store: Arc<Mutex<Store>>)`。

**Tests**
- 启动 rewrite 后立刻发 `SET`，重启后该值存在（在 incr 里）。
- 连续两次 `BGREWRITEAOF`：第二次不 panic、不产生两套文件。
- 压力：rewrite 期间并发写，重启后状态一致。

**Pitfall**：clone 与「切换 incr」必须在同一把锁内完成（快照点 = 切换点），否则丢写。

---

## ar7 — Crash safety & orphan cleanup（可选）

**Goal**：保证 rewrite 任意时刻崩溃都能从旧/新 manifest 恢复，并清理孤儿文件。

**Behavior**
- 所有写入先写 `.tmp` 再 rename（base、manifest）。
- 启动时清理：`.tmp` 文件；manifest 未引用的 `*.base.*` / `*.incr.*`。
- 文档化恢复语义：
  - manifest rename 前崩溃 → 旧 manifest 有效，新文件是孤儿。
  - rename 后崩溃 → 新 manifest 有效，旧文件是孤儿。

**Tests**
- 目录里预置一个 `.tmp` 和一个不在 manifest 里的 incr → 启动后被清理（或忽略）。
- manifest 引用文件都存在时正常启动。

**Pitfall**：清理必须**只删 manifest 未引用**的文件；不要误删正在写的当前 incr。

---

## ar8 — Truncated tail repair（可选，独立）

**Goal**：崩溃留下半条命令时，加载后把文件截断到最后一个完整命令，避免后续 append 错位。

**Behavior**
- 加载时用 `Decoder` 逐条 parse，记录最后一条成功命令结束的字节偏移 `valid`。
- 全部加载后 `file.set_len(valid)`。
- 可选配置 `aof-load-truncated yes/no`（默认 yes）。

**Changes**
- `Decoder` 暴露「本次消费字节数」（`last_len()`）或让 `parse` 返回 `(Frame, usize)`。
- `lib.rs`/`aof.rs` 的 replay 返回 `valid_len`，再 truncate。

**Tests**
- 文件末尾多出半条命令 → 启动正常，文件被截断；再次重启加载正常。
- 完整的文件不受影响。

**Pitfall**：只截断**最后一个完整命令之后**的字节；不要把合法内容截掉。

---

## ar9 — INFO persistence & auto-trigger（可选）

**Goal**：暴露 rewrite 状态，并支持按大小自动触发。

**Behavior**
- `INFO persistence` 增加：
  - `aof_enabled:0/1`、`aof_rewrite_in_progress:0/1`
  - `aof_current_size`、`aof_base_size`
  - `aof_last_bgrewrite_status:ok/err`
- 启动/周期检查：`aof_current_size > auto-aof-rewrite-min-size` 且增长率超阈值 → 自动 rewrite。
- 可选 `CONFIG GET auto-aof-rewrite-min-size`。

**Tests**
- `INFO persistence` 字段存在且值合理。
- 小文件不触发；超过阈值触发一次。

**Pitfall**：自动触发要有并发保护，避免与手动 `BGREWRITEAOF` 重叠。

---

## 非目标

- 写 RDB / `SAVE` / `BGSAVE`（本挑战只读 RDB；base 用 `.base.aof`）。
- fork 子进程（单进程方案已够学习）。
- LZF / RDB preamble / 增量 fsync。

## 一句话

> **rewrite = 用「当前数据集的最小命令集」替换历史日志。**
> 顺序：**切新 incr → 拍快照 → 锁外写 base → 原子 rename manifest → 删旧文件**；
> TTL 一律 `PXAT`；加载 **base 先、incr 按 seq 后**。
