# 交接文档 — codecrafters Redis (Rust)

> 更新于 2026-09-27。**学习 session**：用户自己写代码，助手只做 review / 挑毛病。
> 本文件在仓库根目录，`codecrafters submit` 会一起 push，不需要时可删。
> 其他有用的文档：`LESSON.md`（经验教训）、`STAGES.md`（124 个 stage 与进度）、
> `docs/dn4-pubsub-design.md`（Pub/Sub 架构设计）。

---

## 1. 项目

[codecrafters "Build Your Own Redis"](https://codecrafters.io/challenges/redis)，Rust。
入口 `src/main.rs` + `src/resp.rs`。本地：`./your_program.sh`（`cargo build --release` 后 exec）。
`your_program.sh` 已转发 `"$@"`。

## 2. 进度

参考 `STAGES.md`（含逐 stage 状态）。当前：**46 / 124 完成**。

- **核心 #1–7**：jm1…yz1 ✅
- **Lists #8–18**：mh6…xj7 ✅
- **Streams #19–31**：cc3…xu1 ✅
- **Pub/Sub #85–91**：mx3…ze9 ✅（**乱序提前做的**）
- **Sorted Sets #92–99**：ct1…sq7 ✅（**乱序提前做的**）
- 其余（Transactions / Optimistic Locking / Replication / RDB / AOF / Bitmaps /
  Geospatial / Authentication）**待做**。

⚠️ 平台进度远落后于本地：`codecrafters task` 显示大约仍在 `wy1`。只有
`codecrafters submit` 才推进远端；要按平台顺序补做 **Transactions #32–42**，
再往后补。Pub/Sub 与 Sorted Sets 已完成，轮到时会直接过。

> 官方顺序（已用 `codecrafters task -stage +N` 验证）：core → lists → streams →
> transactions → optimistic-locking → replication → rdb → aof → pub/sub →
> sorted-sets → bitmaps → geospatial → auth。
> 用 `codecrafters task -stage <slug>` 可看任意 stage 的官方说明。

### 下一步：`si4` — The INCR command (1/3)

`INCR key` / `DECR` / `INCRBY`：对 string 值做整数自增，返回新值（RESP integer）。
- 值不是整数 → `-ERR value is not an integer or out of range`
- key 不存在 → 当作 0，结果为 1
- 非 string 类型 → WRONGTYPE
- 溢出（i64）→ `-ERR increment or decrement would overflow`

## 3. 当前代码状态

### 3.1 RESP 层（`src/resp.rs`）

- `Frame`：`SimpleString`、`BulkString`、`Int(i64)`、`Array(Vec<Frame>)`、
  `Err(String)`、`Null`(`$-1`)、`NullArray`(`*-1`)。
  - **`Err` 的 message 原样输出**（不自动加 `ERR `），调用方给完整错误码。
  - 构造器 `Frame::bulk(impl AsRef<[u8]>)` / `Frame::simple(...)` / `Frame::array(Vec<impl AsRef<[u8]>>)`。
- `Decoder`：增量解析，只接受「bulk string 组成的 array」（请求格式）。

### 3.2 数据模型（`src/main.rs`）

```rust
struct Entry { value: Value, expires_at: Option<Instant> }
enum Value {
    String(Vec<u8>),
    List(VecDeque<Vec<u8>>),
    ZSet(BTreeSet<(Score, Vec<u8>)>),              // Score 用 total_cmp 排序
    Stream(BTreeMap<StreamId, Vec<(Vec<u8>, Vec<u8>)>>),  // StreamId(ms, seq)
}
struct Db {
    data: HashMap<Vec<u8>, Entry>,
    waiters: HashMap<Vec<u8>, VecDeque<oneshot::Sender<()>>>,   // BLPOP / XREAD BLOCK
    channel_subscribers: HashMap<Vec<u8>, HashMap<u64, Outbox>>, // Pub/Sub: channel -> conn_id -> 发件箱
}
type Store = Db;
type Outbox = tokio::sync::mpsc::UnboundedSender<Frame>;
```

- **过期**：`Instant`（单调时钟）。`live_entry(store, key) -> Option<&mut Entry>`
  命中即惰性删除；所有命令都应经它，保证「过期 == 不存在」。
- **共享状态**：`Arc<Mutex<Store>>`，`main` 建一次，`clone` 进每个 task。

### 3.3 命令分发

- `type Handler = fn(&[Frame], &mut Store) -> Frame`；`lookup(name)` 返回 handler。
- 已实现同步命令：`PING, COMMAND, ECHO, GET, SET(PX), TYPE, RPUSH, LPUSH, LRANGE,
  LLEN, LPOP, ZADD, ZRANK, ZRANGE, ZCARD, ZSCORE, ZREM, XADD, XRANGE, PUBLISH`。
- `resolve_range(len, start, stop)`：LRANGE / ZRANGE 共用的索引归一化（负数、越界）。

### 3.4 连接与异步（`StreamHandler`）

- 每连接：`decoder`、`subscriptions: HashSet<Vec<u8>>`、`out_tx: Outbox`、`conn_id: u64`
  （来自 `static NEXT_CONN: AtomicU64`）。
- `main`：`tokio::io::split(stream)` → 起一个 **writer task**（独占写半，drain
  `out_rx` 写 socket）→ 起连接 task（`next()` 读 + `respond()` 把回复塞进 `out_tx`）。
  **单写者**，回复与推送不会交错。
- `generate_reply` 按需走异步 / 特判：
  - `BLPOP` → `blpop(args, store).await`
  - `XREAD ... BLOCK ...` → `xread_block(args).await`
  - `SUBSCRIBE` / `UNSUBSCRIBE` → 改 `subscriptions` 与 `channel_subscribers`
  - subscribed mode：非白名单命令 → `-ERR Can't execute '<name>' ...`；
    subscribed 时 `PING` → `["pong", msg]`
- **Pub/Sub 投递**：`cmd_publish` 把 `["message", ch, msg]` 发给
  `channel_subscribers[ch]` 里每个 `out_tx`（sync `send`，持锁调用没问题）。
- **阻塞命令**：`oneshot` 当作「只响一次的闹钟」；
  - BLPOP：`RPUSH/LPUSH` 每入队一个元素唤醒一个等待者；
  - XREAD BLOCK：`XADD` 唤醒该 key 的**所有**等待者；`$` 的 id **在进循环前解析一次**。

## 4. 已验证的事实

| 验证项 | 结果 |
|---|---|
| `cargo test` | **8 unit + 215 integration，全绿，0 warning** |
| redis-cli 管道 `PING` | `+PONG` 正常 |
| 100 并发连接各发 PING | 100/100 |
| BLPOP / XREAD BLOCK 阻塞与唤醒、超时、`$` | 均验证通过 |
| Pub/Sub 订阅/发布/退订 | 均验证通过 |
| Sorted sets / Lists 各边界 | 均有集成测试 |

## 5. 已知遗留（不阻塞当前）

1. **命令名大小写敏感**。`lookup` 与多处 `cmd == b"PING"` 精确比较；`ping` 会
   unknown command。Redis 不区分大小写。tester 用大写，暂不影响。
2. **断开的阻塞者会残留**。`BLPOP` / `XREAD BLOCK` 客户端断线后，task 与
   `oneshot::Sender` 可能滞留（`timeout 0` 时）。未做断线清理。
3. **多 key 的阻塞 XREAD** 只在第一个 key 上注册等待者（单 key 已够用）。
4. **空集合键不删除**（列表清空、zset 清空、流清空后 key 仍在内存里）；
   命令行为一致，暂不可观测。
5. `push_impl` 里 bulk 失败分支的错误信息硬编码 `'rpush'`（实际不可达）。
6. RESP 协议错误（畸形输入）未返回 `-ERR Protocol error`，会静默等待更多数据。

## 6. 踩过的坑（详见 `LESSON.md`）

- `MutexGuard` 不能跨 `.await`；锁范围收进 block。
- panic 持锁 → Mutex 中毒，整服务器瘫痪（切片越界踩过）。
- `Duration::from_secs_f64(NaN)` panic；`"nan"`/`"inf"` 能被 `parse::<f64>()` 接受 →
  用 `!is_finite()`。
- RESP：命令参数恒为 bulk string；null bulk `$-1` 与 null array `*-1` 是不同类型；
  错误消息要带错误码。
- 测试：新测试先在旧实现上确认失败；只读要断言的长度（`read_at_least` 的 `want`
  过大会白等超时）；端口 6379 硬编码，全局锁串行化。
- `TestCtx`/`Client` 封装：`Client` 内部缓冲，`expect` 精确消费断言字节数
  （否则一次 read 拿到两帧会串味）。

## 7. 测试基础设施

`tests/pipelining.rs`（**黑盒**，raw `TcpStream`）：
- `TestCtx::new()`：拿全局锁 + 起真实 server 子进程 + 一个默认连接；
  `t.connect()` 开额外连接。
- `Client`：`cmd(&[&[u8]])` 编码并发命令；`raw(bytes)` 发原始字节；
  `expect(bytes)` 断言并消费；`expect_prefix`（整行）；`expect_silent`；
  `read_line`（读一行，用于 `XADD *` 这类非确定值）。
- `Server::start()` spawn 前先探测端口，被占直接 panic（防测到残留旧 server）。

## 8. 怎么开始新 session

```sh
cd /home/dd/lab/rust-lab/codecrafters-redis-rust
cargo test                    # 应全绿（8 unit + 215 integration）
codecrafters task -stage si4  # 看下一 stage 官方说明
./your_program.sh             # 另开终端手动试
```

当前 git 状态：干净（最新提交 `6b877ee`）。
