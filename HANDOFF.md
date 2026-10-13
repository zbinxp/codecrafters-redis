# 交接文档 — codecrafters Redis (Rust)

> 更新于 2026-10-14。**学习 session**：用户自己写代码，助手只做 review / 挑毛病。
> 本文件在仓库根目录，`codecrafters submit` 会一起 push，不需要时可删。
> 其他有用的文档：`LESSON.md`（经验教训，第 7 节是本轮新增）、`STAGES.md`（124 个
> stage 与进度）、`docs/dn4-pubsub-design.md`（Pub/Sub 架构设计）。

---

## 1. 项目

[codecrafters "Build Your Own Redis"](https://codecrafters.io/challenges/redis)，Rust。
本地：`./your_program.sh`（`cargo build --release` 后 exec；已转发 `"$@"`）。

现在是 **library + thin binary**：`src/main.rs` 只解析 CLI 参数、构造
`codecrafters_redis::Config`，然后 `run(config)`；所有逻辑在库里。

## 2. 进度

参考 `STAGES.md`。当前：**83 / 124 完成**。

- core #1–7、Lists #8–18、Streams #19–31 ✅
- **Transactions #32–42** ✅（si4…jf8）
- **Optimistic Locking #43–50** ✅（jb7…hq1）
- **Replication #51–68** ✅（bw1…na2）
- Pub/Sub #85–91、Sorted Sets #92–99 ✅（**乱序提前做的**）
- 待做：**RDB #69–74**、AOF #75–84、Bitmaps #100–108、Geospatial #109–116、
  Authentication #117–124。

> ⚠️ 平台进度远落后于本地，只有 `codecrafters submit` 才推进远端。
> 用 `codecrafters task -stage <slug>` 看任意 stage 的官方说明。

### 下一步：`zg5` — RDB file config (#69)

解析 `--dir <path>` / `--dbfilename <name>`，实现 `CONFIG GET <param>`：
返回 `*2\r\n$<len>\r\n<param>\r\n$<len>\r\n<value>\r\n`。本阶段不用读 RDB 文件。

## 3. 模块结构

```
src/
  main.rs          只做 CLI 解析（parse_args）→ codecrafters_redis::run(Config)
  lib.rs           pub struct Config；pub async fn run(config)；建 Store + accept loop
  resp.rs          Frame（含 Rdb 变体）、encode、Frame::frame_len、as_bulk、Decoder
  db.rs            Entry / Value / Db / Role / Score / StreamId；live_entry、touch/version
  client.rs        Conn：每连接状态（id/subscriptions/out_tx/multi/watched/from_master）
  connection.rs    StreamHandler：读/写；generate_reply 里特判 BLPOP / XREAD BLOCK / WAIT
  dispatch.rs      lookup + execute（门禁 → MULTI 入队 → handler → 写命令广播）
  error.rs         RedisError（每种 wire 消息一个构造器）
  util.rs          parse<T>、now_ms
  replica.rs       副本侧：连主库、握手、读 RDB、Decoder 循环应用命令 + GETACK/ACK
  commands/
    mod.rs         type CmdResult、bulk_at、resolve_range、各命令 re-export
    connection.rs  PING / ECHO / TYPE
    string.rs      GET / SET(PX) / INCR
    list.rs        RPUSH / LPUSH / LRANGE / LLEN / LPOP / BLPOP(async)
    zset.rs        ZADD / ZRANK / ZRANGE / ZCARD / ZSCORE / ZREM
    stream.rs      XADD / XRANGE / XREAD + collect_xread / resolve_ids
    pubsub.rs      PUBLISH / SUBSCRIBE / UNSUBSCRIBE
    transaction.rs MULTI / EXEC / DISCARD / WATCH / UNWATCH
    server.rs      INFO / REPLCONF / PSYNC / WAIT(async)
```

## 4. 核心抽象

```rust
pub type CmdResult = Result<Vec<Frame>, RedisError>;   // 一条命令 → 0..N 帧；空 = 无回复
pub type Handler = fn(&[Frame], &mut Conn, &mut Store) -> CmdResult;
```

`dispatch::execute(req, conn, store)` 的顺序：

1. `*0` / 首元素非 bulk → `Ok(vec![])`（无回复）。
2. 订阅模式门禁：非白名单 → `-ERR Can't execute ...`；订阅态 `PING` → `["pong", msg]`。
3. **只读门禁**：`is_write_command(name) && matches!(store.role, Role::Replica{..}) &&
   !conn.from_master` → `-READONLY You can't write against a read only replica.`
4. **MULTI 入队**：`conn.multi` 为 `Some` 且非控制命令 → push + `+QUEUED`。
   （控制命令 `MULTI/EXEC/DISCARD/WATCH/UNWATCH` 正常执行。）
5. `lookup(name)` 派发；handler 成功后若是**写命令**，广播给所有 replica 并累加 offset。

异步命令（`BLPOP`、`XREAD ... BLOCK`、`WAIT`）不进 `lookup`，在
`connection.rs::generate_reply` 里特判并 `.await`。只在「非 MULTI 且非订阅模式」下走特判。

`Conn`（`client.rs`）：`id`（来自 `NEXT_CONN`）、`subscriptions`、`out_tx`（出站队列）、
`multi: Option<Vec<Frame>>`、`watched: HashMap<key, u64>`、`from_master: bool`。

## 5. Replication 架构（master ↔ replica）

### 5.1 状态

```rust
enum Role { Master, Replica { host: String, port: u16 } }

struct Db {
    data, waiters, channel_subscribers,
    role: Role,
    master_replid: String,            // 40 位，硬编码 8371b4fb...990aeeb
    master_repl_offset: u64,          // 主库：随每次广播的写命令累加
    replicas: HashMap<u64, Outbox>,   // conn_id -> 该副本连接的出站队列
    replica_acks: HashMap<u64, u64>,  // conn_id -> 副本最近 ACK 的 offset
    versions, version_seq,            // WATCH 用
}
```

启动：`run(config)` 决定 `Role`（有无 `--replicaof`），建**同一个** `Arc<Mutex<Store>>`
既服务客户端连接、也交给副本任务；若是副本，`tokio::spawn(replica::handshake(...))`。

### 5.2 主库侧流程

```
副本 --PING----------------> 主库  +PONG
副本 --REPLCONF listening-port <p>-->  +OK
副本 --REPLCONF capa psync2------>  +OK
副本 --PSYNC ? -1--------------->  登记 conn.out_tx 进 replicas
                                    +FULLRESYNC <replid> 0\r\n
                                    $88\r\n<空 RDB>            （Frame::Rdb，无尾 CRLF）
客户端 --SET k v--> 主库 执行 → 把 *3\r\n$3\r\nSET\r\n... 发给所有 replica.out_tx
                             master_repl_offset += frame_len
副本 --REPLCONF ACK <off>--> 主库 记 replica_acks[conn_id]=off（不回响应）
客户端 --WAIT n t--> 主库 发 REPLCONF GETACK * 给所有副本 → 轮询 replica_acks
                          直到 >= n 个副本的 offset >= target，或超时 → 回 :count
```

关键点：
- `cmd_replconf` 对 `ACK` **不回响应**（`Ok(vec![])`），其它（listening-port/capa）回 `+OK`。
- `cmd_psync` 把 `conn.out_tx.clone()` 存进 `store.replicas`；`cmd_psync` 之后这条连接
  的 writer task 继续 drain 它的 `out_tx`，所以广播能写到副本连接。
- 写命令的广播在 `dispatch::execute` 里做（handler 成功之后），所以失败的写不广播。
- `Frame::Rdb(Vec<u8>)` 编码为 `$<len>\r\n<bytes>`（**无尾 CRLF**，区别于 bulk string）。
- `WAIT` 是 async：**发完 GETACK 就释放 store 锁**，然后每 10ms 重新加锁查一次
  `replica_acks`。持锁等待会和 ACK 处理 task 死锁。

### 5.3 副本侧流程（`replica.rs::handshake`）

1. `TcpStream::connect(master)`。
2. 依次 `PING` / `REPLCONF listening-port <own>` / `REPLCONF capa psync2` / `PSYNC ? -1`，
   每步 `read_line` 读一行响应（simple string，`Decoder` 解析不了，要手动读）。
   ⚠️ `send_and_read_reply(PSYNC)` 已经把 `+FULLRESYNC` 那行读掉了，别再补 `read_line`。
3. 读 `$<len>\r\n`，`read_exact` 读满 `len` 字节的 RDB（不解析内容）。
4. 建一个 `from_master=true` 的一次性 `Conn`，`Decoder` 循环解析主库命令流：
   - `REPLCONF GETACK *`（大小写不敏感）→ 回 `REPLCONF ACK <offset>`，然后 `offset += 该命令字节数`。
   - 其它命令 → `execute(&frame, &mut conn, &mut store)`，**忽略返回**（副本不回响应），
     然后 `offset += 该命令字节数`。
   - `offset` = 已处理命令的 RESP 字节总数；GETACK 自身不计入**当次** ACK。

副本本身也用同一个 `store` 服务客户端（只读）。

## 6. 已验证的事实

| 验证项 | 结果 |
|---|---|
| `cargo test` | **8 lib unit + 4 bin unit + 249 integration，全绿，0 warning** |
| Transactions（MULTI/EXEC/DISCARD/WATCH/UNWATCH） | 集成测试覆盖，含官方场景 |
| WATCH touch/多 key/缺失 key/改回原值 | 集成测试覆盖 |
| 复制握手（PING/REPLCONF×2/PSYNC/FULLRESYNC/RDB） | mock-master 集成测试 |
| 单/多副本写命令传播 | 集成测试（1 个 + 2 个真副本） |
| 副本应用命令（yg4） | mock-master + 真副本端到端 |
| 只读副本拒绝客户端写 | 集成测试 |
| REPLCONF GETACK/ACK + offset 累计 | 集成测试（0/51/146） |
| WAIT（无副本 / 有副本无写 / 有副本有写） | 集成测试（主库 + 2 真副本） |

## 7. 已知遗留（不阻塞当前）

1. **命令名大小写敏感**：`lookup` 精确匹配大写；`ping` 会 unknown command。
2. **断开的连接/副本会残留**：`Db.replicas`、`replica_acks`、阻塞等待者不清理。
3. **多 key 的阻塞 XREAD** 只在第一个 key 注册等待者。
4. **空集合键不删除**（列表/zset/流清空后 key 仍在）。
5. **RESP 协议错误**（畸形输入）未返回 `-ERR Protocol error`，会静默等待。
6. **副本上 `master_repl_offset` 也会被累加**：`dispatch::execute` 对所有写命令都加，
   包括副本应用传播命令时（`from_master=true`，但副本没有 replicas）。这与 `replica.rs`
   里用于 ACK 的局部 `offset`（含 GETACK）不一致。
7. **`WAIT` 的 `numreplicas==0`** 返回连接副本数（真 Redis 返 0），用于测试里「等副本连上」。
8. **BLPOP 在 EXEC 里** 会因不在 `lookup` 而变成 `unknown command`。
9. `is_transaction_control` 把 `UNWATCH` 也列为控制命令（立即执行），真 Redis 是入队。
10. 拼写：`Db.replica_acks` 之类命名可再统一。

## 8. 测试基础设施（`tests/pipelining.rs`，黑盒 raw `TcpStream`）

- `lock_server()`：全局 `Mutex`，串行化所有测试（默认端口 6379 共享）。
- `TestCtx::new()`：拿锁 + 起真实 server 子进程（无参，默认 6379）+ 一个 `Client`。
- `Server::start_on(addr, extra_args)`：在任意地址起服务器（自动加 `--port`，可带
  `--replicaof` 等），轮询直到可连；spawn 前探测端口，被占就 panic。
- `Client::connect_to(addr)`：连任意地址；`cmd` / `raw` / `expect` / `expect_prefix` /
  `expect_silent` / `read_line`。
- **mock-master 模式**：测试自己 `TcpListener::bind("127.0.0.1:0")` 当主库，起副本指向它，
  断言副本发来的字节。`accept` 用非阻塞轮询 + deadline。
- 多进程测试（master + N 真副本）：`Server::start_on` 起多个实例，用
  `Client::connect_to(master)` 发命令，轮询 `WAIT 0` 等副本连上。

## 9. 怎么开始新 session

```sh
cd /home/dd/lab/rust-lab/codecrafters-redis-rust
cargo test                    # 应全绿（8 lib + 4 bin unit + 249 integration）
cargo check --all-targets     # 0 warning
codecrafters task -stage zg5  # 看下一 stage 官方说明
./your_program.sh             # 另开终端手动试
```

当前 git 状态：干净（最新提交 `4afa184`）。
