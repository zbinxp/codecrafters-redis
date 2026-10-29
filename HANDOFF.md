# 交接文档 — codecrafters Redis (Rust)

> 更新于 2026-10-29。**学习 session**：用户自己写代码，助手只做 review / 挑毛病。
> 本文件在仓库根目录，`codecrafters submit` 会一起 push，不需要时可删。
> 其他文档：`README.md`（项目概览）、`LESSON.md`（经验教训）、`STAGES.md`（124 个
> stage 与进度）、`docs/aof-rewrite-design.md`（AOF rewrite 扩展设计）、
> `docs/dn4-pubsub-design.md`（Pub/Sub 设计）。

---

## 1. 项目

[codecrafters "Build Your Own Redis"](https://codecrafters.io/challenges/redis)，Rust。
本地：`./your_program.sh`（`cargo build --release` 后 exec；已转发 `"$@"`）。

形态是 **library + thin binary**：`src/main.rs` 只解析 CLI 参数、构造
`codecrafters_redis::Config`，然后 `run(config)`；所有逻辑在库里。

## 2. 进度

参考 `STAGES.md`。当前：**99 / 124 完成**。

- core #1–7、Lists #8–18、Streams #19–31 ✅
- **Transactions #32–42** ✅（si4…jf8）
- **Optimistic Locking #43–50** ✅（jb7…hq1）
- **Replication #51–68** ✅（bw1…na2）
- **RDB Persistence #69–74** ✅（zg5…sm4，**只读 RDB**）
- **AOF Persistence #75–84** ✅（uj3…kn2）
- Pub/Sub #85–91、Sorted Sets #92–99 ✅（**乱序提前做的**）
- 待做：**Bitmaps #100–108**、Geospatial #109–116、Authentication #117–124。

> ⚠️ 平台进度远落后于本地，只有 `codecrafters submit` 才推进远端。
> 用 `codecrafters task -stage <slug>` 看任意 stage 的官方说明。

### 下一步：`bq9` — Create a bitmap (#100)

`SETBIT key offset value`：bitmap 就是 `Value::String`；offset 0 = 首字节 **MSB**
（`mask = 1 << (7 - offset % 8)`），返回旧 bit；offset 超出长度时用 0 扩展。
**记得把 `SETBIT` 加进 `is_write_command`**（AOF/复制）。之后才是 GETBIT/BITCOUNT 等。

### 额外扩展（不在官方 124 stage 内）

AOF rewrite 扩展 `ar1–ar9` 已全部完成，设计/测试提示见 `docs/aof-rewrite-design.md`。

## 3. 模块结构

```
src/
  main.rs          只做 CLI 解析（parse_args）→ codecrafters_redis::run(Config)
  lib.rs           pub struct Config；run()：建 Store、加载 RDB、初始化/清理/重放 AOF、起 listener
  resp.rs          Frame（含 Rdb 变体）、encode、frame_len、as_bulk、Decoder（+ pending）
  db.rs            Entry/Value/Db/Role/Score/StreamId；live_entry、readonly_entry、touch/version；AOF 状态
  client.rs        Conn：id/subscriptions/out_tx/multi/watched/from_master/replaying
  connection.rs    StreamHandler：读/写；generate_reply 里特判 BLPOP / XREAD BLOCK / WAIT
  dispatch.rs      lookup + execute（门禁 → MULTI 入队 → handler → AOF/广播/自动 rewrite）
  error.rs         RedisError（每种 wire 消息一个构造器）
  util.rs          parse<T>、now_ms、unix_ms_to_instant、instant_to_unix_ms
  rdb.rs           RDB 只读解析（字符串 + 过期，binary-safe）
  aof.rs           manifest、serialize_dataset、cmd_bgrewriteaof、aof_sizes、
                   should_auto_rewrite、cleanup、truncate 辅助
  replica.rs       副本侧：连主库、握手、读 RDB、命令流 + GETACK/ACK + offset
  commands/
    mod.rs         type CmdResult、bulk_at、resolve_range、各命令 re-export
    connection.rs  PING / ECHO / TYPE
    string.rs      GET / SET(PX/PXAT/EXAT) / INCR / PEXPIREAT
    list.rs        RPUSH / LPUSH / LRANGE / LLEN / LPOP / BLPOP(async)
    zset.rs        ZADD / ZRANK / ZRANGE / ZCARD / ZSCORE / ZREM
    stream.rs      XADD / XRANGE / XREAD + collect_xread / resolve_ids
    pubsub.rs      PUBLISH / SUBSCRIBE / UNSUBSCRIBE
    transaction.rs MULTI / EXEC / DISCARD / WATCH / UNWATCH
    server.rs      INFO / REPLCONF / PSYNC / WAIT(async) / CONFIG / KEYS
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
   !conn.from_master` → `-READONLY ...`。
4. **MULTI 入队**：`conn.multi` 为 `Some` 且非控制命令 → push + `+QUEUED`。
5. `lookup(name)` 派发；handler 成功后若是**写命令**：
   - append 到 `store.aof`（`appendfsync=always` 时 `sync_all`）；
   - 广播给所有 replica 并累加 `master_repl_offset`；
   - 若 `!conn.replaying`，检查 `should_auto_rewrite`，达标就触发 `BGREWRITEAOF`。

异步命令（`BLPOP`、`XREAD ... BLOCK`、`WAIT`）不进 `lookup`，在
`connection.rs::generate_reply` 里特判并 `.await`（只在非 MULTI/非订阅模式下）。

`Conn`：`id`、`subscriptions`、`out_tx`、`multi`、`watched`、`from_master`、`replaying`。

## 5. 持久化

### 5.1 RDB（只读）

- 启动时读 `{dir}/{dbfilename}`，`rdb::load(&bytes, &mut store)`；文件不存在 → 空库。
- 支持 string key + 过期（`FD` 4 字节 LE 秒 / `FC` 8 字节 LE 毫秒）；
  size 编码大端，integer-encoded string 是**小端**；已过期的 key 加载时丢弃。
- **不写 RDB**：没有 SAVE/BGSAVE（挑战没考）；复制 `FULLRESYNC` 用硬编码空 RDB。

### 5.2 AOF（Multi-Part）

配置（默认）：`appendonly=no`、`appenddirname=appendonlydir`、
`appendfilename=appendonly.aof`、`appendfsync=everysec`、
`auto-aof-rewrite-min-size=67108864`、`auto-aof-rewrite-percentage=100`。

启动（`appendonly yes`）：

```
create_dir_all(<dir>/<appenddirname>)
→ 读 manifest；没有就创建默认 incr + manifest（pb9）
→ cleanup_orphan：删 *.tmp / 未引用的 *.base.* / *.incr.*（保留 manifest 与 active 文件）
→ 按 manifest 重放（base 先，incr 按 seq；*.rdb base 用 rdb::load）
→ 流式 replay，最后 truncate_file(valid_len) 修掉尾部半条命令（ar8）
→ 以 append 打开最后一个 incr
```

写路径：`dispatch::execute` 把成功的写命令**原样** append；`appendfsync=always` 时
`sync_all`。`PEXPIREAT` 是绝对时间；`SET ... PX` 仍按原文进入 AOF（见 §8.10）。

`BGREWRITEAOF`（ar5/ar6）：

```
锁内：建新 incr、store.aof = 新 incr、clone snapshot、
      置 aof_rewrite_in_progress=true、clone ok flag
锁外 spawn_blocking：
      写 <appendfilename>.<seq>.base.aof（temp→fsync→rename）
      写新 manifest（temp→fsync→rename）
      成功后才删旧文件
      aof_last_bgrewrite_ok = result.is_ok(); in_progress=false
```

- 第二次 `BGREWRITEAOF` 在 `in_progress` 时立即返回 `-ERR ... already in progress`。
- 自动 rewrite：每条写命令后 `should_auto_rewrite` 检查
  `current > min && (current-base)*100 >= base*percentage`（base=0 视为首次）。

`INFO persistence`：`aof_enabled:0/1`、`aof_rewrite_in_progress:0/1`、
`aof_current_size`（**活动 incr** 大小）、`aof_base_size`、`aof_last_bgrewrite_status:ok/err`。
`CONFIG GET` 支持 `auto-aof-rewrite-min-size` / `auto-aof-rewrite-percentage`。

## 6. Replication 架构（master ↔ replica）

### 6.1 状态

```rust
enum Role { Master, Replica { host: String, port: u16 } }

struct Db {
    data, waiters, channel_subscribers,
    role: Role,
    master_replid: String,            // 40 位，硬编码 8371b4fb...990aeeb
    master_repl_offset: u64,          // 主库：随每次广播的写命令累加
    replicas: HashMap<u64, Outbox>,   // conn_id -> 该副本连接的出站队列
    replic_acks: HashMap<u64, u64>,   // conn_id -> 副本最近 ACK 的 offset
    versions, version_seq,            // WATCH 用
    dir, dbfilename,
    appendonly, appenddirname, appendfilename, appendfsync,
    aof: Option<File>,
    aof_rewrite_in_progress: Arc<AtomicBool>,
    aof_last_bgrewrite_ok: Arc<AtomicBool>,
    auto_aof_rewrite_min_size, auto_aof_rewrite_percentage,
}
```

启动：`run(config)` 决定 `Role`（有无 `--replicaof`），建**同一个** `Arc<Mutex<Store>>`
既服务客户端连接、也交给副本任务；若是副本，`tokio::spawn(replica::handshake(...))`。

### 6.2 主库侧流程

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
- `cmd_replconf` 对 `ACK` **不回响应**（`Ok(vec![])`），其它回 `+OK`。
- `cmd_psync` 把 `conn.out_tx.clone()` 存进 `store.replicas`。
- 广播在 `dispatch::execute` 里做（handler 成功之后），失败的写不广播。
- `Frame::Rdb(Vec<u8>)` 编码为 `$<len>\r\n<bytes>`（**无尾 CRLF**）。
- `WAIT` 是 async：**发完 GETACK 就释放 store 锁**，每 10ms 重新加锁查
  `replica_acks`；持锁等待会和 ACK 处理 task 死锁。

### 6.3 副本侧流程（`replica.rs::handshake`）

1. `TcpStream::connect(master)`。
2. 依次 `PING` / `REPLCONF listening-port <own>` / `REPLCONF capa psync2` / `PSYNC ? -1`，
   每步 `read_line` 读一行响应（simple string，`Decoder` 解析不了）。
   ⚠️ `send_and_read_reply(PSYNC)` 已读掉 `+FULLRESYNC`，别再补 `read_line`。
3. 读 `$<len>\r\n`，`read_exact` 读满 RDB。
4. 建 `from_master=true` 的一次性 `Conn`，`Decoder` 循环：
   - `REPLCONF GETACK *`（大小写不敏感）→ 回 `REPLCONF ACK <offset>`，然后
     `offset += 该命令字节数`。
   - 其它命令 → `execute(...)` 并**忽略返回**（副本不回响应），再 `offset += ...`。
   - GETACK 自身不计入**当次** ACK。

副本本身也用同一个 `store` 服务客户端（只读）。

## 7. 已验证的事实

| 验证项 | 结果 |
|---|---|
| `cargo test` | **21 lib unit + 4 bin unit + 292 integration，全绿，0 warning** |
| `cargo check --all-targets` | 0 warning |
| Transactions / WATCH touch / 多 key / 缺失 key / 改回原值 | 集成测试覆盖 |
| 复制握手、单/多副本传播、只读副本、GETACK/ACK、WAIT | 集成测试（含真副本端到端） |
| RDB：string、二进制值、多 key、过期 | 集成测试 |
| AOF：append、manifest、replay、orphan 清理、截断修复 | 集成测试 |
| BGREWRITEAOF：压缩、重启恢复、rewrite 期间写入 | 集成测试 |
| INFO persistence / CONFIG GET / 自动 rewrite | 集成测试 |

## 8. 已知遗留（不阻塞当前）

1. **命令名大小写敏感**：`lookup` 精确匹配大写；`ping` 会 unknown command。
2. **断开的连接/副本会残留**：`Db.replicas`、`replica_acks`、阻塞等待者不清理。
3. **多 key 的阻塞 XREAD** 只在第一个 key 注册等待者。
4. **空集合键不删除**（列表/zset/流清空后 key 仍在）。
5. **RESP 协议错误**（畸形输入）未返回 `-ERR Protocol error`，会静默等待。
6. **副本上 `master_repl_offset` 也会被累加**：`dispatch::execute` 对所有写命令都加，
   包括副本应用传播命令时；与 `replica.rs` 里用于 ACK 的局部 `offset` 口径不同。
7. **`WAIT` 的 `numreplicas==0`** 返回连接副本数（真 Redis 返 0）。
8. **BLPOP 在 EXEC 里** 会因不在 `lookup` 而变成 `unknown command`。
9. `is_transaction_control` 把 `UNWATCH` 也列为控制命令（立即执行），真 Redis 是入队。
10. **AOF 里 `SET ... PX` 是相对时间**：live append 原样写命令，replay 时会从 replay
    时刻重新计时。base 序列化用 `PXAT` 规避了 rewrite，但普通 append 还没改写。彻底修需
    在写 AOF 时把 `PX` 转成 `PXAT`（或记录绝对过期）。
11. **自动 rewrite 的开销**：每条写命令都 `read_to_string(manifest)` + `metadata`，
    有额外 syscall；可加计数器或周期检查。
12. **BGREWRITEAOF 打开新 incr 失败**时仍会继续（可能删掉旧的 active incr）；
    应提前 return 并复位 flag。
13. `aof_last_bgrewrite_ok` 若后台闭包 panic 不会复位（当前 fs 错误都走 Result）。
14. 拼写/命名：`Db.replic_acks` 等可统一。

## 9. 测试基础设施（`tests/pipelining.rs`，黑盒 raw `TcpStream`）

- `lock_server()`：全局 `Mutex`，串行化所有测试（默认端口 6379 共享）。
- `TestCtx::new()`：拿锁 + 起真实 server 子进程（无参，默认 6379）+ 一个 `Client`。
- `Server::start_on(addr, extra_args)`：任意地址起服务器（自动加 `--port`），轮询直到可连；
  spawn 前探测端口。
- `Client::connect_to(addr)`：`cmd` / `raw` / `expect` / `expect_prefix` / `expect_silent`
  / `read_line`。
- **mock-master**：测试自己 `TcpListener::bind("127.0.0.1:0")` 当主库，起副本指向它，
  断言副本发来的字节；`accept` 用非阻塞轮询 + deadline。
- AOF helper：`fresh_dir`、`setup_aof_dir`、`setup_aof_dir_manifest`、`free_addr`、
  `wait_for_rewrite`、`wait_until`、`info_persistence_lines`、`info_value`、
  `expect_bulk_pair`，以及 RDB 常量 `ONE_KEY_RDB` / `TWO_KEY_RDB` / `RDB_WITH_EXPIRY`。
- RESP 注意：`INFO` 文本以 `\r\n` 结尾，bulk string 编码又加一个 `\r\n`；逐行读时要
  多消费一次空行，否则下一条回复会串味。

## 10. 怎么开始新 session

```sh
cd /home/dd/lab/rust-lab/codecrafters-redis-rust
cargo test                    # 应全绿（21 lib + 4 bin unit + 292 integration）
cargo check --all-targets     # 0 warning
codecrafters task -stage bq9  # 看下一 stage 官方说明
./your_program.sh             # 另开终端手动试
```

当前 git 状态：干净（最新提交 `b1e3fe6`）。
