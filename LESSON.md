# LESSON.md — 经验与教训汇总

> 项目：CodeCrafters "Build Your Own Redis"（Rust）。
> 覆盖范围：la7（SET/GET）→ na2（WAIT），含模块化重构、Transactions、Optimistic
> Locking 与 Replication。
> 最后更新：2026-10-14。

---

## 1. Rust 语言 / 借用检查

### 1.1 enum variant 不能单独加方法

`impl Value::List { ... }` 不合法。方法只能加在 enum 本身（内部 `match`），
或把 variant 的载荷包成一个 newtype。

```rust
impl Value {
    fn rpush(&mut self, elem: Vec<u8>) -> Option<usize> {
        match self {
            Value::String(_) => None,
            Value::List(list) => { list.push_back(elem); Some(list.len()) }
        }
    }
}
```

返回 `Option`/`Result` 而不是 panic，把「类型不对」交给调用方决定怎么回错。

### 1.2「取或插入，并返回引用」用 `entry().or_insert_with()`

不能从分支里返回指向局部的引用（E0382 borrow-after-move / E0597 生命周期不够）：
一个分支把局部 move 进 map，另一个分支还想返回它的借用，编不过。
统一改成：

```rust
let entry = store
    .entry(key.to_vec())
    .or_insert_with(|| Entry::new(Value::List(VecDeque::new())));
```

`entry()` 返回的 `&mut Entry` 生命周期绑定在 map 上，两个场景合并成一条路径。

### 1.3 `MutexGuard` 不能跨 `.await`

`std::sync::MutexGuard` 是 `!Send`，跨 `.await` 会编译失败。
把加锁范围收进一个 block，写完再 await：

```rust
let reply = {
    let mut guard = self.store.lock().unwrap();
    execute(&val, &mut guard)
}; // guard 在这里 drop，之后才 await
if let Some(reply) = reply {
    self.stream.write_all(...).await?;
}
```

### 1.4 Mutex 中毒会让**整个**服务器瘫痪

若 panic 发生在持锁期间，锁被 poison，之后所有 `lock().unwrap()` 都 panic，
所有连接、所有命令全部失效。防御式写法：

```rust
let mut store = self.store.lock().unwrap_or_else(|e| e.into_inner());
```

但根本办法是**不要 panic**（见 1.5）。

### 1.5 切片前先做边界检查

- `&list[start..stop + 1]` 在 `start > stop + 1` 时 panic。
- `len - 1` 在空集合上下溢（debug panic / release wrap）。

顺序：先判空 → 再判 `start >= len || start > stop` → 最后用包含区间 `start..=stop`。
这样可以覆盖 Redis 的四种边界（空、start 越界、start > stop、stop 越界）。

### 1.6 函数指针不能捕获环境

`type Handler = fn(&[Frame], &mut Store) -> Frame` 是**裸函数指针**，
捕获了变量的闭包不能强转。把变化量当**参数**传：

```rust
fn push_impl(..., push: fn(&mut VecDeque<Vec<u8>>, Vec<u8>)) -> Frame { ... }
fn cmd_rpush(...) -> Frame { push_impl(..., VecDeque::push_back) }
fn cmd_lpush(...) -> Frame { push_impl(..., VecDeque::push_front) }
```

### 1.7 struct 字段按声明顺序（从上到下）drop

**Drop 顺序 = 字段声明顺序**，不是构造顺序。当「资源 + 锁」组合时，
要先把「需要先释放的资源」声明在前面：

```rust
struct TestCtx {
    _server: Server,                     // 先 drop：杀进程、释放端口 6379
    _guard: MutexGuard<'static, ()>,     // 后 drop：释放全局锁
    client: Client,
}
```

反过来（`_guard` 在前）会导致：锁先释放 → 下一个测试抢到锁 →
上一个 server 进程还没死 → `Server::start()` 报 `:6379 already in use`。
这个 bug 当场让 26 个测试失败。**教训：资源与锁的 drop 顺序要显式设计。**

### 1.8 `f64` 解析陷阱：`nan` / `inf` 能解析成功，`from_secs_f64` 会 panic

`"nan".parse::<f64>()` 和 `"inf".parse::<f64>()` 都返回 `Ok`，
但 `Duration::from_secs_f64(NaN)` **会 panic**（`inf` 也一样）。

校验浮点输入用 **`is_finite()`**，一个条件同时挡掉 NaN 和 ±inf：

```rust
let Ok(secs) = raw.parse::<f64>() else { return Err(...) };
if !secs.is_finite() || secs < 0.0 {      // 不是 || is_infinite() || is_sign_negative()
    return Err(...);
}
let d = Duration::from_secs_f64(secs);
```

顺带：`(-0.0f64).is_sign_negative()` 为 `true`，会把 `-0` 误判为负；
用 `secs < 0.0` 更准（`-0.0 < 0.0` 为 `false`）。

**通用原则**：任何可能 panic 的标准库函数（`from_secs_f64`、切片、`unwrap`、
窄化转换）在调用前都要做完整校验；尤其别在**持锁路径**上 panic（见 1.4）。

### 1.9 一次性唤醒用 `oneshot`，不要用 `mpsc` / `Notify`

「叫醒某个等待者一次」的需求（如 BLPOP 等 push）用 `tokio::sync::oneshot`：

- `oneshot::channel::<()>() -> (Sender<()>, Receiver<()>)`。
- `Sender::send(v)` 是**同步**函数（不是 async）——可以在持有
  `std::sync::Mutex` 时调用，这是它胜任的关键。`Receiver` 实现 `Future`，
  `await` 得到 `Result<(), RecvError>`。
- 接收端被 drop（超时/断线）后，`send` 返回 `Err`，发送方能感知并跳过。

为什么不是别的原语：

| 需求 | oneshot | mpsc | Notify |
|---|---|---|---|
| 每个等待者醒一次 | ✅ 天然 | 需 N 个 channel | 可以，但 FIFO 不保证 |
| `send` 同步（可持锁调用） | ✅ | ❌（`send` 是 async） | ✅ |
| 发送方感知接收方已死 | ✅ `Err` | `await` 得 `Err` | ❌ |
| 多消费者 | 每等待者一个 | 单消费者 | 多 |

`mpsc` 是「多生产者 → **单消费者** → 多条消息」，方向不对：N 个等待者
需要 N 个 consumer，只能开 N 个 channel，等价于 N 个 oneshot 还更重。
`Notify` 不保证 FIFO，也不知道谁被唤醒。

---

## 2. RESP 协议

- **命令参数永远是 bulk string**。客户端发 `SET k v PX 100` 时，
  `100` 是 `$3\r\n100\r\n`，不是 RESP 整数；服务端要自己解析 ASCII 数字。
- **`:` 整数是「回复」类型**（`Frame::Int(i64)`，如 `:1\r\n`）。请求里不会出现。
- **null bulk string 是 `$-1\r\n`**，必须是**独立的 variant**（`Frame::Null`）。
  不能用空 bulk（`$0\r\n\r\n`），更不能把文本 `$-1\r\n` 当内容塞进 bulk
  （那会得到 `$6\r\n$-1\r\n\r\n`）。
- **错误要带错误码**。`Frame::Err` 不应硬编码 `ERR ` 前缀，应原样输出 message，
  由调用方给 `"ERR ..."` / `"WRONGTYPE ..."`。否则 WRONGTYPE 会错成
  `-ERR WRONGTYPE ...`（Redis 是 `-WRONGTYPE ...`）。
- **binary-safe**：key/value 一律 `Vec<u8>`，不要转 `String`；值里可以含 `\r\n`。
- **null array 是 `*-1\r\n`**（`Frame::NullArray`），和 `$-1\r\n` 是两回事：
  `LPOP key`（无 count）在缺失时用 `$-1`；`LPOP key <count>` 在 key 缺失时用 `*-1`。
- 解析整数参数：`std::str::from_utf8(bytes).ok()?.parse().ok()`（可能非 UTF-8）。
  负索引用 `i64`，非负可用 `usize`；小数（超时秒数）用 `f64`（见 1.8）。

---

## 3. 服务器设计

### 3.1 跨连接共享状态

`Arc<Mutex<HashMap<...>>>`，在 `main` 建一次，`Arc::clone` 进每个 `tokio::spawn`。
每个连接一个 task。

### 3.2 key 的值是 tagged union

```rust
enum Value { String(Vec<u8>), List(VecDeque<Vec<u8>>) }
struct Entry { value: Value, expires_at: Option<Instant> }
```

类型不符返回 WRONGTYPE。**不要**为每种类型开一个独立 HashMap——
Redis 是单一 keyspace，一个 key 只能有一种类型。

### 3.3 TTL 用 `Instant`（单调时钟），不用 `SystemTime`

`SystemTime` 会被系统时钟调整影响；`Instant` 不会。
用 `Instant::now().checked_add(Duration::from_millis(ms))` 防溢出。

### 3.4 惰性过期很容易漏

`GET` 过期时返回 null，但**不删除** entry；`RPUSH` 必须把过期 key
当作不存在，否则会往「死」entry 里追加。这种规则别在每个 handler 重复，
抽一个 `live_entry(store, key)` 统一处理（命中即删）。

### 3.5 列表用 `VecDeque`

`push_front` / `push_back` 都是 O(1)。代价：`VecDeque` **不支持 range 索引**，
`LRANGE` 要用 `list.range(start..=stop)`（`Vec` 才支持 `&v[a..=b]`）。

### 3.6 负索引归一化照 Redis

```
if start < 0 { start += len; }
if stop  < 0 { stop  += len; }
if start < 0 { start = 0; }          // 只夹 start
if start > stop || start >= len { 空 }
if stop >= len { stop = len - 1; }
```

注意：**stop 不夹到 0**。`LRANGE k 0 -6`（长度 5）里 stop 归到 -1，
`start > stop` → 返回空数组（而不是第一个元素）。

### 3.7 阻塞命令不能是同步 handler

`type Handler = fn(&[Frame], &mut Store) -> Frame` 是同步的，无法 `.await`。
`BLPOP` 必须等待，所以在**连接循环里特判**（本例放在 `generate_reply`）：
识别到 `BLPOP` 就 `blpop(...).await`，其余仍走 `execute`。

```rust
async fn generate_reply(&mut self, f: &Frame) -> Option<Frame> {
    if let Frame::Array(args) = f {
        if bulk(args.get(0)) == Some(b"BLPOP") {
            return Some(blpop(args, self.store.clone()).await);
        }
    }
    let mut store = self.store.lock().unwrap_or_else(|e| e.into_inner());
    execute(f, &mut store)
}
```

备选（更干净、改动更大）：让 `execute` 返回 `enum Reply { Now(Frame),
Block { keys, timeout } }`，由循环统一 await。命令多了再升级。

### 3.8 等待者队列 + 唤醒（FIFO）

把等待者放进和数据**同一个** mutex 保护的 struct，这样「push 后通知」和
「检查-注册等待」都原子：

```rust
struct Db {
    data: HashMap<Vec<u8>, Entry>,
    waiters: HashMap<Vec<u8>, VecDeque<oneshot::Sender<()>>>,
}
```

- **注册**：加锁 → 列表为空则把一个 `oneshot::Sender` 放进 `waiters[key]` 尾部
  → 释放锁 → `await rx`。
- **唤醒**：`RPUSH` 每入队一个元素，就在持锁状态下从队头弹一个 Sender 并
  `send(())`；`send` 失败（接收端已死）就继续弹下一个。
- **FIFO** 来自 `pop_front`：最久等待者先被服务（Redis 语义）。
- 唤醒后 waiter **重新加锁检查列表**——通知只是「可能有货」，不是交接数据。

**坑**：通知要按**元素个数**发，别一次唤醒所有人，否则被唤醒者发现无货
又会重新排队、丢失 FIFO 位置。

---

## 4. 测试

- **黑盒**：用 raw `TcpStream` 发字节、断言字节，精确控制分包/粘包，
  断言原始 RESP。
- **每个新测试先确认在旧实现上失败**（HANDOFF §6.2），否则它证明不了任何东西。
  做法：临时把实现改回 buggy 版本，跑测试确认 fail，再改回来。
- **只读你断言的长度**：`read_at_least(want)` 里 `want` 若大于实际回复，
  会白等一整个超时（曾让套件慢 2s）。把 `want` 设成 `expected.len()`。
- **端口硬编码 6379 → 全局 `OnceLock<Mutex<()>>` 串行化**。
  `Server::start()` 在 spawn 前先探测端口，被占直接 panic
  （防止悄悄测到残留的旧 server，HANDOFF §6.5）。
- **封装 `TestCtx` / `Client`**：把「加锁 + 起 server + 连接 + 编码 + 读 + 断言」
  压成一行，测试读起来像命令/断言对：
  ```rust
  let mut t = TestCtx::new();
  t.client.cmd(&[b"RPUSH", b"k", b"a"]).expect(b":1\r\n");
  ```
- 别留 `dbg!` / `println!`（污染服务端日志、泄露数据）。
- 读超时返回「已收到的内容」而不是 hang，失败时能看到有用的 diff。
- **阻塞命令照样用同步测试**：服务端的 async 不传染给客户端。多开几条连接
  （`t.connect()`），按「先 BLPOP → `expect_silent` 证明阻塞 → 另一条连接
  `RPUSH` → 读回唤醒结果」单线程交错即可，不需要 `#[tokio::test]`。
- **`expect_silent` 兼作「注册顺序」的同步点**：写完 c1 的 BLPOP 先沉默一会儿，
  确保它入队，再发 c2 的 BLPOP，FIFO 才有确定性。

---

## 5. CodeCrafters 工作流

- **`codecrafters task -stage <slug>`** 可以看任意 stage 的官方说明（当前或未来）。
- **`codecrafters test`** 只能跑「当前」stage（还有 `--previous`），不能指定 stage。
- **平台进度落后于本地**：只有 `codecrafters submit` 才会推进远端 stage。
  本地写了很多但没提交，`codecrafters task` 会停在很靠前的 stage。
- 官方顺序：**7 个核心 stage 之后接各 extension**
  （lists → streams → transactions → … → authentication）。
  已整理在 `STAGES.md`（124 个 stage，按平台真实顺序）。
- `your_program.sh` 已经转发 `"$@"`，CLI 参数能进到程序。

---

## 6. Bug 复盘（都真实踩过）

| 症状 | 根因 | 教训 |
|---|---|---|
| `GET` 不存在返回 `$6\r\n$-1\r\n\r\n` | 把文本 `$-1\r\n` 当内容塞进 bulk | null bulk string 是独立类型 |
| `RPUSH` 到 string key 返回 `:0` | `len.unwrap_or(0)` 把 WRONGTYPE 吞成 0 | 错误别用默认值掩盖 |
| LRANGE 越界后整个服务器卡死 | 切片 panic 发生在持锁期间 → Mutex 中毒 | 切片前做边界检查；别 panic 持锁 |
| 过期后 `RPUSH` 返回 `:2` | 分支先 `List(vec![v])` 再 `rpush(v)`，push 了两次 | 改分支后重读一遍逻辑 |
| `RPUSH k a b c` 返回 push 个数 | 应返回列表**总长度** | 命令语义别凭直觉，照 spec |
| 新测试在旧实现上也通过 | 断言太弱 / 巧合 | 先验证测试在旧实现上失败 |
| `cargo test` 混着 `AddrInUse` 却 ok | 残留旧 server 占着端口 | spawn 前探测端口，被占就 panic |
| `LPOP k 1` 返回 bulk 而非数组 | 用「弹出了几个」决定回复格式 | 该用「有没有给 count」决定（`Option`） |
| `LPOP k 0` 返回 `$-1` | 同上，且 `0..0` 被当成无结果 | 有 count 一律返回数组（含空数组） |
| `BLPOP k nan` 把连接打挂 | `"nan"` 能 parse 成 `f64`，`from_secs_f64(NaN)` panic | 浮点先 `is_finite()`，别让库函数 panic |
| `BLPOP k 1` 只等 1ms | 超时单位是秒，却用了 `from_millis` | 单位写进名字/注释，并写测试锁住 |
| `BLPOP` 到 string key 永久阻塞 | 类型判断用 `if let Value::List`，String 漏下去注册了等待 | 用 `match`，不匹配的分支明确回错 |

---

## 7. 模块化重构 / 事务 / 乐观锁 / 复制（2026-10 追加）

### 7.1 lib 与 bin 是两个 crate

同一 package 里 `src/lib.rs` 和 `src/main.rs` **各自是独立的 crate root**，`mod` 不跨 crate。
`main.rs` 要用库里的类型，必须 `use <package_name>::...`，且路径上每一级都要 `pub`
（`pub(crate)` 对 binary crate 不可见）。

不要在两个 crate root 都写 `mod resp;`——`resp.rs` 会被编译两遍，产生**两个不同的 `Frame`
类型**，`db::Entry` 里装的是库的 Frame，main 传的是 bin 的 Frame，全是类型错误。

推荐形态：库拥有全部模块，只 `pub` 出 `Config` + `run(config)`；`main.rs` 只做 CLI 解析。
可见性口诀：`pub(crate)` = 本 crate 内跨模块；`pub` = 对外（另一个 crate 也能用）。

### 7.2 错误类型化 & 统一返回值

`RedisError { message: String }` 集中所有 wire 消息（每种一个构造器）。
**错误文案必须逐字节不变**——几百个集成测试精确断言错误行，改一个字就可能全红；
重构时若改了文案又顺手改测试断言，会把回归藏起来。

统一用 `type CmdResult = Result<Vec<Frame>, RedisError>`：一条命令回 0..N 帧，
**空 vec 表示「无回复」**（`*0\r\n`、`REPLCONF ACK`），比 `Option` 更统一。handler
返回 `Result` 后，边界处再统一转 `Frame::Err(e.message)`，`?` 才真正省代码。

### 7.3 把「每连接状态」抽成 `Conn`

`Handler = fn(&[Frame], &mut Conn, &mut Store) -> CmdResult`。`Conn` 装
`id / subscriptions / out_tx / multi / watched / from_master`。这样 `SUBSCRIBE`、`MULTI`、
`WATCH`、只读副本判断都能在 handler 层拿到连接状态，不必把逻辑堆进 `StreamHandler`。
`&mut conn` 与 `&mut store` 是 `StreamHandler` 的**不相交字段**，可同时可变借用。

### 7.4 事务：入队与 EXEC

- MULTI 后，`execute` 把所有**非控制命令** push 进 `conn.multi` 并回 `+QUEUED`；控制命令
  `MULTI/EXEC/DISCARD/WATCH/UNWATCH` 正常走 handler。
- `EXEC` 用 `conn.multi.take()` 清队列（否则 EXEC 后连接还停在 MULTI），逐条递归
  `execute`，返回 `Frame::Array(各命令回复)`；单条失败塞 `Frame::Err` 不中断。
- 入队检查要放在 `lookup` **之前**（否则 `BLPOP` 这种不在 lookup 里的命令会变成 unknown）。

### 7.5 WATCH：追踪「被改过」，不比较值

官方 Notes 给了两条硬规则：
- **不能比较值**：改完再改回原值也要 abort → 用版本号。
- **不能全局 dirty**：只有被 watch 的 key 变了才 abort → 必须按 key。

实现：`Db.versions: HashMap<key, u64>` + `version_seq`，`Db::touch(key)` 命中即 bump；
`Conn.watched: HashMap<key, u64>` 在 WATCH 时记版本；EXEC 比对，任一不同 → 返 `*-1`。
`touch` 只在**真的改到了**时调（`LPOP k 0`、`ZREM 不存在的成员` 不 touch，否则假 abort）。
EXEC/DISCARD 后都清 watched。

### 7.6 主从复制：主库侧

- `Db.replicas: HashMap<conn_id, Outbox>`。副本发 `PSYNC ? -1` 时把该连接的 `out_tx` 登记
  进来；`cmd_psync` 回 `+FULLRESYNC <replid> <offset>\r\n` + 空 RDB。
- 空 RDB 用 `Frame::Rdb(Vec<u8>)` 编码成 `$<len>\r\n<bytes>`（**没有尾 CRLF**，和普通
  bulk string 不同）。
- 写命令成功后，`dispatch::execute` 把原始 `Frame::Array(args)` 发给每个 replica 的 outbox，
  并 `master_repl_offset += cmd.frame_len()`。
- `REPLCONF ACK <offset>` → 记 `replica_acks[conn_id] = offset`，**不回响应**。
- `WAIT <n> <timeout>`：目标 = 当前 `master_repl_offset`；给所有副本发 `REPLCONF GETACK *`，
  轮询 `replica_acks` 里 `>= target` 的数量，达标或超时就返回该数量。

### 7.7 主从复制：副本侧

握手：`PING`（等 `+PONG`）→ `REPLCONF listening-port`（等 `+OK`）→ `REPLCONF capa psync2`
（等 `+OK`）→ `PSYNC ? -1`（读掉 `+FULLRESYNC`）→ 读 `$<len>\r\n` + RDB。
之后 `Decoder` 循环解析主库命令流：`REPLCONF GETACK *` 回 `REPLCONF ACK <offset>`；其它命令
用一个 `from_master=true` 的一次性 `Conn` 调 `execute`（**忽略返回 = 不回响应**）。副本本地维护
`offset`（处理过的字节数），GETACK 自身不计入**当次** ACK。

只读副本：客户端写 → `-READONLY You can't write against a read only replica.`；复制链接
（`from_master`）豁免。

### 7.8 复制里踩过的坑

- **`WAIT` 不能是同步 handler**：它要等 ACK，而 ACK 由**别的连接 task** 处理、也要抢
  `store` 锁。持锁等待会和它死锁。必须像 `BLPOP` 一样在连接循环里特判 async，且每次
  `lock()` 只做一小段、立刻释放。
- **`WAIT <n>` 的返回**：一旦 acked ≥ n 就返回**当前** acked 数（不是等到 n）。所以 2 个
  副本下 `WAIT 1` 返回 `:1` 是正常的；要确定性等于 2 就测 `WAIT 2`。
- **`+FULLRESYNC` 被读两次**：`send_and_read_reply(PSYNC)` 已经消费了那一行，再补一个
  `read_line` 会读走 `$<len>\r\n`，导致 RDB 长度解析读到二进制垃圾而失败。
- **读定长二进制**：`read_exact(&mut rdb)`，或 `read(&mut rdb[got..])`。写成
  `read(&mut rdb)` 每次都从 index 0 覆盖，分片时数据错位。
- **`Decoder` 只解析请求数组**（`*...$...`）。主库的 `+PONG`/`+OK`/`+FULLRESYNC` 是
  simple string，`Decoder` 返回 `None` → 握手阶段要手动逐字节 `read_line`。

### 7.9 二进制常量与字面量

空 RDB 这类二进制：`base64 -d | xxd` 解码后硬编码 `b"..."`。**坑**：Rust 字符串字面量
**可以跨行，且会把换行和续行缩进算进内容**（除非行尾 `\`）。我们把 88 字节的空 RDB 写成
跨行字面量，实际变成 90 字节，而测试因为**复制了同一份错误常量**而假绿。教训：二进制常量
走**单一权威表示**（hex/base64 还原），别在测试里手抄。

### 7.10 测试复制的技巧

- 泛化基座：`Server::start_on(addr, extra_args)` + `Client::connect_to(addr)`，才能在一台
  机器上起「主库 + 多个副本」。
- **mock-master**：测试自己 `TcpListener::bind("127.0.0.1:0")` 当主库，起副本指向它，断言
  副本发来的字节；`accept` 用非阻塞轮询 + deadline，避免永久 hang（std 的 `accept` 无超时）。
- **挑空闲端口**：`TcpListener::bind(":0").local_addr()`，那个临时 listener 在语句结束即 drop，
  端口归还，子进程随即绑定。用 `127.0.0.1:1` 这类「死端口」可让副本握手快速失败但仍保留副本角色。
- 跨进程有竞态：用轮询（GET/WAIT）或短 sleep，别假设「发完立刻能读到」。
- `assert_*` helper 要**按预期长度精确读**，否则可能把后续字节吞进断言缓冲。
- `dbg!`/`println!` 在测试里默认被 libtest **捕获**，`cargo test -- --nocapture` 才显示；
  `dbg!` 的输出格式带 `文件:行:列`。

### 7.11 编辑器 / 工作流

- VSCode rust-analyzer 的 **Organize Imports**（`editor.codeActionsOnSave` 里的
  `source.organizeImports`）会在保存时把同 crate 的 `use` 合并成**一整行**，和 rustfmt
  （默认会换行）行为相反。要保留换行就把 `source.organizeImports` 设成 `never`。
- 纯搬移 / 纯格式化与语义改动**分开 commit**；重构要逐字节行为不变，别顺手改文案。

---

## 8. 持久化：RDB 读 & AOF（2026-10 追加）

### 8.1 RDB：这个挑战只考「读」

RDB 组（#69–74）全是加载：启动时读 `{dir}/{dbfilename}` 到内存。**没有写 RDB / SAVE /
BGSAVE 的 stage**；复制握手用的 RDB 可以硬编码为空。

RDB 结构（按顺序）：
1. 头部 `"REDIS0011"`（9 字节）。
2. metadata：`0xFA` + 名字 string + 值 string。
3. database：`0xFE` + db index（size 编码）；`0xFB` + 两个 size（key 数、带过期 key 数）；
   随后是 key-value。
4. 每个 key-value：可选过期（`0xFD` + 4 字节 LE 秒 / `0xFC` + 8 字节 LE 毫秒）→ 类型字节
   （`0x00`=string）→ key（string）→ value。
5. `0xFF` + 8 字节 CRC64（可忽略）。

编码：
- **size 编码**：首字节高 2 位 `00` → 低 6 位；`01` → 14 位**大端**；`10` → 后 4 字节
  **大端**；`11` → string 特殊编码。
- **string 编码**：size 编码的长度 + 原始字节；`0xC0/0xC1/0xC2` → int8/int16/int32
  （**小端**），要转成十进制 ASCII 存成字符串；`0xC3` 是 LZF，挑战不出现。
- **坑**：size/容器长度是**大端**，过期时间戳和整数内容是**小端**，别搞混。

过期：RDB 里是**绝对 Unix 时间戳**，而 `Entry.expires_at` 是 `Instant`。加载时换算剩余毫秒；
已过期的 key 直接丢弃（GET → `$-1`，KEYS 也不列出）。

解析要防御式：`take` 用 `.get(pos..end)?` 而不是切片索引（越界 panic 会让服务器**启动即崩**）；
每个 opcode 后面该消费多少字节必须严格对齐，否则整条流错位。

### 8.2 AOF：Multi-Part AOF 与 manifest

配置默认值：`appendonly=no`、`appenddirname=appendonlydir`、`appendfilename=appendonly.aof`、
`appendfsync=everysec`，`dir` 默认启动时的 cwd（绝对路径）。

Redis 7 的 AOF 不是单个文件，而是多段：
- **base**（`type b`）：上次 rewrite 的快照（通常是 RDB 格式）。
- **incr**（`type i`）：快照之后追加的写命令（`<appendfilename>.<seq>.incr.aof`）。

`<appendfilename>.manifest` 是**索引**：列出有哪些 AOF 文件、顺序（`seq`）和类型（`b`/`i`），例如：

```
file appendonly.aof.1.base.rdb seq 1 type b
file appendonly.aof.1.incr.aof seq 1 type i
```

用途：
1. **启动恢复**：先加载 base，再按 seq 顺序 replay 各个 incr 文件。
2. **原子切换**：rewrite 时先写新文件，再「临时 manifest → rename」成正式 manifest；rename 原子，
   保证任何时刻 manifest 指向一组完整一致的文件（crash-safe）。
3. **识别陈旧文件**：manifest 未列出的 `.incr.aof` / `.base.rdb` 是旧的/孤儿，可清理。

挑战里的简化版：只有一行（`.1.incr.aof seq 1 type i`），没有 base、没有 rewrite、没有真正的
rename。创建时机必须在**绑定端口之前**（`Server::start_on` 返回时文件已存在），顺序：

```
create_dir_all(appenddir)
→ 创建空 incr 文件（OpenOptions::new().create(true).append(true)，不截断，为以后 replay 保留内容）
→ fs::write manifest（单空格分隔、结尾换行）
```

---

## 9. 一句话原则

1. **不确定就问/查官方 spec**，别凭直觉（`codecrafters task -stage`）。
2. **改代码前后都跑测试**，并且让新测试在旧实现上先红。
3. **不 panic**，尤其在持锁/切片/解析时。
4. **资源与锁的释放顺序**要显式设计（Rust 是字段声明顺序）。
5. **校验所有外部输入**再调用可能 panic 的 API（`from_secs_f64`、切片、`unwrap`）。
6. **一次性唤醒用 `oneshot`**；顺序自己用 `VecDeque` 管。
7. **写下来的教训才算学到。**
