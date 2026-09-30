# LESSON.md — 经验与教训汇总

> 项目：CodeCrafters "Build Your Own Redis"（Rust）。
> 覆盖范围：la7（SET/GET）→ gu5（LPUSH），以及测试基础设施重构。
> 最后更新：2026-09-27。

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
- 解析整数参数：`std::str::from_utf8(bytes).ok()?.parse().ok()`（可能非 UTF-8）。
  负索引用 `i64`，非负可用 `usize`。

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

---

## 7. 一句话原则

1. **不确定就问/查官方 spec**，别凭直觉（`codecrafters task -stage`）。
2. **改代码前后都跑测试**，并且让新测试在旧实现上先红。
3. **不 panic**，尤其在持锁/切片/解析时。
4. **资源与锁的释放顺序**要显式设计（Rust 是字段声明顺序）。
5. **写下来的教训才算学到。**
