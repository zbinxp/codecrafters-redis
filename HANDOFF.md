# 交接文档 — codecrafters Redis (Rust)

> 生成于 2026-09-23。这是一个**学习 session**：用户自己动手写代码，助手只做 review / 挑毛病，不代为修改实现。
> 本文件在仓库根目录，`codecrafters submit` 会把它一起 push，不需要时可直接删除。

---

## 1. 项目

[codecrafters "Build Your Own Redis"](https://codecrafters.io/challenges/redis) 挑战，Rust 实现。
入口 `src/main.rs`，本地跑 `./your_program.sh`（内部 `cargo build --release` 后 exec）。

## 2. 进度

官方 stage 顺序（`jm1` → …）与当前位置：

| Stage | 名称 | 状态 |
|---|---|---|
| `jm1` | Bind to a port | ✅ |
| `rg2` | Respond to PING | ✅ |
| `wy1` | Respond to multiple PINGs | ✅ |
| `zu2` | Handle concurrent clients | ✅ |
| `qq0` | **Implement the ECHO command** | ✅ |
| `la7` | Implement the SET & GET commands | ⬅️ **下一步** |
| `yz1` | Expiry (`SET ... PX`) | 待办 |

> 注意：本仓库 README 里说的「stage 2 & beyond」和官方 slug 编号对不上，以官方 stage 名为准。

## 3. 当前代码状态

`src/main.rs`：
- `#[tokio::main]`，`tokio::net::TcpListener`（**真异步**，已从阻塞版 std 迁移过来）
- `listener.accept().await` → `tokio::spawn` per connection
- 每连接独立的 `buf: Vec<u8>`，累积字节后按 RESP 切分
- `parse_command(&buf) -> Option<(Vec<Vec<u8>>, usize)>`：解析出「参数列表 + 消费字节数」，不完整返回 `None`。参数是**原始字节**（二进制安全），不是 String
- `parse_header(buf, &mut pos, marker)`：解析 `*<n>\r\n` / `$<n>\r\n` 头
- 命令分发（**按字节比较**命令名，不转 String）：
  - `PING` → `+PONG\r\n`
  - `ECHO <msg>` → bulk string `$<len>\r\n<msg>\r\n`；缺参数 → `-ERR ...` 且**保持连接**
  - `COMMAND ...` → `+PONG\r\n`（只为哄 redis-cli，见 6.1）
  - 其它 → `-ERR wrong command <lossy>\r\n`（`from_utf8_lossy`，不 panic）
- 空数组 `*0\r\n` 被跳过（`continue`），不影响后续命令

`tests/pipelining.rs`：8 个集成测试，全绿。

## 4. 已验证的事实（含复现方式）

| 验证项 | 结果 |
|---|---|
| `cargo test` | 8 passed, 0 warnings |
| `echo -e "PING\nPING" \| redis-cli -p 6379` | `PONG` `PONG`，exit 0 |
| 100 个并发连接各发一条 PING | **100/100 全部回复**（迁移前是 20/100） |
| 5000 条 PING 一次性写入（70000 字节） | 35000 字节响应，全部正确 |

复现并发测试（`/tmp/load.py` 的思路）：开 N 个 socket 全部 `connect`，**每个都发 PING 后保持连接不关**，`sleep 3` 后统一读。关键点是连接要一直开着——这才是真实客户端的行为。

## 5. 待办

### 5.1 遗留问题（小，但真实）

- **缓冲区无上限**。客户端若只发半个命令就不再发数据，`buf` 会一直挂着；若持续发不完整数据则无限增长。真实 Redis 有 `proto-max-bulk-len` 限制。低优先级，但值得知道。
- **畸形输入会让服务端静默卡住**。`parse_command` 里所有 `?` 在解析失败时返回 `None`，于是「等更多数据」和「数据是垃圾」被混为一谈。比如发 `PING\r\n`（内联格式）或 `*x\r\n`，服务端会永远不回。到后面 stage 需要返回 `-ERR Protocol error` 时要处理。

### 5.2 下一阶段 `la7` — SET & GET

- 需要**跨连接共享状态**：`Arc<Mutex<HashMap<Vec<u8>, Vec<u8>>>>`（或 tokio 的 `RwLock`）。现在 `tokio::spawn` 的闭包里没有共享状态，加的时候注意 **lock 的持有范围不要跨 `.await`**（`std::sync::Mutex` 跨 await 会编译不过，这其实是好事，会强制你想清楚）。建议在 `main` 里建好 `Arc`，每次 accept 后 `clone` 进 task。
- `SET <key> <value>` → `+OK\r\n`。
- `GET <key>` → bulk string `$<len>\r\n<value>\r\n`；key 不存在 → **null bulk string** `$-1\r\n`（不是空字符串）。
- key/value 是任意字节，继续用 `Vec<u8>`，不要转 String。
- 参数个数校验同样要处理。

## 6. 踩过的坑（重要，别重复踩）

### 6.1 redis-cli 管道模式实际发的是 RESP 数组，不是内联命令

实测抓包（redis-cli 7.0.15），`echo -e "PING\nPING" | redis-cli` 的完整流量是：

```
*2\r\n$7\r\nCOMMAND\r\n$4\r\nDOCS\r\n     ← 连接时先发的握手，期待一个 map 响应
*1\r\n$4\r\nPING\r\n
*1\r\n$4\r\nPING\r\n
```

三条命令**挤在一个缓冲区里一起到达**，这是粘包的真实来源。

两个推论：
- **server 必须能响应 `COMMAND DOCS`**，否则 redis-cli 会段错误（实测：回畸形响应直接把 redis-cli 打崩）。目前回 `+PONG` 就够用，因为它只拿这个做 tab 补全元数据。
- `redis-cli --pipe` 模式会在数据末尾追加一条 `ECHO <随机token>` 来判定「响应都收完了」。**在实现 ECHO 之前，`--pipe` 必然超时**（exit 124）。这不是流水线的 bug，不要被误导。

### 6.2 「测试立刻通过」什么也证明不了

写「半包」测试时第一版是：发半个命令 → 等 100ms → 发剩下半个 → 再发一条完整命令 → 断言收到 2 个 PONG。
这个测试在**旧的错误实现**上也通过了——因为旧实现「每次 read 回一个 PONG」，碰巧凑出 2 个 PONG，纯属巧合。

改成用 `assert_silent` 断言「服务端对半截命令**必须保持沉默**」之后才有区分度。

**规矩：每个新测试都要先确认它在旧/错误实现上真的失败，再让它变绿。** 验证方法：把 `src/main.rs` 临时换成上一版（或写个 stub），跑测试确认 fail，再换回来。

### 6.3 并行测试会抢 6379 端口

服务端硬编码 `127.0.0.1:6379`，而 Rust 测试默认多线程并行跑。`tests/pipelining.rs` 用一把 `OnceLock<Mutex<()>>` 全局锁把测试串行化，每个测试开头 `let _guard = lock_server();`。**新加测试别忘了这行**，否则会随机失败。

### 6.4 关于 tokio 的一个反直觉点

之前把 server 用 `taskset -c 0` 钉在单核上测过，**1 个客户端是正常的**——`#[tokio::main]` 的 `block_on` 不占用 worker 线程，所以主线程阻塞在 accept 上并不会饿死后台 task。
真正的症状是**吞吐被钉在「核数」量级**（阻塞式实现下 20 核机器 100 并发只服务 20 个），不是死锁。别按「单核会挂」去推理。

### 6.5 残留的旧 server 会让测试「假通过」（实际踩到）

用 `./your_program.sh` 手动起的 server 如果没关（它 `exec` 到 `/tmp/codecrafters-build-redis-rust/release/codecrafters-redis`），会一直占着 6379。此时 `cargo test`：

- `Server::start()` spawn 的新二进制因 `AddrInUse` panic 退出；
- 但 `TcpStream::connect(ADDR)` **照样成功**（连到旧 server）；
- 于是测试继续跑，**测的是旧二进制**，而且不报错。

症状：`cargo test` 输出里混着 `panicked ... AddrInUse`，但测试结果却是 `ok`；或者同一段代码不同时候行为不一致（debug 时被这个坑了好几轮）。

修法：`Server::start()` 在 spawn 前先 `TcpStream::connect(ADDR)`，连得上就 panic（见 7）。排查时用 `ss -ltnp | grep 6379` 看谁占着。

## 7. 测试基础设施

`tests/pipelining.rs` 里可复用的部件：

| 部件 | 作用 |
|---|---|
| `lock_server()` | 全局锁，串行化测试（因为端口硬编码） |
| `Server::start()` | 启动真实 server 子进程（`env!("CARGO_BIN_EXE_codecrafters-redis")`）。**spawn 前先检查端口空闲**，被占则 panic（防止悄悄测到残留旧 server —— 曾踩过，见 6.5）；轮询时也检查子进程是否已退出。`Drop` 时 kill |
| `read_at_least(stream, want, timeout)` | 读到至少 N 字节或超时。**超时返回已收到的内容而不是 panic**，这样失败时能看到有用的 diff 而不是挂死 |
| `assert_silent(stream, timeout)` | 断言服务端什么都不发（用于半包场景） |
| `const PING` | `b"*1\r\n$4\r\nPING\r\n"` |

风格是**黑盒**：只通过原始 `TcpStream` 发字节、读字节，不碰内部函数。这样能精确控制分包/粘包，也能精确断言原始 RESP。

## 8. 环境

- Linux (WSL2), Rust `edition = "2024"`，codecrafters buildpack `rust-1.96`
- `nproc` = 20
- 已装 `redis-cli 7.0.15`（`/usr/bin/redis-cli`）
- `Cargo.toml` 依赖：`anyhow`、`bytes`、`thiserror`、`tokio`(features=["full"])。`bytes` 目前没用到。

## 9. 怎么开始新 session

```sh
cd /home/dd/lab/rust-lab/codecrafters-redis-rust
cargo test                    # 应全绿（8 passed）
./your_program.sh             # 另开终端起 server
echo -e "PING\nPING" | redis-cli -p 6379
```

当前 git 状态：`qq0` 完成时 `src/main.rs` + `tests/` + 本文件一起提交（见最新 commit）。
