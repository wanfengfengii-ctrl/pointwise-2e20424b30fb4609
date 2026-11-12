# 迁移与恢复验证说明

## 命令清单

```bash
# 旧库（schema1）保持原行为：enqueue / work(SDK1) / read 不变
python3 run.py --db archive.sqlite enqueue JOB PACKET REVISION
python3 run.py --db archive.sqlite work JOB samples/source.json
python3 run.py --db archive.sqlite read

# 原子迁移 schema1 -> schema2（幂等，成功后 PRAGMA user_version=2）
python3 run.py --db archive.sqlite migrate

# 单步推进（每次至多一次 pull）；--remote 为独立持久远端状态，v1 端点禁用
python3 run.py --db archive.sqlite step JOB samples/source.json \
    --remote remote.sqlite --worker W --now 100 --ttl 30

# 迁移后 work 改用 SDK2 推进至同一原始回执；
# 远端状态固定使用 archive.sqlite.remote.sqlite，重启后续用而非重置
python3 run.py --db archive.sqlite work JOB samples/source.json

# 验证与演示
python3 checks.py verify
python3 checks.py demo
```

## schema2 结构

- `objects(digest TEXT PRIMARY KEY, body BLOB NOT NULL)`：完整载荷唯一存放处，digest 为 body 的 SHA256。
- `refs(packet,path,media,digest)`：每包每路径的引用，media 独立保存；同一 digest 可被多包、多路径、不同 media 共享。
- `heads(packet,revision,digest)`：当前已发布修订与包摘要；`queue` 为待办请求；`receipts` 原样保留；`meta.archive_id` 不变。
- `transfers / segments / done_files`：续传状态（会话、游标、暂存段、已校验文件、租约、失败原因）。

迁移在单个 `BEGIN IMMEDIATE` 事务内完成：载荷以 CHUNK 为单位经 `blobopen` 流式搬运并按 digest 即时去重，随后 `DROP TABLE files/packets/jobs` 并置 `user_version=2`。旧表被删除是数据库级防线：迁移前构造的旧进程读取既有 receipts 仍可返回，但其任何迟到的旧式写 SQL（files/packets/jobs）都会失败并整体回滚，不依赖新构造函数的版本检查。

## 恢复不变量

- consumer 恒为 `canonical([archive_id, job])`，重启/接替/重试不变，session 永不重建；operation 名按游标确定（`op-N`）。
- 每段先持久化到 `segments` 再 ack；ack 幂等可无限重试。崩溃后凭 `status` 对账：远端 pending 与本地 `pending_op` 比对——本地有段则补 ack；本地缺段（pull 提交后响应丢失）则以同一 operation/cursor 重取完全相同响应；已 ack 段绝不补抓。
- 文件全部段到齐后，在同一事务内流式校验 manifest 的 size 与 sha256，通过才写入 `objects`（已存在则共享）并记入 `done_files`；全部文件校验合格后，`refs` 与成功回执才在同一事务原子可见；低修订迟到只写 superseded 回执，不回退 head。
- 校验失败（零字节、截断、偏移、摘要不符）抛 `ValueError("CONTENT_MISMATCH")`，失败原因持久化于 `transfers.failed`，当前包不变，后续调用持续失败。
- 租约为每次获取生成的随机 token（非 worker 名）：每个本地写事务先校验 token；正常结束释放租约，异常退出保留至 `lease_until`，截止点 `<= now` 时才可被接替；接替后失效在途调用的写被守卫拒绝，不能改写进度或发布。
- 网络等待不持有本地写事务或全局锁：open/status/pull/ack 全部在事务外，不同包可并行推进。
- 迁移与续传的 Python 载荷驻留量与 CHUNK 成正比（实测 8MiB 单文件迁移峰值约 2.1×CHUNK、续传约 9×CHUNK），不随文件大小增长。

## 验证覆盖（checks.py）

- `normal` / `sdk`：既有 schema1 回归与 SDK2 协议断言，原样保留。
- `migration`：旧库升级前后 read 完全一致；archive_id、绑定、旧回执、待办保留；对象去重；旧表消失；旧进程迟到写入失败且库内容不变；SDK1 run 在 schema2 上被拒绝。
- `recovery`：open/pull/ack 提交后丢响应、持久化边界中断、租约 busy 与过期接替、在途失效调用写拒绝、迟到低修订 superseded、内容校验失败持久化、多大段/整 CHUNK/空文件/空包保真。
- `contention`：同名 worker 双连接并发同包安全且回执唯一；不同包单连接并行完成；共享内容单对象多 media 引用。
- `cli`：真实子进程覆盖 enqueue/migrate/step/work/read、step 自动迁移、work 重启后续用持久远端游标而非重下。
- `memory`：供应商走真实 HTTP 子进程，归档进程峰值 < 16×CHUNK（8MiB 包）。
