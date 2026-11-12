# 监管资料 RPA 归档：已交付旧系统与供应商协议

本仓库是可运行的旧系统，不包含迁移实现。Python 3.11+ 标准库、SQLite；供应商两版客户端均以源码交付，无安装依赖。portal.py 是真实执行游标消费和回执删除的供应商参考服务，不是按测试输入返回预置答案的桩；既可运行 HTTP 传输，也可通过 transport=portal.dispatch 使用同一事务实现重放。供应商服务与本地归档使用独立数据库，不存在跨库事务。

## 当前入口、旧格式和样本

在仓库根运行：
- python3 run.py --db archive.sqlite enqueue JOB PACKET REVISION
- python3 run.py --db archive.sqlite work JOB samples/source.json
- python3 run.py --db archive.sqlite read
- python3 checks.py verify
- python3 checks.py demo

enqueue/work/read 成功向 stdout 输出一个 JSON 对象，错误输出 {"error":"原因"} 并退出2。对象字段顺序无关。首次打开空库初始化 schema1。Python 调用为 Archive(sqlite3.Connection)，enqueue(job,packet,revision)、同步 run(job,client,fail_at=None)、read()；调用前连接不在事务中。旧版 run 的故障点 before_receipt 抛出 RuntimeError("INJECTED")，整批回滚。旧版 Python run 的SDK1调用方式只用于schema1回归，升级后的work命令改用SDK2；Python read/enqueue及它们的结果保持兼容。

job、packet是非空字符串，revision是正整数，布尔值不作整数。job全库唯一。binding = SHA256(canonical([packet,revision]).encode("utf-8"))；canonical定义见portal.py。已成功job同绑定入队/执行返回第一次的原始回执，不再次产生效果；不同绑定为JOB_CONFLICT。未完成job同绑定重复入队为空操作，不同绑定同样冲突。回执检查先于重新访问供应商。

schema1的完整DDL见archive.DDL，完整可恢复旧库为samples/legacy.sql，用sqlite3.Connection.executescript读取即可。meta中archive_id是归档身份，不得升级时重新生成。packets保留每包当前revision和内容digest；files以(packet,path)为键，body为BLOB，media属于引用；jobs存未完成请求；receipts存绑定与首次结果。digest是按path排序后，每件{path,media,size,sha256}组成数组的canonical字节之SHA256；size按原始字节计算。

samples/legacy.sql当前只有R-17修订1，三件内容：
- 主表.pdf = hex 255044462d312e340a2525454f460a，application/pdf；
- 附件/A.txt = hex 6162630a，text/plain；
- 附件/empty.txt = 空字节，text/plain。
old-1是已发布回执；pending-2是待办R-17修订2。完整binding、digest及回执JSON均在SQL内，不允许按这些样本ID特判。

samples/source.json是供应商源包格式：{"packets":[{"packet":"标识","revision":1,"files":[{"path":"原始路径","media":"媒体类型","body":"标准base64"}]}]}。packet/revision唯一且内容不可变。path在包内唯一，按原始Unicode字符串比较，不做大小写折叠或路径规范化。不同包可以使用相同path。SDK1的Client.export(packet,revision)同步返回该形状中的单个包；文件完整base64放在body。

read()返回{"packets":[{"packet":"R-17","revision":1,"files":[{"path":"主表.pdf","media":"application/pdf","body":"JVBERi0xLjQKJSVFT0YK","sha256":"14bcd090baf31edba64e9cbd8cdfc15f943344aa72cb3675ad8e91bfcbce03ad"},{"path":"附件/A.txt","media":"text/plain","body":"YWJjCg==","sha256":"edeaaff3f1774ad2888673770c6d64097e391bc362d7d6fb34982ddf0efd18cb"},{"path":"附件/empty.txt","media":"text/plain","body":"","sha256":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"}]}]}，packets按packet、files按path排序；body使用标准base64。
完成pending-2后R-17为修订2，仅保留主表.pdf和附件/a.txt，原附件/A.txt与空文件消失；再完成R-18修订1后，R-18的copy.bin与R-17的附件/a.txt同为hex 6162630a，但media分别为application/octet-stream与text/plain。旧回执old-1仍原样返回published；之后新提交的R-17修订1返回superseded，不能回退当前包。请求修订等于当前修订但内容digest不同为REVISION_CONFLICT；任何失败不得更改当前包或伪造成功回执。正常回归具体断言已在checks.py中。

## SDK2的实际接口

vendor/export_v2.py的Client(origin="",transport=None)提供四个async方法。HTTP POST路径为/v2/open、/v2/status、/v2/pull、/v2/ack；SDK会把远端{"error":"CODE"}变为RemoteError("CODE")，无自动重试，无本地持久化。网络错误/协程取消可能发生在服务端提交之前或之后，不能根据异常判断远端是否提交。

- open(consumer,packet,revision) -> {"session":"不透明ID","manifest":{"packet":"包标识","revision":修订整数,"files":[{"path":"原始路径","media":"媒体类型","size":字节数,"sha256":"64位小写hex"}]}}。同consumer精确绑定同包同修订，重试返回同session；换绑定为CONSUMER_CONFLICT。consumer必须稳定使用canonical([archive_id,job])，不得因重启、接替或重试改变。不能为绕过已确认段而另起consumer。manifest是不可变完整包，空包合法。
- status(session) -> {"cursor":整数,"pending":operation或null,"done":布尔,"manifest":同open的manifest}。初始cursor=0。所有文件按path排序，逐文件分为至多CHUNK=65536字节的段；空文件也产生一段。cursor对整个包连续计数。done表示全部段已消费且均已确认；空包初始即done。
- pull(session,operation,cursor) -> {"path":"原始路径","offset":文件内起始字节,"body":"base64","eof":布尔,"cursor":本段序号,"next":下一序号}。operation是客户端选择的非空字符串，在session内唯一绑定cursor。首次成功使远端cursor前进并留下唯一pending；相同operation/cursor在未ack时可重取完全相同的响应。另一个operation在pending存在时为UNACKED；错误cursor为CURSOR_CONFLICT；同operation换cursor为OPERATION_CONFLICT；末尾为END。
- ack(session,operation) -> {"acked":operation}。确认不可撤销，远端删除该operation的response载荷，仅保留轻量去重记录；之后pull该operation为ACKED。ack可无限次重试；旧operation迟到的重复ack不会清掉新的pending。未知operation为NO_OPERATION。

Portal.dispatch在SQLite事务内实现以上语义；传入文件连接即可持久保存远端session、cursor、pending、operation去重记录。使用async客户端重放时连接须以check_same_thread=False建立。归档只可通过SDK2访问供应商业务状态与载荷，不得直接读写远端表、读取源JSON载荷代替下载，或修改供应商实现的消费规则。供应商内存源加载、测试驱动和read输出不计入归档载荷工作内存；除此以外，迁移/续传的Python载荷驻留量应与CHUNK成正比，不随单件文件大小增长。接收零字节、截断、偏移或摘要错误不能冒充成功。

## 本轮公开目标入口及可观察结果

保留旧CLI的enqueue、work、read及其JSON结果。新增：
- Archive.migrate() -> {"version":2}，同步、幂等；未迁移的库保持schema1行为，成功后PRAGMA user_version=2。
- await Archive.advance(job,client,worker,now,ttl) -> {"job":JOB,"state":"pending"或"busy"或"done","receipt":null或原始成功回执}。client是SDK2 Client；worker为非空字符串；now为非负整数，ttl为正整数，不接受bool；有效租约截止点<=now时可接替。每次调用至多发起一次pull，可以仅处理本地恢复或ack。busy无归档副作用，done携带首次回执；校验失败抛ValueError("CONTENT_MISMATCH")且持久保留失败原因，当前包不变。未知job为NO_JOB。并发即使使用相同worker字符串也必须安全，不能把worker名字当作不可伪造的代次。
- python3 run.py --db archive.sqlite migrate
- python3 run.py --db archive.sqlite step JOB samples/source.json --remote remote.sqlite --worker W --now 100 --ttl 30

step打开独立持久remote.sqlite承载Portal的状态，禁用v1端点，使用同一个SDK2实现。连续step推进同一job至done；work使用SDK2推进至同样的原始回执，可以使用按本地归档定位的持久远端状态文件。远端文件路径由调用方显式传给step；work使用archive.sqlite.remote.sqlite，不能在重启后把远端状态重新初始化为空。HTTP与重放传输的业务语义一致，不要求另建远端服务。

schema2要求提供objects(digest TEXT PRIMARY KEY, body BLOB NOT NULL)，digest为body的SHA256；完整载荷仅在此存一份，多包、路径、media的引用须独立保存。其他表结构自行决定；中间段不算完整对象，允许暂存。成功迁移后不保留schema1的重复载荷副本，Archive.read/enqueue仍可直接处理同一库。旧进程是迁移前构造Archive并已完成远端fetch、之后才执行其原始写SQL的进程；不允许仅靠新版Python构造函数检查版本阻止它。旧进程读取既有回执可继续返回；任何迟到的旧式修改必须失败并回滚。本轮不要求迁移期间持续接纳新任务，可在迁移事务期间冻结本地写入。

归档可见性以read的一致快照为准：某包旧整包或新整包以及对应回执，不出现半包；其他包独立推进。每个完成文件必须同时验证manifest的size与sha256，packet的最高已发布修订不能被迟到任务回退。回执与发布或superseded判定一起提交，之后任何重试返回首次结果，不能因为head后来变化而改写旧回执。迁移前后原始文件、media、排序、请求绑定、归档身份和旧回执必须一致。

恢复要求涵盖每一个本地持久化边界，及open/pull/ack在提交后丢响应；租约接替后失效调用返回也不能改写当前进度或发布。网络期间不占本地写事务或全局锁，另一包的advance能够完成。已经ack的段不得向供应商补抓，也不得丢弃后通过新会话重建。租约/阶段/操作身份及暂存结构属于实现设计，本契约不指定方案。

保留checks.py的normal和sdk断言，并扩展verify与demo，实际验证和展示迁移及恢复后的业务结果；运行清单格式保持兼容。最终交付可执行代码与简短迁移、恢复验证说明。
