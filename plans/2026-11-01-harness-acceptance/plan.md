# Claude Code Harness 插件验收

## 共用验收套件（2026-11-01）

开发线程 `ea1ca906-a59d-5505-b352-b13d7e0a7ac3`、`64800b89-3e8b-5ca4-9616-bc6611f56477` 指出两个 harness
实现的验收测试已经不同步；`cd031028-273f-55bb-86eb-01efddbceba3` 确定套件不随主仓库发布，
contrib-codex 与 contrib-claudecode 各保留同一份，双方都支持的测试保持一致，一方不支持的测试只在另一方运行；
`4a8b15d7-9c90-5de1-b285-c4c97d0e02b4` 确定在当前分支完成。

### 现状

两仓的 tests/ 来自同一份拷贝。gateway_acceptance.py、gateway_preheat.mjs、gateway_resources.py、
run_application.py、conftest.py 与 .github/actions/prefix-cache 逐字相同，测试 channel 只差包名与
harness builder 种类，其余部分已经分叉：

- Codex 有 mcp-session、mcp-always 两个场景；Claude Code 的 harness 只提供 ONCE 审批范围，没有这两个场景。
- Claude Code 有 lifecycle（每个场景运行）与 steering（bcc 场景运行），Codex 没有。
- 共有的六个场景主体相同；Claude Code 后来移入线程的 psutil 调用及补充的断言信息没有同步到 Codex。
- 两仓 test_process_cleanup 覆盖的后代类型与结束方式不同。
- 最低节点版本分别为 0.3.0rc9 与 0.3.0rc10。

### 共用文件

以下文件两仓逐字相同，修改在同一步骤内两仓同步：

- tests/conftest.py、gateway_acceptance.py、gateway_preheat.mjs、gateway_resources.py、run_application.py
  与 .github/actions/prefix-cache。
- tests/audit_hold.py：包装 NodeAudit 提供 AUDIT，测试卡点在 harness 等待的 publish 内执行。
  bazaar-core 0.3.0rc12 起监听器不再阻塞发布方，卡点只能放在服务调用内。
- tests/fixtures/channel：包名统一为 bcn-test-channel；harness 种类由 agent 的 channel 配置提供，
  channel 按该种类依赖对应的 harness builder 并执行 inspect。
- tests/bcc_application.py、prepare_application.py、test_application.py、lifecycle_application.py 与
  steering_application.py。

实现差异只放在各自的 tests/provider.py：

- CLI 配置：Codex 写 config.toml 与 rules，Claude Code 写 settings.json 与 .mcp.json；按场景调整的沙箱、
  api_key，以及刷新所用的指令文件（Claude Code 另有 CLAUDE.md）。
- 原生值识别：契约中 ToolCall 的 name、input、output 与 ApprovalRequest 的 action、details 保留原生值，
  由 provider.py 识别 bcc 命令、MCP 调用及其结果、MCP 与命令审批及其请求 id、steering 的输入、确认与
  结果帧，以及会话所属的 CLI 进程（Codex 为 CODEX_HOME 对应的 app-server，Claude Code 为带
  --session-id 或 --resume 的 CLI）。
- 支持的场景、只在本实现运行的脚本、后台任务提示语、steering 输入文本与关闭审计的发布方。

两仓 ci.yml 的 e2e job 结构一致，只在 CLI 安装、沙箱依赖与场景矩阵上不同；release.yml 只差包名。

### 测试归属

两仓都运行：

- 场景 mcp-once、bcc、full-access、recovery、refresh 与 background。
- lifecycle：零秒预算停止会话；CLI 自行退出后零秒预算停止会话，仍在发布的关闭审计不被取消；
  停止被取消后没有残留进程；各来源的审计序号从 1 连续到关闭时声明的序号。两个实现都是每个会话
  一个 CLI 进程，Codex 新增。关闭审计的发布方由 provider.py 声明：Claude Code 的 watcher 在 CLI
  退出时发布，Codex 在停止触发的关闭中发布。
- steering 通用部分：活动回合中连续 steer 均被接受，回合只完成一次且没有失败或取消；完成后 steer
  返回 False，随后的新回合正常完成；provider 收到的输入数与发送数一致。Codex 新增，Claude Code
  由现有 steering 拆出。
- test_process_cleanup：child、grandchild 与 detached（新会话或进程组）三类后代，在根进程退出、
  正常停止、零预算停止与取消停止后全部回收，所有平台运行；detached 在根进程退出后的回收只在 Windows
  运行，POSIX 上进入新会话的后代在根进程退出后既不在其进程组也不在其进程树内。
  用例各自驱动内部进程模块，代码不共用。
  现状 Codex 缺 detached；Claude Code 的 child、grandchild 只在 Windows 运行，detached 只覆盖取消停止。
  对齐后暴露的实现缺口在同一步骤修复；Claude Code 的 detached 取消停止曾在 ubuntu 偶发残留
  （CI 41286239525），一并定位。

只在一方运行：

- mcp-session、mcp-always 只由 Codex 选用，场景代码位于共用的 bcc_application.py，按 provider.py
  声明的场景运行。
- result 边界 steering 只在 Claude Code（tests/boundary_application.py）。Claude Code 从 stdin 排队读取输入，
  result 已输出而 harness 尚未读取时到达的 steer 需要并入同一回合；Codex 的 turn/steer 等待 app-server
  响应，边界由 expectedTurnId 判定，同样的卡点会阻塞该响应。

### 步骤

- [x] 共用文件与 provider.py：六个共有场景两仓逐字相同，合并 Claude Code 已有的线程化与断言信息，
  mcp-session、mcp-always 迁入共用驱动；最低节点版本统一为 0.3.0rc12，去掉重复的 psutil 安装参数。
- [x] 补齐共有测试：Codex 增加 lifecycle 与 steering 通用部分；Claude Code 的 steering 拆为通用部分与
  boundary_application.py。
- [x] 进程清理矩阵对齐，修复暴露的实现缺口。Codex 关闭时的后代快照原本位于预算之内，
  零预算或取消关闭会连同快照一起放弃，新会话中的后代因此残留；快照移到预算之前并免于取消后通过。
  Claude Code 此前的偶发残留来自用例在 SIGKILL 生效前检查进程，统一用例等待进程结束后不再出现。

每个步骤两仓各一个提交，提交时勾选对应项，提交前逐个比对共用文件两仓一致。验证以 CI 为准，
本地只运行 Ruff、format、pyright 与非 e2e 测试。
