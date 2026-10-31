# 一个自用的最小化 LLM 网关

> 客户端只会说一种api，上游各有各的协议。网关夹在中间：对外一套接口，对内按模型名分流。

零依赖，只用 Node 内置模块（`http` / `sqlite` / `crypto` / `zlib`）；一个 `server.mjs` + 一个 `dashboard.html`。需要 Node ≥ 22.5（用了内置 `node:sqlite`）。

## 它做什么

客户端说一种协议，上游说另一种，网关负责把它们对上：

| 客户端 → ＼ 上游 ↓ | chat | responses | messages |
|---|---|---|---|
| **chat**（`/v1/chat/completions`） | ✅ 原样透传 | ✅ 双向翻译 | ✅ 双向翻译 |
| **responses**（`/v1/responses`） | ⚙️ 可选 | ✅ 原样直通 | ⚙️ 可选 |
| **messages**（`/v1/messages`） | ⚙️ 可选 | ⚙️ 可选 | ✅ 原样直通 |

**chat 是中枢**：客户端只要说 chat，就能用任意协议的上游。responses / messages 原生客户端（Codex、Claude Code 这类）默认只能同协议直通；`⚙️ 可选` 那几格要开「协议互转」——**默认关**，见下。

| 能力 | 说明 |
|---|---|
| 路由 | 按模型名前缀或正则分流到不同上游，可 `strip` 掉前缀 |
| 三协议 | 默认自动试，成功后记忆协议类型 |
| 协议互转（可选） | **默认关**，按 key 勾选：让 responses / messages 客户端跨协议用其它协议的模型。会丢保真度，见下 |
| 推理档位 | 每个对外模型可声明「上游支持的档位」（`model_efforts`）+ 厂商自有映射（`model_effort_overrides`，如 medium→high）；客户端给的档位超范围时就近降级（只降不升），没配置＝原样透传。面板可逐档打最小请求探测，结果缓存到 `effort_probes` |
| key 白名单 | 每个客户端 key 可限定可用模型（`keys[].models`），空＝不限制；按对外模型名或「上游实体」比对，越权直接 403/404 |
| 虚拟 key | 每个客户端一把，可选绑定协议、可吊销、单独统计 |
| 上游池（可选） | 一个对外模型背后挂多个上游：`strategy` 可选 `sticky`（按客户端 key 粘性）/ `quota`（按剩余额度分流）/ `rr`（加权轮询）/ `least`（按并发）/ `failover`（恒首选）。失败自动换成员，连续失败进冷却。成员可用 `models` 声明自己提供哪些模型 |
| 会话身份 | `session_hosts` 里列的上游自动带 `x-upstream-session: __SESSION__`（运行时解析成该 key 的 `session_id`），同时给 key 预置一个替代 UA——缺这两样的上游会回 400 `MissingSessionID` 或 403 `error 1010` |
| 多人共用（可选） | 管理员在面板建账号后，普通用户只能看到自己的上游、自己的 key、自己 key 的用量；组可设 `user_join` 让别人把自己的上游加进来，模型与额度在组内共享 |
| 统计 | 每条请求落 SQLite：token / 缓存命中 / TTFT / 耗时 / 生成速度（token/s）/ 字节 / 调用方 IP / 状态 |
| 统计范围 | 面板「供应商」页可把某个上游设为「不参与统计」：它的流量不计入概览 / 按模型 / 按上游（「最近请求」始终全量）。状态只存浏览器本地，不改配置、不改数据库；默认把 base_url 指向本机（mock/测试上游）的算作不参与 |
| 面板 | 单页 HTML、无构建：用量统计（多时间口径）、模型列表、加供应商、客户端 key；暗亮主题、手机适配 |
| 供应商管理 | 「加供应商」页把上游分成**正式 / 测试**两组（可折叠，测试组默认折叠）：显式 `providers.<名>.test` 优先，没写则按 base_url 是否指向本机推断，点 tag 即可切换。加模型用**供应商下拉**选目标，接口是幂等 upsert——模型已存在就用新配置覆盖（协议留空＝清掉指定与探测缓存、回到自动） |
| 抓包开关 | 一键把「客户端原始请求体」和「翻译后发给上游的 payload」并排落盘——判锅用 |
| 首字节保活 | 上游迟迟不吐数据时，先开 SSE 流并每 10s 发注释心跳，绕开反代的 120s 超时 |
| 订阅周期用量 | 按上游用量接口给的续费时刻切「月付周期」，逐周期列出 token（输入 / 命中缓存 / 输出）、已产生费用、以及费用占该模型月限额的比例。分母基数 `quota.base_usd`（默认 60）、无接口时按 `quota.cycle_day` 切分 |
| 推理链回灌 | 把 reasoning item 按 `call_id` 归档并插回下一轮，多轮工具调用不再丢思维链 |

## 协议互转（可选，默认关）

> 留着这个功能是因为「总觉得应该有」，但它本质是**围绕 chat 做的中枢式转换**，不是无损互通。除非你真的需要「messages 客户端跑 responses 上游的模型」这种组合，否则别开。

**打开**：面板「客户端 key」页 → 每个 key 一列「协议互转」勾选框（默认关）。开的是**那一个 key**，其它 key 不受影响；对应 `config.json` 里 `keys[].protocol_interop`。

| | |
|---|---|
| 做什么 | 上游协议 ≠ 客户端协议时走 `客户端协议 → chat → 上游协议`，响应再翻回来。请求侧 `responsesToChatReq` / `messagesToChatReq`；响应侧按客户端协议渲染 Responses / Anthropic 事件流（`responsesSink` / `messagesSink`） |
| 同协议 | **不受影响**，仍走原生直通（逐字节回吐，保真度最高） |
| 保真度代价 | ① Responses 的加密 reasoning item 在 chat 中枢里装不下 → 丢；② Anthropic 流式的 `message_start.usage.input_tokens` 只能给 0（真值补在 `message_delta`）；③ 图片块被忽略 |
| 模型可见性 | `/v1/models` 默认仍按客户端协议过滤；置 `config.json` 的 `model_filter_protocol: false` 可放开成全量 |
| 排查 | 互转请求打 `CONVERT <客户端协议> -> <上游协议>`；统计表新增 `cproto` 字段（客户端协议） |
| 回归 | `scripts/interop_test/run.sh` —— 起假上游 + 第二个网关实例，跑 3 客户端 × 3 上游 × 流式/非流式 + 多轮工具回传 |

改这段代码时**必须守住**：Responses 事件顺序里 `response.content_part.added` 不能漏——漏了 Codex 会报 `OutputTextDelta without active item` 并**丢弃全部正文**，而 HTTP 仍是 200、流也合法，最难查。

## 上游池（可选）

一个对外模型背后可以挂多个上游（多个订阅、主备、中转），路由里写 `"pool": "<池名>"` 代替 `"provider"`：

```json
"pools": {
  "pool-a": {
    "strategy": "sticky",          // sticky | quota | rr | least | failover
    "cooldown_sec": 60, "max_fails": 2,
    "members": [
      { "provider": "upstream-a", "weight": 3 },
      { "provider": "upstream-b", "weight": 1, "models": ["some-model"] }
    ]
  }
}
```

- **sticky**：按客户端 key 的 `session_id` 做稳定哈希 → 同一个 key 恒定落同一个成员，prompt 缓存与推理链不会被拆散（默认）。
- **quota**：按成员「剩余额度百分比」分流，并对每个 key 记住当前成员，只有落后超过 `quota_margin` 个百分点（默认 2）才换家——避免每轮换人把缓存废掉。
- 失败（408/429/5xx/网络错）自动换下一个成员，`max_fails` 次后进 `cooldown_sec` 冷却；全挂时回结构化错误 `upstream_pool_exhausted`，`error.attempts` 里列出每个成员的状态。
- 池成员供应商的现有路由会被自动接管，所以「同一个上游既要能单独直连、又要参与池」只用配一次。

## 多人共用（可选）

`users` 里建账号后，面板就分管理员 / 普通用户两种身份：

- 普通用户只看得到**自己的**上游、自己的 key、自己 key 的用量（概览 / 按模型 / 最近请求三张视图都按 `keys[].owner` 过滤，筛选下拉里的模型与 IP 也只来自自己的记录）。
- 池上开 `"user_join": true`，普通用户就能把自己的上游填进来一起用；退出时会连带清理自己那份供应商 / 路由 / 模型。
- 管理员在「用户」页建账号、重置密码、看每个人的加入情况。

## 会话身份与替代 UA

有的上游要求请求带一个稳定的会话标识（否则直接 400），有的会按 UA 判断客户端（非浏览器 UA 被 WAF 拦 403）。做法：

- 每个客户端 key 自带一个稳定的 `session_id`；把需要会话头的上游域名写进 `session_hosts`，网关自动给这些上游补 `x-upstream-session: __SESSION__`，值就是该 key 的 `session_id`。
- 建 key 时默认写入一个替代 UA（`keys[].user_agent`），请求时覆盖供应商的 UA；key 上留空则回落到 `providers.<n>.user_agent`。
- 想对某个上游手工指定头，直接在 `providers.<n>.extra_headers` 里写 `{"x-upstream-session": "__SESSION__"}` 或固定字符串。

## 跑起来

```bash
cp config.example.json config.json    # 填 providers / routes / models / keys
cp .env.example .env                  # 填上游 API key（变量名对应 config 里的 api_key_env）
node server.mjs                       # 或 docker compose up -d
```

默认监听 `0.0.0.0:8010`，面板就是 `/`。客户端把 base_url 指到 `http://<host>:8010/v1`，key 填网关签发的虚拟 key。

配置在 `config.json`（providers / pools / routes / models / keys / users / prices / quota / session_hosts），key 在 `.env`（启动时读进进程环境，所以 `docker restart` 不会丢），统计在 `stats/requests.db`（自动建，jsonl 只滚动留 3 天）。面板上改配置会自动备份 + 热加载。

## 注意

- 抓包文件是**完整对话历史**，排查完记得关。
- 上游额度卡片默认是空的（`config.quota` 不填就不显示）；返回形状各家不同，改 `upstreamBalance()` / `upstreamUsage()` 两个函数。周期统计的锚点取 `usage.monthly.resets_at`，取不到退回 `quota.cycle_day`（默认 1 号）。
- 单价表在 `config.prices` / `config.peak_prices`（单位：每 1M token 的美元），`cap` 是该模型的月额度上限；填了才会算「占限额」。
- 反代后面要关掉响应缓冲，否则流式会被攒成一坨再吐。
- 面板挂公网默认单步验证（`config.dashboard_auth.hosts`）。
- `responses`-`messages` 的互转是**可选且默认关**的（面板 key 页勾选），会丢保真度——见上面「协议互转」。

## 目录

```
server.mjs              网关核心（路由、三协议翻译、统计、面板 API）
dashboard.html          面板（单页、无构建，改完刷新即可）
config.example.json     配置模板
.env.example            上游 key 模板
docker-compose.yml
scripts/smoke_test.py   冒烟脚本
scripts/interop_test/   协议互转矩阵回归（假上游 + 第二实例）
```

MIT
