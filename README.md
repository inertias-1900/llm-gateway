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

## 跑起来

```bash
cp config.example.json config.json    # 填 providers / routes / models / keys
cp .env.example .env                  # 填上游 API key（变量名对应 config 里的 api_key_env）
node server.mjs                       # 或 docker compose up -d
```

默认监听 `0.0.0.0:8010`，面板就是 `/`。客户端把 base_url 指到 `http://<host>:8010/v1`，key 填网关签发的虚拟 key。

配置在 `config.json`（providers / routes / models / keys / prices / quota），key 在 `.env`（启动时读进进程环境，所以 `docker restart` 不会丢），统计在 `stats/requests.db`（自动建，jsonl 只滚动留 3 天）。面板上改配置会自动备份 + 热加载。

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
