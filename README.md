# 一个自用的最小化 LLM 网关

> 客户端只会说一种api，上游各有各的协议。网关夹在中间：对外一套接口，对内按模型名分流。

零依赖，只用 Node 内置模块（`http` / `sqlite` / `crypto` / `zlib`）；一个 `server.mjs` + 一个 `dashboard.html`。需要 Node ≥ 22.5（用了内置 `node:sqlite`）。

## 它做什么

| 客户端 → ＼ 上游 ↓ | chat | responses | messages |
|---|---|---|---|
| **chat**（`/v1/chat/completions`） | ✅ 原样透传 | ✅ 双向翻译 | ✅ 双向翻译 |
| **responses**（`/v1/responses`） | ❌ | ✅ 原样直通 | ❌ |
| **messages**（`/v1/messages`） | ❌ | ❌ | ✅ 原样直通 |

| 能力 | 说明 |
|---|---|
| 路由 | 按模型名前缀或正则分流到不同上游，可 `strip` 掉前缀 |
| 三协议 | 默认自动试，成功后记忆协议类型 |
| 虚拟 key | 每个客户端一把，可选绑定协议、可吊销、单独统计 |
| 统计 | 每条请求落 SQLite：token / 缓存命中 / TTFT / 耗时 / 字节 / 调用方 IP / 状态 |
| 面板 | 单页 HTML、无构建：用量统计（多时间口径）、模型列表、加供应商、客户端 key；暗亮主题、手机适配 |
| 抓包开关 | 一键把「客户端原始请求体」和「翻译后发给上游的 payload」并排落盘——判锅用 |
| 首字节保活 | 上游迟迟不吐数据时，先开 SSE 流并每 10s 发注释心跳，绕开反代的 120s 超时 |
| 推理链回灌 | 把 reasoning item 按 `call_id` 归档并插回下一轮，多轮工具调用不再丢思维链 |

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
- 上游额度卡片默认是空的（`config.quota` 不填就不显示）；返回形状各家不同，改 `upstreamBalance()` / `upstreamUsage()` 两个函数。
- 反代后面要关掉响应缓冲，否则流式会被攒成一坨再吐。
- 面板挂公网默认单步验证（`config.dashboard_auth.hosts`）。

## 目录

```
server.mjs              网关核心（路由、三协议翻译、统计、面板 API）
dashboard.html          面板（单页、无构建，改完刷新即可）
config.example.json     配置模板
.env.example            上游 key 模板
docker-compose.yml
scripts/smoke_test.py   冒烟脚本
```

MIT
