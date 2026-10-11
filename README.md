# 一个自用的 LLM 网关

> 客户端只会说 OpenAI 那一套，上游各有各的协议。这个网关夹在中间，对外一套接口，对内按模型名分流。

零依赖：只用 Node 内置模块（`http` / `sqlite` / `crypto` / `zlib`），一个 `server.mjs` + 一个 `dashboard.html`。
需要 Node ≥ 22.5（用了内置 `node:sqlite`）。

## 为什么写它

手上几个上游，协议不统一：有的只认 `/chat/completions`，有的要 `/v1/responses`，有的要 Anthropic 的 `/messages`。
而客户端（酒馆、Codex、Claude Code、各种脚本）只会说一种。加上想按模型分流、想给每个客户端单独发 key、想知道谁在烧额度、缓存命中率到底多少——干脆自己写一个。

## 它做什么

| 能力 | 说明 |
|---|---|
| 路由 | 按模型名前缀或正则分流到不同上游，可 `strip` 掉前缀 |
| 三协议 | `chat` 原样透传 / `responses` / `messages` 双向翻译；不确定时自动试、成功后记住 |
| 虚拟 key | 每个客户端一把，可绑协议、可吊销、可单独统计 |
| 统计 | 每条请求落 SQLite：token / 缓存命中 / TTFT / 耗时 / 字节数 / 调用方 IP / 状态 |
| 面板 | 单页 HTML，无构建：用量统计（多时间口径）、模型列表、加供应商、客户端 key；暗/亮主题、手机适配 |
| 抓包开关 | 一键把「客户端原始请求体」和「翻译后发给上游的 payload」并排落盘——判锅用 |
| 首字节保活 | 上游迟迟不吐数据时，先开 SSE 流并每 10s 发注释心跳，绕开反代的 120s 超时 |
| 推理链回灌 | 把上游的 reasoning item 按 `call_id` 归档并插回下一轮，多轮工具调用不再丢思维链 |

## 跑起来

```bash
cp config.example.json config.json    # 填 providers / routes / models / keys
cp .env.example .env                  # 填上游 API key（变量名对应 config 里的 api_key_env）
node server.mjs                       # 或 docker compose up -d
```

默认监听 `0.0.0.0:8010`，面板就是 `/`。客户端把 base_url 指到 `http://<host>:8010/v1`，key 填网关签发的虚拟 key。

## 配置在哪

| 文件 | 作用 |
|---|---|
| `config.json` | providers（上游地址 + key 环境变量 + UA/额外头）、routes（模型名 → 上游 + 协议）、models（对外模型清单）、keys（客户端虚拟 key）、prices（单价表，用于额度估算）、quota（余额/用量接口） |
| `.env` | 上游 API key。网关启动时自己读进进程环境，所以改完 `docker restart` 也不会丢 |
| `stats/requests.db` | 统计库（自动建）。`stats/*.jsonl` 只滚动留 3 天，供 `grep`/取证 |

面板上改配置会自动备份到 `backups/` 并热加载。

## 几个坑，先说了

- **抓包文件是完整对话历史**，排查完记得关（面板顶栏有开关）。
- **上游额度卡片默认是空的**：`config.quota` 不填就不显示。它的返回形状各家不同，`upstreamBalance()` / `upstreamUsage()` 两个函数按你的上游改。
- **反代后面要关掉响应缓冲**（Nginx `proxy_buffering off`、Cloudflare 那条路径别加缓冲），否则流式会被攒成一坨再吐。
- 面板挂公网记得给它单独加一层认证（`config.dashboard_auth.hosts`），`/v1` 那侧本来就是 key 鉴权。

## 目录

```
server.mjs         网关核心（路由、三协议翻译、统计、面板 API）
dashboard.html     面板（单页，无构建，改完刷新即可）
config.example.json
.env.example
docker-compose.yml
scripts/smoke_test.py   改完跑一遍的冒烟脚本
```

## 许可

MIT
