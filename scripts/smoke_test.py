#!/usr/bin/env python3
"""最小化 LLM 网关的冒烟测试：改完配置/代码后跑一遍。不打印任何 key。

用法: python3 smoke_test.py [base_url]        # 默认 http://127.0.0.1:8010
环境: GW_BASE=... GW_CFG=config.json 路径
"""
import json, os, re, sys, urllib.request, urllib.error

BASE = (sys.argv[1] if len(sys.argv) > 1 else os.environ.get('GW_BASE', 'http://127.0.0.1:8010')).rstrip('/')
CFG_PATH = os.environ.get('GW_CFG', os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'config.json'))

cfg = json.load(open(CFG_PATH, encoding='utf-8'))
KEY = (cfg.get('keys') or [{}])[0].get('key', '')
MODELS = cfg.get('models') or []

# 直通路由挑一个「纯 chat」模型；responses 路由挑一个模型名命中 routes.responses_regex 的
RESP_RE = re.compile(next((r['responses_regex'] for r in (cfg.get('routes') or []) if r.get('responses_regex')), r'^(gpt-|grok-)'))
def _match(m, want):
    return bool(RESP_RE.match(str(m).split('/')[-1])) == want
CHAT_MODEL = next((m for m in MODELS if _match(m, False)), (MODELS or [''])[0])
RESP_MODEL = next((m for m in MODELS if _match(m, True)), CHAT_MODEL)

results = []
def check(name, ok, detail=''):
    results.append((name, ok, detail))
    print(('  PASS  ' if ok else '  FAIL  ') + name + ('   ' + str(detail)[:110] if detail else ''))

def req(path, body=None, key=KEY, stream=False, timeout=180):
    h = {'Content-Type': 'application/json'}
    if key:
        h['Authorization'] = 'Bearer ' + key
    data = json.dumps(body).encode() if body is not None else None
    r = urllib.request.Request(BASE + path, data=data, headers=h)
    try:
        resp = urllib.request.urlopen(r, timeout=timeout)
        return resp.status, (resp if stream else resp.read().decode('utf-8', 'replace'))
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode('utf-8', 'replace')
    except Exception as e:
        return 0, str(e)

def run_stream(model, tools=None, prompt=None):
    body = {'model': model, 'messages': [{'role': 'user', 'content': prompt or '只回复两个字：收到'}], 'max_tokens': 256, 'stream': True}
    if tools:
        body['tools'] = tools
    s, resp = req('/v1/chat/completions', body, stream=True)
    if s != 200:
        return s, {'err': resp}
    content, chunks, done, usage, tool_deltas = [], 0, False, None, 0
    calls = {}
    for raw in resp:
        line = raw.decode('utf-8', 'replace').strip()
        if not line.startswith('data:'):
            continue
        p = line[5:].strip()
        if p == '[DONE]':
            done = True
            break
        try:
            j = json.loads(p)
        except Exception:
            continue
        chunks += 1
        if j.get('usage'):
            usage = j['usage']
        for c in j.get('choices', []):
            d = c.get('delta') or {}
            if d.get('content'):
                content.append(d['content'])
            if d.get('tool_calls'):
                tool_deltas += 1
                for tc in d['tool_calls']:
                    i = tc.get('index', 0)
                    cu = calls.setdefault(i, {'name': '', 'args': ''})
                    fn = tc.get('function') or {}
                    if fn.get('name'):
                        cu['name'] = fn['name']
                    if fn.get('arguments'):
                        cu['args'] += fn['arguments']
    return s, {'chunks': chunks, 'done': done, 'text': ''.join(content), 'usage': usage,
               'tool_deltas': tool_deltas, 'tool_args': [c['args'] for c in calls.values()],
               'tool_names': [c['name'] for c in calls.values()]}

print(f'网关冒烟测试 @ {BASE}')
print(f'  配置: {len(MODELS)} 模型 / {len(cfg.get("keys") or [])} 个 key / 上游 {list((cfg.get("providers") or {}).keys())}')

s, b = req('/healthz', key=None)
check('健康检查 /healthz', s == 200, b[:60])

s, b = req('/v1/models', key='sk-wrong-key')
check('坏 key 被拒(401)', s == 401, s)

s, b = req('/v1/models')
# 不带 protocol 的请求会按「该 key 最近用过的协议」过滤，所以条数可能是子集，不能硬断言全量
n = len(json.loads(b).get('data', [])) if s == 200 else 0
check('模型列表 /v1/models', s == 200 and 0 < n <= len(MODELS), f'{s}, {n} 项（全量 {len(MODELS)}）')

s, d = run_stream(CHAT_MODEL)
check(f'流式·直通路由 ({CHAT_MODEL})', s == 200 and d.get('done') and d.get('text'), f'chunks={d.get("chunks")} text={d.get("text", "")[:20]!r}')

s, d = run_stream(RESP_MODEL)
check(f'流式·responses 翻译 ({RESP_MODEL})', s == 200 and d.get('done') and d.get('text'), f'chunks={d.get("chunks")} text={d.get("text", "")[:20]!r}')
check('流式回吐 usage', bool(d.get('usage')), json.dumps(d.get('usage'), ensure_ascii=False) if d.get('usage') else '缺失')

s, d = run_stream(RESP_MODEL, tools=[{'type': 'function', 'function': {'name': 'get_time', 'description': '取时间', 'parameters': {'type': 'object', 'properties': {}, 'required': []}}}], prompt='必须调用工具 get_time')
check('流式·工具调用', s == 200 and d.get('tool_deltas', 0) > 0, f'tool_deltas={d.get("tool_deltas")}')

# 关键回归：工具参数必须完整传到客户端（曾经只取 delta、忽略 done，客户端收到空 {}）
BASH_TOOL = [{'type': 'function', 'function': {'name': 'bash', 'description': '执行 shell 命令',
              'parameters': {'type': 'object', 'properties': {'command': {'type': 'string'}}, 'required': ['command']}}}]
_s, _d = run_stream(RESP_MODEL, tools=BASH_TOOL, prompt='用 bash 工具执行 ls /tmp，必须调用工具，不要用文字回答')
_ok, _detail = False, f'没有工具调用 args={_d.get("tool_args")}'
if _s == 200 and _d.get('tool_args'):
    try:
        _a = json.loads(_d['tool_args'][0])
        _ok = isinstance(_a.get('command'), str) and _a['command'] != ''
        _detail = f'args={_d["tool_args"][0][:70]!r}'
    except Exception:
        _detail = f'args 不是合法 JSON: {_d["tool_args"][0][:70]!r}'
check('流式·工具调用参数非空（防丢参数）', _ok, _detail)

s, b = req('/api/stats')
check('统计快照 /api/stats', s == 200 and 'totals' in b, s)

s, b = req('/api/stats', key='sk-wrong-key')
check('统计快照鉴权(401)', s == 401, s)

s, b = req('/', key=None)
check('看板页面 /', s == 200 and '<table' in b, f'{len(b)} 字节')

bad = [n for n, ok, _ in results if not ok]
print()
print(f'结果: {len(results) - len(bad)}/{len(results)} 通过' + (f'  失败: {bad}' if bad else '  全部通过'))
sys.exit(1 if bad else 0)
