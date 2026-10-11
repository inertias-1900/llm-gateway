// 协议互转矩阵回归：3 种客户端协议 × 3 种上游协议 × 流式/非流式（+ 多轮工具回传）。
// 判据：客户端能按自己协议解析出正文与工具参数；正文里的 ECHO 摘要证明「上游收到了什么」——
// 由此同时验证请求方向与响应方向两段翻译。responses 客户端还断言 content_part.added 存在（Codex 硬要求）。
const GW = 'http://127.0.0.1:8098/v1';
const KEY = 'sk-gw-interop-test';
const TAG = { chat: 'chat', responses: 'resp', messages: 'msg' };
const okArgs = (s) => { try { const o = JSON.parse(s); return !!(o && typeof o.command === 'string' && o.command); } catch { return false; } };

const TOOLS_CHAT = [{ type: 'function', function: { name: 'bash', description: 'run a command', parameters: { type: 'object', properties: { command: { type: 'string' } }, required: ['command'] } } }];
const TOOLS_RESP = [{ type: 'function', name: 'bash', description: 'run a command', parameters: { type: 'object', properties: { command: { type: 'string' } }, required: ['command'] } }];
const TOOLS_MSG = [{ name: 'bash', description: 'run a command', input_schema: { type: 'object', properties: { command: { type: 'string' } }, required: ['command'] } }];

function reqFor(cp, up, stream, multi) {
  const model = { chat: 'm-chat/model', responses: 'm-resp/model', messages: 'm-msg/model' }[up];
  if (cp === 'chat') {
    const messages = [{ role: 'system', content: 'SYS' }];
    if (multi) {
      messages.push({ role: 'user', content: 'do it' });
      messages.push({ role: 'assistant', content: null, tool_calls: [{ id: 'call_1', type: 'function', function: { name: 'bash', arguments: JSON.stringify({ command: 'echo hi' }) } }] });
      messages.push({ role: 'tool', tool_call_id: 'call_1', content: 'hi' });
    } else messages.push({ role: 'user', content: 'hi' });
    return { path: '/chat/completions', body: { model, stream, messages, tools: TOOLS_CHAT, max_tokens: 256 } };
  }
  if (cp === 'responses') {
    const input = [];
    if (multi) {
      input.push({ type: 'message', role: 'user', content: [{ type: 'input_text', text: 'do it' }] });
      input.push({ type: 'function_call', call_id: 'call_1', name: 'bash', arguments: JSON.stringify({ command: 'echo hi' }) });
      input.push({ type: 'function_call_output', call_id: 'call_1', output: 'hi' });
    } else input.push({ type: 'message', role: 'user', content: [{ type: 'input_text', text: 'hi' }] });
    return { path: '/responses', body: { model, stream, instructions: 'SYS', input, tools: TOOLS_RESP, max_output_tokens: 256 } };
  }
  const messages = [];
  if (multi) {
    messages.push({ role: 'user', content: 'do it' });
    messages.push({ role: 'assistant', content: [{ type: 'tool_use', id: 'toolu_1', name: 'bash', input: { command: 'echo hi' } }] });
    messages.push({ role: 'user', content: [{ type: 'tool_result', tool_use_id: 'toolu_1', content: 'hi' }] });
  } else messages.push({ role: 'user', content: 'hi' });
  return { path: '/messages', body: { model, stream, system: 'SYS', max_tokens: 256, messages, tools: TOOLS_MSG } };
}

async function readAll(r) { const rd = r.body.getReader(); const dec = new TextDecoder(); let s = ''; for (; ;) { const { done, value } = await rd.read(); if (done) break; s += dec.decode(value, { stream: true }); } return s; }

function parseChat(raw, stream) {
  if (!stream) { const d = JSON.parse(raw); const m = d.choices[0].message; return { text: m.content || '', args: (m.tool_calls || []).map((t) => t.function.arguments), usage: d.usage || {}, saw: '' }; }
  let text = '', args = [''], usage = {}, fr = null;
  for (const line of raw.split('\n')) {
    if (!line.startsWith('data:')) continue;
    const p = line.slice(5).trim(); if (p === '[DONE]') continue;
    let d; try { d = JSON.parse(p); } catch { continue; }
    if (d.usage) usage = d.usage;
    const c = d.choices && d.choices[0]; if (!c) continue;
    if (c.finish_reason) fr = c.finish_reason;
    const dl = c.delta || {};
    if (dl.content) text += dl.content;
    for (const tc of (dl.tool_calls || [])) { if (tc.index != null && args[tc.index] == null) args[tc.index] = ''; if (tc.function && tc.function.arguments) args[tc.index || 0] += tc.function.arguments; }
  }
  return { text, args: args.filter((x) => x != null), usage, saw: fr || '' };
}
function parseResponses(raw, stream) {
  if (!stream) {
    const d = JSON.parse(raw); let text = ''; const args = [];
    for (const it of d.output || []) { if (it.type === 'message') for (const c of it.content || []) if (c.type === 'output_text') text += c.text; if (it.type === 'function_call') args.push(it.arguments); }
    return { text, args, usage: d.usage || {}, saw: '' };
  }
  let text = '', usage = {}, saw = [], args = [];
  for (const block of raw.split('\n\n')) {
    const dl = block.split('\n').find((l) => l.startsWith('data:')); if (!dl) continue;
    let d; try { d = JSON.parse(dl.slice(5).trim()); } catch { continue; }
    saw.push(d.type);
    if (d.type === 'response.output_text.delta') text += d.delta || '';
    if (d.type === 'response.function_call_arguments.delta' && d.delta) { if (!args[0]) args[0] = ''; args[0] += d.delta; }
    if (d.type === 'response.function_call_arguments.done') args[0] = d.arguments || '';
    if (d.type === 'response.completed' && d.response && d.response.usage) usage = d.response.usage;
  }
  return { text, args: args.filter((x) => x != null), usage, saw: saw.join(',') };
}
function parseMessages(raw, stream) {
  if (!stream) {
    const d = JSON.parse(raw); let text = ''; const args = [];
    for (const b of d.content || []) { if (b.type === 'text') text += b.text; if (b.type === 'tool_use') args.push(JSON.stringify(b.input)); }
    return { text, args, usage: d.usage || {}, saw: d.stop_reason || '' };
  }
  let text = '', usage = {}, saw = [], args = [], stop = '';
  for (const block of raw.split('\n\n')) {
    const dl = block.split('\n').find((l) => l.startsWith('data:')); if (!dl) continue;
    let d; try { d = JSON.parse(dl.slice(5).trim()); } catch { continue; }
    saw.push(d.type);
    if (d.type === 'message_start' && d.message && d.message.usage) usage = d.message.usage;
    if (d.type === 'content_block_delta') {
      const x = d.delta || {};
      if (x.type === 'text_delta') text += x.text || '';
      if (x.type === 'input_json_delta') { if (!args[0]) args[0] = ''; args[0] += x.partial_json || ''; }
    }
    if (d.type === 'content_block_stop' && d.content_block && d.content_block.type === 'tool_use' && !args[0]) args[0] = JSON.stringify(d.content_block.input || {});
    if (d.type === 'message_delta') { if (d.usage) { if (d.usage.output_tokens != null) usage.output_tokens = d.usage.output_tokens; if (d.usage.input_tokens != null) usage.input_tokens = d.usage.input_tokens; if (d.usage.cache_read_input_tokens != null) usage.cache_read_input_tokens = d.usage.cache_read_input_tokens; } if (d.delta && d.delta.stop_reason) stop = d.delta.stop_reason; }
  }
  return { text, args: args.filter((x) => x != null), usage, saw: saw.join(',') + '|stop=' + stop };
}
const PARSERS = { chat: parseChat, responses: parseResponses, messages: parseMessages };

const results = [];
let fails = 0;
function check(name, cond, detail) { if (!cond) { fails++; results.push(['FAIL', name, detail]); } else results.push(['ok', name, '']); }

for (const cp of ['chat', 'responses', 'messages']) {
  for (const up of ['chat', 'responses', 'messages']) {
    for (const stream of [false, true]) {
      for (const multi of [false, true]) {
        const name = `${cp}<-${up} ${stream ? 'stream' : 'json '} ${multi ? 'multi' : 'single'}`;
        try {
          const { path, body } = reqFor(cp, up, stream, multi);
          const r = await fetch(GW + path, { method: 'POST', headers: { 'Content-Type': 'application/json', Authorization: 'Bearer ' + KEY }, body: JSON.stringify(body) });
          const raw = await readAll(r);
          if (r.status !== 200) { check(name, false, `HTTP ${r.status}: ${raw.slice(0, 200)}`); continue; }
          const p = PARSERS[cp](raw, stream);
          check(name + ' [text]', p.text.includes(`ECHO ${TAG[up]}`), `text=${JSON.stringify(p.text.slice(0, 90))}`);
          const sysN = Number((p.text.match(/sys=(\d+)/) || [])[1] || 0);
          check(name + ' [sys]', sysN > 0, `sys=${sysN}`);
          check(name + ' [tools]', p.text.includes('tools=1'), p.text.slice(0, 90));
          check(name + ' [args]', p.args.length >= 1 && okArgs(p.args[0]), `args=${JSON.stringify(p.args)}`);
          check(name + ' [emptyargs]', p.text.includes('emptyargs=0'), p.text.slice(0, 120));
          const u = p.usage || {}; const inTok = u.prompt_tokens ?? u.input_tokens ?? u.in ?? 0;
          const cached = u.prompt_tokens_details?.cached_tokens ?? u.input_tokens_details?.cached_tokens ?? u.cache_read_input_tokens ?? 0;
          // messages 上游 + 非 messages 客户端 = 经网关转换，in 被合并成 input_tokens+cache_read=16；
          // messages<-messages 是同协议直通，原样透传上游的 11。
          const wantIn = (up === 'messages' && cp !== 'messages') ? 16 : 11;
          check(name + ' [usage]', inTok === wantIn && cached === 5, `usage=${JSON.stringify(u)}`);
          if (multi) {
            const want = up === 'chat' ? /tool=1/ : up === 'responses' ? /fo=1/ : /tr=1/;
            const want2 = up === 'chat' ? /calls=1/ : up === 'responses' ? /fc=1/ : /tu=1/;
            check(name + ' [roundtrip]', want.test(p.text) && want2.test(p.text), p.text.slice(0, 140));
          }
          if (cp === 'responses' && stream) check(name + ' [content_part.added]', p.saw.includes('response.content_part.added'), p.saw.slice(0, 200));
        } catch (e) { check(name, false, 'EXC ' + e.message); }
      }
    }
  }
}

const bad = results.filter((r) => r[0] === 'FAIL');
console.log(`\n==== 协议互转矩阵：${results.length - bad.length}/${results.length} 通过，${bad.length} 失败 ====`);
for (const b of bad) console.log('FAIL |', b[1], '|', b[2]);
if (!bad.length) console.log('全部通过 ✅');
process.exit(bad.length ? 1 : 0);
