// 假上游：同时提供 /v1/chat/completions、/v1/responses、/v1/messages（流式 + 非流式）。
// 它把「自己收到了什么」回显进正文，用来验证网关的请求方向翻译；并在有 tools 时回一个必填参数的 tool_call。
// 工具参数刻意走三种不同的投递路径（覆盖真实上游的坑）：
//   chat     → arguments 在 delta 里
//   responses→ delta 为空，完整 arguments 只在 response.function_call_arguments.done（真实上游的常见行为）
//   messages → 不发 input_json_delta，完整 input 只在 content_block_stop
import http from 'node:http';
import crypto from 'node:crypto';

const PORT = Number(process.argv[2] || 8099);
const ARGS = JSON.stringify({ command: 'echo hi' });
const j = (o) => JSON.stringify(o);
const textOf = (c) => typeof c === 'string' ? c : Array.isArray(c) ? c.map((p) => p && p.text ? p.text : '').join('') : '';
const nonce = () => crypto.randomBytes(3).toString('hex');
function readBody(req) { return new Promise((r) => { const c = []; req.on('data', (d) => c.push(d)); req.on('end', () => r(Buffer.concat(c).toString('utf8'))); }); }
const hasTools = (b) => Array.isArray(b.tools) && b.tools.length > 0;

function sumChat(b) {
  const ms = b.messages || [];
  const sys = ms.filter((m) => m.role === 'system').map((m) => textOf(m.content)).join('');
  const tool = ms.filter((m) => m.role === 'tool').length;
  const calls = ms.reduce((a, m) => a + ((m.tool_calls && m.tool_calls.length) || 0), 0);
  const badArgs = ms.flatMap((m) => (m.tool_calls || []).map((tc) => tc.function && tc.function.arguments)).filter((a) => !a || a === '').length;
  const roles = ms.map((m) => m.role).join(',');
  return `ECHO chat msgs=${ms.length} sys=${sys.length} tool=${tool} calls=${calls} tools=${(b.tools || []).length} emptyargs=${badArgs} roles=${roles}`;
}
function sumResp(b) {
  const items = Array.isArray(b.input) ? b.input : [];
  const fc = items.filter((i) => i.type === 'function_call').length;
  const fo = items.filter((i) => i.type === 'function_call_output').length;
  const msgs = items.filter((i) => i.type === 'message').length;
  const sys = (typeof b.instructions === 'string' ? b.instructions : '').length;
  const emptyargs = items.filter((i) => i.type === 'function_call' && (!i.arguments || i.arguments === '')).length;
  const types = items.map((i) => i.type || i.role).join(',');
  return `ECHO resp items=${items.length} msgs=${msgs} sys=${sys} fc=${fc} fo=${fo} tools=${(b.tools || []).length} emptyargs=${emptyargs} types=${types}`;
}
function sumMsg(b) {
  const ms = b.messages || [];
  let tu = 0, tr = 0;
  for (const m of ms) for (const p of (Array.isArray(m.content) ? m.content : [])) { if (p.type === 'tool_use') tu++; if (p.type === 'tool_result') tr++; }
  const sys = typeof b.system === 'string' ? b.system : Array.isArray(b.system) ? b.system.map((p) => p.text || '').join('') : '';
  const emptyargs = ms.flatMap((m) => (Array.isArray(m.content) ? m.content : [])).filter((p) => p.type === 'tool_use' && (!p.input || !Object.keys(p.input).length)).length;
  const roles = ms.map((m) => m.role).join(',');
  return `ECHO msg msgs=${ms.length} sys=${sys.length} tu=${tu} tr=${tr} tools=${(b.tools || []).length} emptyargs=${emptyargs} roles=${roles}`;
}

const send = (res, code, obj) => { const b = j(obj); res.writeHead(code, { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(b) }); res.end(b); };
const sse = (res) => res.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache' });

// ---------------- chat ----------------
function chatBody(b, sum) {
  const msg = { role: 'assistant', content: `${sum} nonce=${nonce()}` };
  if (hasTools(b)) msg.tool_calls = [{ id: 'call_' + nonce(), type: 'function', function: { name: 'bash', arguments: ARGS } }];
  return { id: 'chatcmpl-mock', object: 'chat.completion', created: Math.floor(Date.now() / 1000), model: b.model, choices: [{ index: 0, message: msg, finish_reason: hasTools(b) ? 'tool_calls' : 'stop' }], usage: { prompt_tokens: 11, completion_tokens: 22, total_tokens: 33, prompt_tokens_details: { cached_tokens: 5 }, completion_tokens_details: { reasoning_tokens: 3 } } };
}
function chatStream(res, b, sum) {
  sse(res);
  const w = (o) => res.write('data: ' + j(o) + '\n\n');
  const id = 'chatcmpl-mock';
  const base = { id, object: 'chat.completion.chunk', created: Math.floor(Date.now() / 1000), model: b.model };
  const text = `${sum} nonce=${nonce()}`;
  const mid = Math.ceil(text.length / 2);
  w({ ...base, choices: [{ index: 0, delta: { role: 'assistant', content: '' }, finish_reason: null }] });
  w({ ...base, choices: [{ index: 0, delta: { content: text.slice(0, mid) }, finish_reason: null }] });
  w({ ...base, choices: [{ index: 0, delta: { content: text.slice(mid) }, finish_reason: null }] });
  if (hasTools(b)) {
    const cid = 'call_' + nonce();
    w({ ...base, choices: [{ index: 0, delta: { tool_calls: [{ index: 0, id: cid, type: 'function', function: { name: 'bash', arguments: '' } }] }, finish_reason: null }] });
    w({ ...base, choices: [{ index: 0, delta: { tool_calls: [{ index: 0, function: { arguments: ARGS } }] }, finish_reason: null }] });
  }
  w({ ...base, choices: [{ index: 0, delta: {}, finish_reason: hasTools(b) ? 'tool_calls' : 'stop' }] });
  w({ ...base, choices: [], usage: { prompt_tokens: 11, completion_tokens: 22, total_tokens: 33, prompt_tokens_details: { cached_tokens: 5 }, completion_tokens_details: { reasoning_tokens: 3 } } });
  res.write('data: [DONE]\n\n');
  res.end();
}

// ---------------- responses ----------------
function respBody(b, sum) {
  const output = [{ type: 'message', id: 'msg_mock', status: 'completed', role: 'assistant', content: [{ type: 'output_text', text: `${sum} nonce=${nonce()}`, annotations: [] }] }];
  if (hasTools(b)) output.push({ type: 'function_call', id: 'fc_mock', call_id: 'call_' + nonce(), name: 'bash', arguments: ARGS, status: 'completed' });
  return { id: 'resp_mock', object: 'response', created_at: Math.floor(Date.now() / 1000), status: 'completed', model: b.model, output, usage: { input_tokens: 11, output_tokens: 22, total_tokens: 33, input_tokens_details: { cached_tokens: 5 }, output_tokens_details: { reasoning_tokens: 3 } } };
}
function respStream(res, b, sum) {
  sse(res);
  const ev = (t, o) => res.write(`event: ${t}\ndata: ${j({ type: t, ...o })}\n\n`);
  const rid = 'resp_mock', now = Math.floor(Date.now() / 1000);
  const rbase = { id: rid, object: 'response', created_at: now, model: b.model };
  ev('response.created', { response: { ...rbase, status: 'in_progress', output: [] } });
  ev('response.in_progress', { response: { ...rbase, status: 'in_progress', output: [] } });
  const text = `${sum} nonce=${nonce()}`;
  ev('response.output_item.added', { output_index: 0, item: { id: 'msg_mock', type: 'message', status: 'in_progress', role: 'assistant', content: [] } });
  ev('response.content_part.added', { item_id: 'msg_mock', output_index: 0, content_index: 0, part: { type: 'output_text', text: '', annotations: [] } });
  for (const chunk of [text.slice(0, Math.ceil(text.length / 2)), text.slice(Math.ceil(text.length / 2))]) ev('response.output_text.delta', { item_id: 'msg_mock', output_index: 0, content_index: 0, delta: chunk });
  ev('response.output_text.done', { item_id: 'msg_mock', output_index: 0, content_index: 0, text });
  ev('response.content_part.done', { item_id: 'msg_mock', output_index: 0, content_index: 0, part: { type: 'output_text', text, annotations: [] } });
  ev('response.output_item.done', { output_index: 0, item: { id: 'msg_mock', type: 'message', status: 'completed', role: 'assistant', content: [{ type: 'output_text', text, annotations: [] }] } });
  const output = [{ type: 'message', id: 'msg_mock', status: 'completed', role: 'assistant', content: [{ type: 'output_text', text, annotations: [] }] }];
  if (hasTools(b)) {
    const cid = 'call_' + nonce();
    ev('response.output_item.added', { output_index: 1, item: { id: 'fc_mock', type: 'function_call', status: 'in_progress', call_id: cid, name: 'bash', arguments: '' } });
    ev('response.function_call_arguments.delta', { item_id: 'fc_mock', output_index: 1, delta: '' });   // 真实上游：delta 是空的
    ev('response.function_call_arguments.done', { item_id: 'fc_mock', output_index: 1, arguments: ARGS });  // 全量只在 done
    const fc = { type: 'function_call', id: 'fc_mock', call_id: cid, name: 'bash', arguments: ARGS, status: 'completed' };
    ev('response.output_item.done', { output_index: 1, item: fc });
    output.push(fc);
  }
  ev('response.completed', { response: { ...rbase, status: 'completed', output, usage: { input_tokens: 11, output_tokens: 22, total_tokens: 33, input_tokens_details: { cached_tokens: 5 }, output_tokens_details: { reasoning_tokens: 3 } } } });
  res.end();
}

// ---------------- messages ----------------
function msgBody(b, sum) {
  const content = [{ type: 'text', text: `${sum} nonce=${nonce()}` }];
  if (hasTools(b)) content.push({ type: 'tool_use', id: 'toolu_' + nonce(), name: 'bash', input: { command: 'echo hi' } });
  return { id: 'msg_mock', type: 'message', role: 'assistant', model: b.model, content, stop_reason: hasTools(b) ? 'tool_use' : 'end_turn', stop_sequence: null, usage: { input_tokens: 11, output_tokens: 22, cache_read_input_tokens: 5 } };
}
function msgStream(res, b, sum) {
  sse(res);
  const ev = (t, o) => res.write(`event: ${t}\ndata: ${j({ type: t, ...o })}\n\n`);
  const text = `${sum} nonce=${nonce()}`;
  ev('message_start', { message: { id: 'msg_mock', type: 'message', role: 'assistant', model: b.model, content: [], stop_reason: null, usage: { input_tokens: 11, output_tokens: 1, cache_read_input_tokens: 5 } } });
  ev('content_block_start', { index: 0, content_block: { type: 'text', text: '' } });
  ev('content_block_delta', { index: 0, delta: { type: 'text_delta', text } });
  ev('content_block_stop', { index: 0 });
  if (hasTools(b)) {
    const cid = 'toolu_' + nonce();
    ev('content_block_start', { index: 1, content_block: { type: 'tool_use', id: cid, name: 'bash', input: {} } });
    // 真实上游：不发 input_json_delta，完整 input 只在 content_block_stop
    ev('content_block_stop', { index: 1, content_block: { type: 'tool_use', id: cid, name: 'bash', input: { command: 'echo hi' } } });
  }
  ev('message_delta', { delta: { stop_reason: hasTools(b) ? 'tool_use' : 'end_turn', stop_sequence: null }, usage: { output_tokens: 22 } });
  ev('message_stop', {});
  res.end();
}

http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://x');
  let raw = '';
  try { raw = await readBody(req); } catch { }
  let b = {}; try { b = JSON.parse(raw || '{}'); } catch { }
  const stream = !!b.stream;
  if (url.pathname.endsWith('/chat/completions')) { const s = sumChat(b); return stream ? chatStream(res, b, s) : send(res, 200, chatBody(b, s)); }
  if (url.pathname.endsWith('/responses')) { const s = sumResp(b); return stream ? respStream(res, b, s) : send(res, 200, respBody(b, s)); }
  if (url.pathname.endsWith('/messages')) { const s = sumMsg(b); return stream ? msgStream(res, b, s) : send(res, 200, msgBody(b, s)); }
  send(res, 404, { error: { message: 'mock: no such endpoint ' + url.pathname } });
}).listen(PORT, '127.0.0.1', () => console.log('mock3 upstream on', PORT));
