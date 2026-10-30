#!/usr/bin/env node
/**
 * 网关核心：对客户端暴露 OpenAI Chat Completions，按模型路由到不同上游/不同协议。
 * - 上游 formats: "chat" = 原样透传 /chat/completions；"responses" = 翻译成 /v1/responses 再翻回来
 * - 鉴权：虚拟 key（Bearer 或 x-api-key），每个 key 可绑定一个稳定的上游 session 头
 * - 统计：每条请求落 stats/requests-YYYY-MM-DD.jsonl（按天分文件），超过 3 天的自动 gzip
 */
import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import zlib from 'node:zlib';
import { fileURLToPath } from 'node:url';
import { StringDecoder } from 'node:string_decoder';

const ROOT = path.dirname(fileURLToPath(import.meta.url));
const CONFIG_PATH = path.join(ROOT, 'config.json');
const ENV_PATH = path.join(ROOT, '.env');
let CFG = JSON.parse(fs.readFileSync(CONFIG_PATH, 'utf8'));
// 启动时把 .env 读进进程环境：这样 `docker restart` 也不会丢面板加过的 key
function loadEnvFile() {
    let n = 0;
    try {
        for (const line of fs.readFileSync(ENV_PATH, 'utf8').split('\n')) {
            const m = /^([A-Za-z_][A-Za-z0-9_]*)=(.*)$/.exec(line.trim());
            if (m) { process.env[m[1]] = m[2].replace(/^["']|["']$/g, ''); n++; }
        }
    } catch { }
    return n;
}
const ENV_LOADED = loadEnvFile();
const STATS_DIR = path.join(ROOT, 'stats');
const STATS_FILE = path.join(STATS_DIR, 'requests.jsonl');   // 旧版单文件；启动时按天拆分迁移
fs.mkdirSync(STATS_DIR, { recursive: true });

// ---------------- 按天分文件存储 + 旧文件自动 gzip ----------------
// （旧「3 天明文 + 更早 gzip」逻辑已由下方 SQLite + json 滚动保留取代）
const STATS_READ_CAP = 32 * 1024 * 1024;                    // 单文件最多读这么多（超出只读末尾）
const DAY_RE = /^requests-(\d{4}-\d{2}-\d{2})\.jsonl(\.gz)?$/;
function dayKey(ts) {
    const d = new Date(ts), p = (n) => String(n).padStart(2, '0');
    return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())}`;
}
let CUR_FILE = null, CUR_DAY = null;
function fileFor(ts) {                                      // 当前该往哪个文件追加
    const day = dayKey(ts);
    if (CUR_DAY !== day || !CUR_FILE) { CUR_DAY = day; CUR_FILE = path.join(STATS_DIR, `requests-${day}.jsonl`); }
    return CUR_FILE;
}
function listStatFiles() {                                  // 新的在前
    try {
        const out = fs.readdirSync(STATS_DIR).filter((f) => DAY_RE.test(f)).sort().reverse();
        if (fs.existsSync(STATS_FILE)) out.push('requests.jsonl');   // 未迁移成功的旧文件排最后
        return out;
    } catch { return []; }
}
function fileDay(name) { const m = DAY_RE.exec(name); return m ? m[1] : '0000-00-00'; }
function readStatFile(name, tailOnly) {
    const p = path.join(STATS_DIR, name);
    if (name.endsWith('.gz')) {
        const buf = zlib.gunzipSync(fs.readFileSync(p));
        let text = buf.toString('utf8'), truncated = false;
        if (buf.length > STATS_READ_CAP) { text = text.slice(text.length - STATS_READ_CAP); text = text.slice(text.indexOf('\n') + 1); truncated = true; }
        return { text, truncated };
    }
    const st = fs.statSync(p);
    if (tailOnly && st.size > STATS_READ_CAP) {
        const fd = fs.openSync(p, 'r');
        const b = Buffer.alloc(STATS_READ_CAP);
        fs.readSync(fd, b, 0, STATS_READ_CAP, st.size - STATS_READ_CAP);
        fs.closeSync(fd);
        let t = b.toString('utf8');
        return { text: t.slice(t.indexOf('\n') + 1), truncated: true };
    }
    return { text: fs.readFileSync(p, 'utf8'), truncated: false };
}
// 旧文件的处置交给下面的滚动策略：jsonl 只留 JSON_KEEP_DAYS 天（删前先校验该天已全部入库），
// 更早的 .gz 与 stats/_migrated/ 里的历史文件原样保留，不自动删除。

const jdump = (o) => JSON.stringify(o);
const log = (...a) => console.log(new Date().toISOString(), ...a);
// 兜底：handler 里的异常不能让整个网关退出（Node ≥15 未处理的 rejection 会直接退出）
process.on('unhandledRejection', (e) => log('unhandledRejection:', (e && e.stack) || String(e)));
process.on('uncaughtException', (e) => log('uncaughtException:', (e && e.stack) || String(e)));
const STARTED = Date.now();

// ---------------- 请求抓包（默认关闭；GATEWAY_CAPTURE=1 才写盘） ----------------
// 只抓 body（不含任何鉴权头/key），落到 temp/capture-YYYY-MM-DD.jsonl，单条上限 4MB
const ENV_CAPTURE_DEFAULT = /^(1|true|yes|on)$/i.test(String(process.env.GATEWAY_CAPTURE || ''));
const CAPTURE_DIR = path.join(ROOT, 'temp');
const CAPTURE_MAX = 4 * 1024 * 1024;
// 抓包开关：面板可热切换（config.json 的 capture），config 里没这个键时回落到 .env 的 GATEWAY_CAPTURE
const captureOn = () => (CFG.capture === undefined ? ENV_CAPTURE_DEFAULT : !!CFG.capture);
const captureFile = (ts = Date.now()) => path.join(CAPTURE_DIR, 'capture-' + dayKey(ts) + '.jsonl');
function captureInfo() {                     // 给面板显示：抓到多少 / 最后写入时间
    const out = { on: captureOn(), bytes: 0, last: null, files: 0, today: path.basename(captureFile()) };
    try {
        for (const f of fs.readdirSync(CAPTURE_DIR)) {
            if (!/^capture-\d{4}-\d{2}-\d{2}\.jsonl$/.test(f)) continue;
            const st = fs.statSync(path.join(CAPTURE_DIR, f));
            out.bytes += st.size; out.files++; out.last = Math.max(out.last || 0, st.mtimeMs);
        }
    } catch { }
    return out;
}
function capture(phase, data) {
    if (!captureOn()) return;
    try {
        fs.mkdirSync(CAPTURE_DIR, { recursive: true });
        let s = jdump({ t: Date.now(), phase, ...data });
        if (s.length > CAPTURE_MAX) s = s.slice(0, CAPTURE_MAX) + '…[TRUNCATED]';
        fs.appendFileSync(captureFile(), s + '\n');
    } catch (e) { log('capture fail', e.message); }
}
if (captureOn()) log('CAPTURE ON ->', captureFile());

// ---------------- 存储：SQLite（唯一查询源）+ 按天 jsonl（滚动保留，供 tail/取证） ----------------
let sqliteMod;
try { sqliteMod = await import('node:sqlite'); }
catch (e) { console.error('FATAL: 运行环境缺少 node:sqlite（需要 Node ≥ 22.5，推荐 ≥ 24）：', e.message); process.exit(1); }
const { DatabaseSync } = sqliteMod;
const { pipeline } = await import('node:stream/promises');

const DB_PATH = path.join(STATS_DIR, 'requests.db');
const JSON_KEEP_DAYS = 3;        // 按天 jsonl 保留天数（更早的天：先校验已全部入库，再删）
const CAP_KEEP_DAYS = 3;         // 抓包文件超过这么多天 → gzip 归档
const CAP_GZ_KEEP_DAYS = 30;     // 归档 .gz 再保留这么多天，之后删除（每条都有日志）
const HOUR = 3600e3;
const MODEL_UNKNOWN = '(未指定模型)';
const modelKey = (m) => (m === undefined || m === null || m === '') ? MODEL_UNKNOWN : String(m);
const providerKey = (p) => (p === undefined || p === null || p === '') ? '(未路由)' : String(p);
const keyKey = (k) => (k === undefined || k === null || k === '') ? '(未知)' : String(k);
const num = (v) => { const n = Number(v); return Number.isFinite(n) ? Math.round(n) : 0; };

const DB = new DatabaseSync(DB_PATH);
DB.exec('PRAGMA journal_mode=WAL');
DB.exec('PRAGMA synchronous=NORMAL');
DB.exec(`
CREATE TABLE IF NOT EXISTS requests(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts INTEGER NOT NULL, key TEXT, model TEXT, provider TEXT, upstream_model TEXT, format TEXT,
  status INTEGER, ms INTEGER, ttft_ms INTEGER, stream INTEGER, passthrough INTEGER, ip TEXT,
  in_tok INTEGER, out_tok INTEGER, cached INTEGER, reasoning INTEGER, has_tool INTEGER, reason_inj INTEGER,
  req_bytes INTEGER, res_bytes INTEGER, up_res_bytes INTEGER, usage_src TEXT, last_evt TEXT, error TEXT, conv TEXT);
CREATE INDEX IF NOT EXISTS idx_req_ts ON requests(ts);
CREATE INDEX IF NOT EXISTS idx_req_model ON requests(model);
CREATE INDEX IF NOT EXISTS idx_req_key ON requests(key);
CREATE INDEX IF NOT EXISTS idx_req_provider ON requests(provider);
CREATE INDEX IF NOT EXISTS idx_req_status ON requests(status);
CREATE TABLE IF NOT EXISTS rollup_hour(
  hour INTEGER NOT NULL, model TEXT NOT NULL, provider TEXT NOT NULL, key TEXT NOT NULL,
  requests INTEGER NOT NULL DEFAULT 0, errors INTEGER NOT NULL DEFAULT 0,
  in_tok INTEGER DEFAULT 0, out_tok INTEGER DEFAULT 0, cached INTEGER DEFAULT 0, reasoning INTEGER DEFAULT 0,
  ms INTEGER DEFAULT 0, ttft INTEGER DEFAULT 0, bytes_in INTEGER DEFAULT 0, bytes_out INTEGER DEFAULT 0, bytes_res INTEGER DEFAULT 0,
  PRIMARY KEY(hour, model, provider, key)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v TEXT);
`);
try {   // 迁移：加 cproto（客户端协议），便于排查「哪个协议来的请求」
    const cols = DB.prepare('PRAGMA table_info(requests)').all().map((c) => c.name);
    if (!cols.includes('cproto')) { DB.exec('ALTER TABLE requests ADD COLUMN cproto TEXT'); log('migrate: requests + cproto'); }
} catch (e) { log('migrate cproto fail', e.message); }
const INS_REQ = DB.prepare(`INSERT INTO requests(ts,key,model,provider,upstream_model,format,status,ms,ttft_ms,stream,passthrough,ip,
 in_tok,out_tok,cached,reasoning,has_tool,reason_inj,req_bytes,res_bytes,up_res_bytes,usage_src,last_evt,error,conv,cproto)
 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`);
const INS_ROLL = DB.prepare(`INSERT INTO rollup_hour(hour,model,provider,key,requests,errors,in_tok,out_tok,cached,reasoning,ms,ttft,bytes_in,bytes_out,bytes_res)
 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
 ON CONFLICT(hour,model,provider,key) DO UPDATE SET
  requests=requests+excluded.requests, errors=errors+excluded.errors, in_tok=in_tok+excluded.in_tok,
  out_tok=out_tok+excluded.out_tok, cached=cached+excluded.cached, reasoning=reasoning+excluded.reasoning,
  ms=ms+excluded.ms, ttft=ttft+excluded.ttft, bytes_in=bytes_in+excluded.bytes_in,
  bytes_out=bytes_out+excluded.bytes_out, bytes_res=bytes_res+excluded.bytes_res`);
function dbInsert(r) {                     // 落库：明细表 + 小时汇总表（看板聚合只读汇总表，快）
    const ts = Number(r.ts) || Date.now(), u = r.usage || {}, st = num(r.status);
    INS_REQ.run(ts, r.key ?? null, modelKey(r.model), r.provider ?? null, r.upstream_model ?? null, r.format ?? null,
        st, num(r.ms), r.ttft_ms ?? null, r.stream ? 1 : 0, r.passthrough ? 1 : 0, r.ip ?? null,
        num(u.in), num(u.out), num(u.cached), num(u.reasoning), r.has_tool ? 1 : 0, num(r.reason_inj),
        num(r.req_bytes), num(r.res_bytes), num(r.up_res_bytes),
        r.usage_src ?? null, r.last_evt ?? null, r.error ?? null, r.conv ?? null, r.cproto ?? null);
    INS_ROLL.run(Math.floor(ts / HOUR) * HOUR, modelKey(r.model), providerKey(r.provider), keyKey(r.key),
        1, st >= 400 ? 1 : 0, num(u.in), num(u.out), num(u.cached), num(u.reasoning),
        num(r.ms), num(r.ttft_ms), num(r.req_bytes), num(r.up_res_bytes), num(r.res_bytes));
}
let TOTAL = 0, ERRORS = 0;                 // 计数缓存（启动时从库里取，之后随 record 累加）
function countOut(res, holder) {           // 统计真正写给客户端的字节数（累加到 holder.res_bytes，随 ...base 一起落库）
    const w = res.write.bind(res);
    res.write = (c, ...a) => {
        try { holder.res_bytes = (holder.res_bytes || 0) + (typeof c === 'string' ? Buffer.byteLength(c) : (c ? c.length : 0)); } catch { }
        return w(c, ...a);
    };
}
// —— 首字节保活（防 Cloudflare 524）——
// CF 的 Proxy Read Timeout 是 125s：这期间「完全没有字节流动」就直接回 524（免费/Pro/Business 都改不了，只有 Enterprise 能调）。
// 本网关是「先 await 上游、再 writeHead」，上游慢时（实测出现过 270s 首字节）中间完全静默 → 必踩 524。
// 做法：客户端 SSE 流一打开就每 KA_MS 发一个注释心跳（合规 SSE 客户端忽略注释行），直到第一块真实数据写出为止；
//      若 HOLD_MS 内连响应头都没有，则主动提前把 SSE 头开出去（保证 CF 那边先看到"响应已开始"）。
// 影响面：上游数据正常到达时只多几行心跳注释；GATEWAY_HOLD_MS=0 可整关。
const HOLD_MS = Number(process.env.GATEWAY_HOLD_MS ?? 90000);
const KA_MS = Number(process.env.GATEWAY_KA_MS ?? 10000);
function startFirstByteHold(res, streaming) {
    const h = { sent: false, ka: null, timer: null, writing: false, done: false };
    const stopKa = () => { if (h.ka) { clearInterval(h.ka); h.ka = null; } };
    h.stop = () => { h.done = true; if (h.timer) { clearTimeout(h.timer); h.timer = null; } stopKa(); };
    h.open = () => {           // 客户端流已打开 → 起心跳；第一块真实数据写出即自动收工
        if (h.ka || h.done || !streaming || !(KA_MS > 0)) return;
        const w = res.write.bind(res);
        res.write = (c, ...a) => { if (!h.writing) h.stop(); return w(c, ...a); };
        h.ka = setInterval(() => {   // 空闲时发一行 SSE 注释；客户端/CF 看到字节在流动就不会判超时
            if (res.writableEnded) return stopKa();
            h.writing = true; try { res.write(': ka\n\n'); } catch { } h.writing = false;
        }, KA_MS);
    };
    if (!streaming || !(HOLD_MS > 0)) return h;
    h.timer = setTimeout(() => {
        if (res.writableEnded) return;
        if (!res.headersSent) { h.sent = true; res.writeHead(200, { 'Content-Type': 'text/event-stream; charset=utf-8', 'Cache-Control': 'no-cache', 'Connection': 'keep-alive', 'X-Accel-Buffering': 'no' }); }
        h.open();
        log('HOLD', `上游 ${Math.round(HOLD_MS / 1000)}s 未返回数据，已提前开流 + 心跳（防 CF 524）`);
    }, HOLD_MS);
    return h;
}
function record(r, persist = true) {
    r.model = modelKey(r.model);           // 归一后再落库：新记录自带 model 字段（历史无该键的记录在查询层归一）
    if (persist) {
        try { dbInsert(r); } catch (e) { log('db insert fail', e.message); }
        try { fs.appendFileSync(fileFor(r.ts), jdump(r) + '\n'); } catch (e) { log('stats write fail', e.message); }
    }
    TOTAL++; if (num(r.status) >= 400) ERRORS++;
}
function importJsonlToDb() {               // 一次性：把已有 jsonl/.gz/旧单文件全部导入
    const files = listStatFiles().reverse();                       // 旧 → 新
    log(`首次启动：把 ${files.length} 个统计文件导入 SQLite …`);
    let n = 0, bad = 0, batch = 0;
    DB.exec('BEGIN');
    for (const name of files) {
        let text = '';
        try { ({ text } = readStatFile(name, false)); } catch (e) { log('import read fail', name, e.message); continue; }
        for (const line of text.split('\n')) {
            if (!line.trim()) continue;
            let r; try { r = JSON.parse(line); } catch { bad++; continue; }
            try { dbInsert(r); n++; } catch { bad++; }
            if (++batch % 20000 === 0) { DB.exec('COMMIT'); DB.exec('BEGIN'); }
        }
    }
    DB.exec('COMMIT');
    DB.prepare("INSERT OR REPLACE INTO meta(k,v) VALUES('imported_at',?)").run(String(Date.now()));
    log(`导入完成：${n} 条${bad ? `，跳过 ${bad} 条坏行` : ''}`);
}
// ---- 文件滚动：stats jsonl 留 JSON_KEEP_DAYS 天；抓包文件 3 天后 gzip、归档 30 天后删 ----
async function sha256Of(p) { const h = crypto.createHash('sha256'); for await (const c of fs.createReadStream(p)) h.update(c); return h.digest('hex'); }
async function sha256OfGz(p) { const h = crypto.createHash('sha256'); for await (const c of fs.createReadStream(p).pipe(zlib.createGunzip())) h.update(c); return h.digest('hex'); }
async function gzipArchive(src) {          // 先校验解压后字节一致，才删原文件
    const dst = src + '.gz';
    try {
        await pipeline(fs.createReadStream(src), zlib.createGzip({ level: 9 }), fs.createWriteStream(dst));
        if (await sha256Of(src) !== await sha256OfGz(dst)) { fs.unlinkSync(dst); log('gzip 校验失败，保留原文件', path.basename(src)); return false; }
        fs.unlinkSync(src);
        return true;
    } catch (e) { log('gzip 失败', path.basename(src), e.message); try { fs.unlinkSync(dst); } catch { } return false; }
}
function rollStatsJson() {
    const keepFrom = dayKey(Date.now() - (JSON_KEEP_DAYS - 1) * 86400e3);
    let dropped = 0;
    for (const name of listStatFiles()) {
        if (name.endsWith('.gz') || name === 'requests.jsonl') continue;      // .gz / 旧单文件不动
        const day = fileDay(name);
        if (day === '0000-00-00' || day >= keepFrom) continue;
        const p = path.join(STATS_DIR, name);
        const s = Date.parse(day + 'T00:00:00'), e = s + 86400e3;
        let lines = 0;
        try { lines = fs.readFileSync(p, 'utf8').split('\n').filter((l) => l.trim()).length; } catch { continue; }
        const inDb = DB.prepare('SELECT COUNT(*) c FROM requests WHERE ts >= ? AND ts < ?').get(s, e).c;
        if (inDb < lines) { log(`stats json 保留（DB 里 ${inDb} 条 < 文件 ${lines} 条，先不删）: ${name}`); continue; }
        try { fs.unlinkSync(p); dropped++; log(`stats json 滚动删除（该天 ${inDb} 条已全部入库）: ${name}`); } catch (e) { log('stats json 删除失败', name, e.message); }
    }
    return dropped;
}
async function rollCapture() {
    const plainFrom = dayKey(Date.now() - (CAP_KEEP_DAYS - 1) * 86400e3);
    const gzFrom = dayKey(Date.now() - CAP_GZ_KEEP_DAYS * 86400e3);
    let files = [];
    try { files = fs.readdirSync(CAPTURE_DIR); } catch { return; }
    for (const f of files) {
        const m = /^capture-(\d{4}-\d{2}-\d{2})\.jsonl$/.exec(f);
        const mg = /^capture-(\d{4}-\d{2}-\d{2})\.jsonl\.gz$/.exec(f);
        const p = path.join(CAPTURE_DIR, f);
        if (m && m[1] < plainFrom) { if (await gzipArchive(p)) log('抓包文件已 gzip 归档', f); }
        else if (mg && mg[1] < gzFrom) { try { fs.unlinkSync(p); log('抓包归档过期删除', f); } catch { } }
    }
}
(function initStorage() {
    const imported = DB.prepare("SELECT v FROM meta WHERE k='imported_at'").get();
    if (!imported) importJsonlToDb();
    TOTAL = DB.prepare('SELECT COUNT(*) c FROM requests').get().c;
    ERRORS = DB.prepare('SELECT COUNT(*) c FROM requests WHERE status >= 400').get().c;
    log(`SQLite 就绪：${TOTAL} 条记录（错误 ${ERRORS}），库文件 ${(dbSizeBytes() / 1048576).toFixed(1)}MB`);
    rollStatsJson(); rollCapture();
    setInterval(() => { rollStatsJson(); rollCapture(); }, 3600e3);
})();

// ---------------- 请求历史查询（看板「最近请求」页） ----------------
function parseReqQuery(q) {
    const num = (v) => (v === '' || v === null || v === undefined ? null : Number(v));
    return {
        model: String(q.model || '').trim().toLowerCase(),
        key: String(q.key || '').trim(),
        ip: String(q.ip || '').trim(),
        status: (q.status === '' || q.status === null || q.status === undefined) ? '' : String(q.status),
        since: num(q.since), until: num(q.until),
        offset: Math.max(Number(q.offset) || 0, 0),
        limit: Math.min(Math.max(Number(q.limit) || 200, 1), 5000),
    };
}
function rowToRecord(x) {                   // SQL 行 → 前端用的记录形状
    return {
        ts: x.ts, key: x.key, model: x.model, provider: x.provider, upstream_model: x.upstream_model, format: x.format,
        status: x.status, ms: x.ms, ttft_ms: x.ttft_ms, stream: !!x.stream, passthrough: !!x.passthrough, ip: x.ip,
        usage: { in: x.in_tok || 0, out: x.out_tok || 0, cached: x.cached || 0, reasoning: x.reasoning || 0 },
        has_tool: !!x.has_tool, reason_inj: x.reason_inj, req_bytes: x.req_bytes, res_bytes: x.res_bytes,
        up_res_bytes: x.up_res_bytes, usage_src: x.usage_src, last_evt: x.last_evt, error: x.error, conv: x.conv, cproto: x.cproto,
    };
}
function reqOptions() {                    // 筛选下拉的可选项：直接从库里取 DISTINCT
    const opt = (col) => DB.prepare(`SELECT DISTINCT ${col} v FROM requests WHERE ${col} IS NOT NULL AND ${col} <> '' ORDER BY v`).all().map((x) => x.v);
    return {
        models: opt('model'), keys: opt('key'), ips: opt('ip'),
        statuses: DB.prepare('SELECT DISTINCT status s FROM requests WHERE status IS NOT NULL ORDER BY s').all().map((x) => x.s),
    };
}
function queryRequests(q) {
    const f = parseReqQuery(q || {});
    const where = [], args = [];
    if (f.model) { where.push('LOWER(model) LIKE ?'); args.push('%' + f.model + '%'); }
    if (f.key) { where.push('key = ?'); args.push(f.key); }
    if (f.ip) { where.push('ip LIKE ?'); args.push('%' + f.ip + '%'); }
    if (f.status === 'ok') where.push('status < 400');
    else if (f.status === 'err') where.push('status >= 400');
    else if (f.status) { where.push('status = ?'); args.push(Number(f.status)); }
    if (f.since) { where.push('ts >= ?'); args.push(f.since); }
    if (f.until) { where.push('ts <= ?'); args.push(f.until); }
    const W = where.length ? ' WHERE ' + where.join(' AND ') : '';
    const total = DB.prepare('SELECT COUNT(*) c FROM requests' + W).get(...args).c;
    const items = DB.prepare('SELECT * FROM requests' + W + ' ORDER BY ts DESC LIMIT ? OFFSET ?').all(...args, f.limit, f.offset).map(rowToRecord);
    return {
        total, shown: items.length, scanned: total, source: 'db', truncated: false, offset: f.offset, limit: f.limit, items,
        options: reqOptions(),
    };
}

// ---------------- 统计快照（给看板） ----------------
// 生成速度（token/s）：输出 token ÷ 生成耗时。生成耗时 = 总耗时 − 首字延迟（TTFT）；
// 非流式没有 TTFT（记 0），退化为总耗时。无输出或耗时为 0 时记 0（避免 Infinity）。
function tpsOf(out, ms, ttft) {
    const o = Number(out) || 0, gen = (Number(ms) || 0) - (Number(ttft) || 0);
    return (o > 0 && gen > 0) ? +(o / (gen / 1000)).toFixed(2) : 0;
}
function derived(o) {
    const out = {};
    for (const [k, v] of Object.entries(o)) {
        out[k] = {
            ...v,
            hit: v.in ? +(v.cached / v.in * 100).toFixed(1) : 0,
            avg_ms: v.requests ? Math.round(v.ms / v.requests) : 0,
            avg_ttft: v.requests ? Math.round(v.ttft / v.requests) : 0,
            tps: tpsOf(v.out, v.ms, v.ttft),
        };
    }
    return out;
}
// ---------------- 区间统计（概览页「时间口径」） ----------------
// 前端带浏览器时区偏移 tz（分钟）；缺省用服务端本地偏移
const TZ_DEFAULT_MIN = -new Date().getTimezoneOffset();
const SERIES_MAX = 400;
// 分桶：优先用前端给的口径提示（unit='hour'|'day'）；没给就按跨度自动 —— ≤2 天用小时桶，更长用天桶
// 天桶按整数天步长合并，桶数不超过约 120
function pickBucket(spanMs, unitHint) {
    const day = 86400e3;
    const hourBucket = { unit: 'hour', step: 3600e3 };
    const dayBucket = () => ({ unit: 'day', step: Math.max(1, Math.ceil(Math.ceil(spanMs / day) / 120)) * day });
    if (unitHint === 'hour' && spanMs <= 7 * day) return hourBucket;   // 跨度太大时忽略 hour 提示，免得堆出上千根柱子
    if (unitHint === 'day') return dayBucket();
    return spanMs <= 2 * day ? hourBucket : dayBucket();
}
const bucketStart = (ts, step, tzMs) => Math.floor((ts + tzMs) / step) * step - tzMs;
const emptyTotals = () => ({ requests: 0, in: 0, out: 0, cached: 0, reasoning: 0, ms: 0, ttft: 0, bytes_in: 0, bytes_out: 0, bytes_res: 0 });
function blankSeries(step, tzMs, fromTs, toTs, max = SERIES_MAX) {   // 补齐空桶，让柱子连续（并封顶桶数）
    // toTs 是开区间端点（SQL 用 ts < until），减 1ms 免得末端多出一根必然为空的桶
    // —— 否则「昨天」会画出 25 根（末根落在今天 00:00 且恒为 0）
    const out = [], last = bucketStart(Math.max(fromTs, toTs - 1), step, tzMs);
    let first = bucketStart(fromTs, step, tzMs);
    if ((last - first) / step + 1 > max) first = last - (max - 1) * step;
    for (let t = first; t <= last; t += step) out.push({ ts: t, requests: 0, in: 0, out: 0, cached: 0, reasoning: 0 });
    return out;
}
function mergeIntoSeries(list, map, step) {                    // 把聚合结果铺到补零后的桶上
    for (const [k, v] of map) {
        const i = Math.round((k - list[0].ts) / step);
        if (i >= 0 && i < list.length) list[i] = v;
    }
    return list;
}
// 区间聚合：整小时段读 rollup_hour，两端不足一小时的部分读明细表 —— 与逐条扫描结果完全一致
const ROLL_SQL = `SELECT model, provider, key, SUM(requests) n, SUM(errors) e, SUM(in_tok) i, SUM(out_tok) o, SUM(cached) c,
  SUM(reasoning) rz, SUM(ms) m, SUM(ttft) t, SUM(bytes_in) bi, SUM(bytes_out) bo, SUM(bytes_res) br
  FROM rollup_hour WHERE hour >= ? AND hour < ? GROUP BY model, provider, key`;
const RAW_SQL = `SELECT COALESCE(NULLIF(model,''), '${MODEL_UNKNOWN}') model, COALESCE(NULLIF(provider,''), '(未路由)') provider,
  COALESCE(NULLIF(key,''), '(未知)') key, COUNT(*) n, SUM(status >= 400) e, SUM(in_tok) i, SUM(out_tok) o, SUM(cached) c,
  SUM(reasoning) rz, SUM(ms) m, SUM(ttft_ms) t, SUM(req_bytes) bi, SUM(up_res_bytes) bo, SUM(res_bytes) br
  FROM requests WHERE ts >= ? AND ts < ? GROUP BY 1, 2, 3`;
const ROLL_SERIES = `SELECT hour h, SUM(requests) n, SUM(in_tok) i, SUM(out_tok) o, SUM(cached) c, SUM(reasoning) rz
  FROM rollup_hour WHERE hour >= ? AND hour < ? GROUP BY hour`;
const RAW_SERIES = `SELECT (ts/3600000)*3600000 h, COUNT(*) n, SUM(in_tok) i, SUM(out_tok) o, SUM(cached) c, SUM(reasoning) rz
  FROM requests WHERE ts >= ? AND ts < ? GROUP BY h`;
// 按去向分色的序列（?dim=model|provider|key）：每桶再按该维度拆开，供看板堆叠柱状图着色。
// 只做「已选中的那一维」，避免一次吐三个维度的数据把响应撑大。
const DIM_EXPR = {
    model: `COALESCE(NULLIF(model,''), '${MODEL_UNKNOWN}')`,
    provider: `COALESCE(NULLIF(provider,''), '(未路由)')`,
    key: `COALESCE(NULLIF(key,''), '(未知)')`,
};
const ROLL_SERIES_DIM = (d) => `SELECT hour h, ${DIM_EXPR[d]} k, SUM(requests) n, SUM(in_tok) i, SUM(out_tok) o, SUM(cached) c, SUM(reasoning) rz
  FROM rollup_hour WHERE hour >= ? AND hour < ? GROUP BY h, k`;
const RAW_SERIES_DIM = (d) => `SELECT (ts/3600000)*3600000 h, ${DIM_EXPR[d]} k, COUNT(*) n, SUM(in_tok) i, SUM(out_tok) o, SUM(cached) c, SUM(reasoning) rz
  FROM requests WHERE ts >= ? AND ts < ? GROUP BY h, k`;
function sqlAgg(sql, args) {
    return DB.prepare(sql).all(...args).map((x) => ({
        model: x.model, provider: x.provider, key: x.key,
        requests: x.n || 0, errors: x.e || 0, in: x.i || 0, out: x.o || 0, cached: x.c || 0, reasoning: x.rz || 0,
        ms: x.m || 0, ttft: x.t || 0, bytes_in: x.bi || 0, bytes_out: x.bo || 0, bytes_res: x.br || 0,
    }));
}
function addRow(dst, k, v) {                // 把一条聚合行累加进某个维度表
    const x = dst[k] || (dst[k] = { requests: 0, in: 0, out: 0, cached: 0, reasoning: 0, ms: 0, ttft: 0, bytes_in: 0, bytes_out: 0 });
    x.requests += v.requests; x.in += v.in; x.out += v.out; x.cached += v.cached; x.reasoning += v.reasoning;
    x.ms += v.ms; x.ttft += v.ttft; x.bytes_in += v.bytes_in; x.bytes_out += v.bytes_out;
}
function segments(s, e) {                   // 整小时段走汇总表，两端零头走明细表
    const firstHour = Math.ceil(s / HOUR) * HOUR, lastHour = Math.floor(e / HOUR) * HOUR;
    if (firstHour >= e || lastHour <= s) return [{ raw: true, from: s, to: e }];
    const segs = [];
    if (s < firstHour) segs.push({ raw: true, from: s, to: firstHour });
    if (firstHour < lastHour) segs.push({ raw: false, from: firstHour, to: lastHour });
    if (lastHour < e) segs.push({ raw: true, from: lastHour, to: e });
    return segs;
}
function rangeAgg(since, until, tzMin, unitHint, dim) {
    const tzMs = (Number.isFinite(tzMin) ? tzMin : TZ_DEFAULT_MIN) * 60000;
    const sinceTs = Number.isFinite(since) && since > 0 ? since : 0;
    const untilTs = Number.isFinite(until) && until > 0 ? until : Date.now();
    // 不限起点时（「全部」）跨度必须用库里最早一条来算：以前写成 sinceTs || untilTs - HOUR，
    // 于是「全部」的跨度被当成 1 小时 → 误选小时桶
    const firstTs = sinceTs || (DB.prepare('SELECT MIN(ts) m FROM requests').get().m || untilTs - HOUR);
    const b = pickBucket(Math.max(untilTs - firstTs, HOUR), unitHint);
    const totals = emptyTotals(), byModel = {}, byProvider = {}, byKey = {}, series = new Map();
    let errors = 0, matched = 0, segs = 0;
    const segList = segments(sinceTs, untilTs);
    segs = segList.length;
    const dimNames = [], dimIdx = new Map(), dimSeries = new Map();   // ?dim= 分色：桶 -> 维度 -> 数值
    for (const seg of segList) {
        for (const v of sqlAgg(seg.raw ? RAW_SQL : ROLL_SQL, [seg.from, seg.to])) {
            matched += v.requests; errors += v.errors;
            totals.requests += v.requests; totals.in += v.in; totals.out += v.out; totals.cached += v.cached;
            totals.reasoning += v.reasoning; totals.ms += v.ms; totals.ttft += v.ttft;
            totals.bytes_in += v.bytes_in; totals.bytes_out += v.bytes_out; totals.bytes_res += v.bytes_res;
            addRow(byModel, v.model || MODEL_UNKNOWN, v);
            addRow(byProvider, v.provider || '(未路由)', v);
            addRow(byKey, v.key || '(未知)', v);
        }
        for (const x of DB.prepare(seg.raw ? RAW_SERIES : ROLL_SERIES).all(seg.from, seg.to)) {
            const bk = bucketStart(x.h, b.step, tzMs);
            const cur = series.get(bk) || { ts: bk, requests: 0, in: 0, out: 0, cached: 0, reasoning: 0 };
            cur.requests += x.n || 0; cur.in += x.i || 0; cur.out += x.o || 0; cur.cached += x.c || 0; cur.reasoning += x.rz || 0;
            series.set(bk, cur);
        }
    }
    if (dim) {
        for (const seg of segList) {
            for (const x of DB.prepare(seg.raw ? RAW_SERIES_DIM(dim) : ROLL_SERIES_DIM(dim)).all(seg.from, seg.to)) {
                const bk = bucketStart(x.h, b.step, tzMs);
                const name = x.k || '?';
                let ix = dimIdx.get(name);
                if (ix === undefined) { ix = dimNames.length; dimIdx.set(name, ix); dimNames.push(name); }
                let m = dimSeries.get(bk);
                if (!m) dimSeries.set(bk, m = new Map());
                const cur = m.get(ix) || { in: 0, out: 0, requests: 0 };
                cur.in += x.i || 0; cur.out += x.o || 0; cur.requests += x.n || 0;
                m.set(ix, cur);
            }
        }
    }
    const list = matched
        ? mergeIntoSeries(blankSeries(b.step, tzMs, firstTs, Math.min(untilTs, Date.now())), series, b.step)
        : blankSeries(b.step, tzMs, sinceTs || untilTs - 86400e3, untilTs);
    if (dim && dimSeries.size) {
        for (const bucket of list) {
            const m = dimSeries.get(bucket.ts);
            if (m && m.size) bucket.seg = [...m.entries()].map(([ix, v]) => [ix, v.in, v.out, v.requests]);
        }
    }
    const t = totals;
    return {
        since: sinceTs || null, until: untilTs, bucket: b.unit,
        totals: { ...t, hit: t.in ? +(t.cached / t.in * 100).toFixed(1) : 0, avg_ms: t.requests ? Math.round(t.ms / t.requests) : 0, avg_ttft: t.requests ? Math.round(t.ttft / t.requests) : 0, tps: tpsOf(t.out, t.ms, t.ttft) },
        errors, matched, scanned: matched, segs, truncated: false, series: list,
        dim: dim || null, dim_names: dimNames,
        by_model: derived(byModel), by_provider: derived(byProvider), by_key: derived(byKey),
    };
}
// （区间聚合已全部走 SQL：整点读汇总表、零头读明细表，无需再缓存扫盘结果）
function dbSizeBytes() {
    let n = 0;
    for (const suffix of ['', '-wal', '-shm']) { try { n += fs.statSync(DB_PATH + suffix).size; } catch { } }
    return n;
}
function storageInfo() {
    let db = 0, json = 0;
    db = dbSizeBytes();
    try { json = fs.readdirSync(STATS_DIR).filter((f) => /^requests-\d{4}-\d{2}-\d{2}\.jsonl$/.test(f)).length; } catch { }
    return { db_bytes: db, rows: TOTAL, json_days: JSON_KEEP_DAYS, json_files: json, errors: ERRORS };
}
function statsSnapshot(opts = {}) {
    const tzMin = Number.isFinite(opts.tz) ? opts.tz : TZ_DEFAULT_MIN;
    const ranged = (opts.since != null && Number.isFinite(opts.since)) || (opts.until != null && Number.isFinite(opts.until));
    const r = rangeAgg(opts.since, opts.until, tzMin, opts.unit, opts.dim);
    return {
        now: Date.now(), uptime_s: Math.round((Date.now() - STARTED) / 1000),
        total: r.matched, errors: r.errors, totals: r.totals,
        by_model: r.by_model, by_provider: r.by_provider, by_key: r.by_key,
        series: r.series, bucket: r.bucket, tz_min: tzMin,
        dim: r.dim, dim_names: r.dim_names,
        range: ranged ? { since: r.since, until: r.until, matched: r.matched, segs: r.segs } : null,
        storage: storageInfo(),
        config: {
            enabled: isEnabled(),
            capture: captureInfo(),
            models: CFG.models || [],
            keys: (CFG.keys || []).map((k) => k.name),
            providers: Object.keys(CFG.providers || {}),
            routes: (CFG.routes || []).map((r) => `${r.model_regex} -> ${r.provider}/${r.format}`),
            listen: CFG.listen,
        },
    };
}

// ---------------- 鉴权 / 路由 ----------------
const GATEWAY_OFF_MSG = '网关总开关已关闭：暂不转发请求（在网关面板打开即可恢复）';
const isEnabled = () => CFG.enabled !== false;
let KEYS = new Map((CFG.keys || []).map((k) => [k.key, k]));
function reloadConfig() {
    try {
        CFG = JSON.parse(fs.readFileSync(CONFIG_PATH, 'utf8'));
        KEYS = new Map((CFG.keys || []).map((k) => [k.key, k]));
        return true;
    } catch (e) { log('config reload FAILED', e.message); return false; }
}
function saveConfig(cfg) {
    const bakDir = path.join(ROOT, 'backups');
    fs.mkdirSync(bakDir, { recursive: true });
    const bak = path.join(bakDir, `config-${new Date().toISOString().replace(/[:.]/g, '-')}.json`);
    try { fs.copyFileSync(CONFIG_PATH, bak); } catch { }
    const tmp = CONFIG_PATH + '.tmp';
    fs.writeFileSync(tmp, JSON.stringify(cfg, null, 2));
    fs.renameSync(tmp, CONFIG_PATH);
    try { fs.chmodSync(CONFIG_PATH, 0o600); } catch { }
    try {   // 只保留最近 50 份备份
        const files = fs.readdirSync(bakDir).filter((f) => f.startsWith('config-')).sort();
        for (const f of files.slice(0, Math.max(0, files.length - 50))) fs.unlinkSync(path.join(bakDir, f));
    } catch { }
    return bak;
}
function removeEnvVar(name) {
    let lines = [];
    try { lines = fs.readFileSync(ENV_PATH, 'utf8').split('\n').filter((l) => l.trim()); } catch { }
    fs.writeFileSync(ENV_PATH, lines.filter((l) => !l.startsWith(name + '=')).join('\n') + '\n');
    try { fs.chmodSync(ENV_PATH, 0o600); } catch { }
    delete process.env[name];
}
function setEnvVar(name, value) {
    let lines = [];
    try { lines = fs.readFileSync(ENV_PATH, 'utf8').split('\n').filter((l) => l.trim()); } catch { }
    const out = lines.filter((l) => !l.startsWith(name + '='));
    out.push(`${name}=${value}`);
    fs.writeFileSync(ENV_PATH, out.join('\n') + '\n');
    try { fs.chmodSync(ENV_PATH, 0o600); } catch { }
    process.env[name] = value;
}
function authenticate(req) {
    const h = req.headers['authorization'] || '';
    const bearer = h.startsWith('Bearer ') ? h.slice(7).trim() : '';
    // x-goog-api-key：Google SDK / 原生 Gemini 客户端的标准头
    const key = bearer || (req.headers['x-api-key'] || '').trim() || (req.headers['x-goog-api-key'] || '').trim();
    const hit = KEYS.get(key);
    // allow：该 key 的模型白名单（config.keys[].models）。null/空 = 不限制，全部模型可用（默认行为）。
    return hit ? { name: hit.name, session_id: hit.session_id, key, protocol: hit.protocol || '', interop: !!hit.protocol_interop, allow: Array.isArray(hit.models) && hit.models.length ? hit.models : null } : null;
}
// 客户端 key 的模型白名单判定。白名单为空 = 全部放开（默认）。
// 匹配口径：优先按对外模型名精确比对；其次按「上游实体」（provider + upstream_model）比对，
// 这样客户端用带前缀名（ocg/gpt-x）或不带前缀的裸名（gpt-x，走兜底路由）都会被认成同一个模型。
// 对外用 404/403 而不是悄悄成功——白名单只决定「键能用什么」，不改变路由语义。
const ALLOW_ENTITY = new Map();          // 白名单数组指纹 -> Set(上游实体)
function allowedEntities(allow) {
    const sig = allow.join('\u0000');
    let set = ALLOW_ENTITY.get(sig);
    if (set) return set;
    set = new Set();
    for (const m of allow) {
        const rt = routeFor(m);
        if (rt) set.add(rt.provider + '|' + rt.upstream_model);
    }
    if (ALLOW_ENTITY.size > 200) ALLOW_ENTITY.clear();      // 白名单常改，别无限涨
    ALLOW_ENTITY.set(sig, set);
    return set;
}
function modelAllowed(auth, model) {
    if (!auth || !auth.allow) return true;
    const m = String(model || '');
    if (auth.allow.includes(m)) return true;
    const rt = routeFor(m);
    return !!rt && allowedEntities(auth.allow).has(rt.provider + '|' + rt.upstream_model);
}
function routeFor(model) {
    for (const r of CFG.routes || []) {
        if (new RegExp(r.model_regex).test(model)) {
            const p = CFG.providers[r.provider];
            if (!p) continue;
            const bare = r.strip && model.startsWith(r.strip) ? model.slice(r.strip.length) : model;
            return {
                provider: r.provider, providerCfg: p, format: r.format,
                strip: r.strip || '', responses_regex: r.responses_regex || null,
                upstream_model: (r.model_map || {})[model] || bare,
            };
        }
    }
    return null;
}
function upstreamHeaders(p, sessionId, accept, format) {
    const h = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + (process.env[p.api_key_env] || ''),
        'Accept': accept,
    };
    if (p.user_agent) h['User-Agent'] = p.user_agent;
    for (const [k, v] of Object.entries(p.extra_headers || {})) h[k] = v === '__SESSION__' ? sessionId : v;
    if (format === 'messages') {
        // Anthropic 风格端点：Bearer 会 401 Missing API key，必须 x-api-key
        h['x-api-key'] = process.env[p.api_key_env] || '';
        h['anthropic-version'] = p.anthropic_version || '2023-06-01';
        delete h['Authorization'];
    }
    if (format === 'gemini') {
        // 原生 Google 端点：x-goog-api-key 是标准头（实测 Authorization: Bearer 也能过，但两者并存最稳）
        h['x-goog-api-key'] = process.env[p.api_key_env] || '';
    }
    return h;
}

// ---------------- 协议格式：显式 > 覆盖文件 > 正则推断 ----------------
const OVERRIDES_FILE = path.join(STATS_DIR, 'format_overrides.json');
let FORMAT_OVERRIDES = {};
try { FORMAT_OVERRIDES = JSON.parse(fs.readFileSync(OVERRIDES_FILE, 'utf8')); } catch { }
function saveOverrides() { try { fs.writeFileSync(OVERRIDES_FILE, JSON.stringify(FORMAT_OVERRIDES, null, 1)); } catch (e) { log('override save fail', e.message); } }
const otherFormat = (f) => (f === 'responses' ? 'chat' : 'responses');
function resolveFormat(rt) {
    if (rt.format && rt.format !== 'auto') return rt.format;
    const key = rt.provider + '|' + rt.upstream_model;
    if (FORMAT_OVERRIDES[key]) return FORMAT_OVERRIDES[key];
    const seeded = (CFG.model_formats || {})[key];
    if (seeded) return seeded;
    if (rt.responses_regex && new RegExp(rt.responses_regex).test(rt.upstream_model)) return 'responses';
    return 'chat';
}
// 每个对外模型 id 的协议 + 该协议是否来自“真探测”（FORMAT_OVERRIDES 是首请求试出来的；其它来源＝按规则/配置推断）
function modelProtocol(id) {
    const rt = routeFor(id);
    if (!rt) return { protocol: null, detected: false };
    const learned = !!(FORMAT_OVERRIDES[rt.provider + '|' + rt.upstream_model]);
    return { protocol: resolveFormat(rt), detected: learned };
}
// 对外模型可用的推理档位（手工指定 > 探测缓存；都没配＝null＝网关不干预、原样透传）
function effortsForModel(id) {
    const rt = routeFor(String(id || ''));
    return rt ? effortsFor(rt.provider, rt.upstream_model) : null;
}

// 上游说"协议不对"时允许换协议重试（只对 auto 路由生效）
const PROTOCOL_HINT = /ModelProtocolUnsupported|does not support this protocol|unsupported protocol|endpoint is unavailable|not supported for this endpoint/i;
const protocolMismatch = (status, txt) => PROTOCOL_HINT.test(txt) && [400, 404, 405, 422, 500, 501, 503].includes(status);
const ALL_FORMATS = ['chat', 'responses', 'messages', 'gemini'];
function protocolCandidates(rt) {
    const pref = resolveFormat(rt);
    if (rt.format && rt.format !== 'auto') return [pref];
    // gemini 排最后：只有前三家全都报「不支持该协议」时才会试到它，普通模型不会多打一格
    return [pref, ...ALL_FORMATS.filter((f) => f !== pref)];
}
// ---------------- 推理档位（reasoning effort）能力层 ----------------
// 阶梯与 Hermes 本体一致（hermes_constants.VALID_REASONING_EFFORTS）；ultra 是 Hermes 内部档位，语义＝max。
const EFFORT_LADDER = ['minimal', 'low', 'medium', 'high', 'xhigh', 'max', 'ultra'];
const EFFORT_BUDGET = { minimal: 1024, low: 2048, medium: 8192, high: 16384, xhigh: 32768, max: 65536, ultra: 65536 };
const effortKeyOf = (provider, model) => String(provider || '') + '|' + String(model || '');
const normEfforts = (arr) => {                     // 归一化 + 按阶梯排序（clampEffort 依赖这个顺序）
    const src = (Array.isArray(arr) ? arr : []).map((x) => String(x).trim().toLowerCase());
    return EFFORT_LADDER.filter((lvl) => src.includes(lvl));
};
// 面板输入 → 档位数组：支持数组或 "high,max" 这种串；空串/空数组＝清除；出现非法档位名直接报错
function parseEffortsInput(v) {
    const raw = Array.isArray(v) ? v.join(',') : String(v == null ? '' : v).trim();
    if (!raw) return [];
    const parts = raw.split(/[,\s/|]+/).filter(Boolean);
    const norm = normEfforts(parts);
    const bad = parts.map((x) => x.toLowerCase()).filter((x) => !EFFORT_LADDER.includes(x));
    if (bad.length) throw new Error(`不认识的推理档位: ${bad.join(', ')}（只能是 ${EFFORT_LADDER.join(' / ')}，留空＝清除）`);
    return norm;
}

// 生效档位：手工指定（config.model_efforts）> 探测缓存（config.effort_probes）> null（＝不干预，原样透传）
function effortsFor(provider, upstreamModel) {
    const manual = CFG.model_efforts || {};
    const direct = normEfforts(manual[effortKeyOf(provider, upstreamModel)]);
    if (direct.length) return direct;
    const providerWide = normEfforts(manual[effortKeyOf(provider, '*')]);
    if (providerWide.length) return providerWide;
    const probe = (CFG.effort_probes || {})[effortKeyOf(provider, upstreamModel)];
    const probed = normEfforts(probe && probe.levels);
    if (probed.length) return probed;
    return null;
}

// 就近降级（与 Hermes agent/reasoning_effort.py::clamp_effort 同语义）：支持的照发；不支持的降到
// 「最接近的弱档」，没有更弱的档就落到表里最弱的一档——只降不升，绝不因为降级而变贵。
function clampEffort(effort, supported) {
    const want = String(effort || '').trim().toLowerCase();
    if (!want || !Array.isArray(supported) || !supported.length) return effort;
    if (supported.includes(want)) return effort;
    const wi = EFFORT_LADDER.indexOf(want);
    if (wi < 0) return effort;                     // 自定义档位名：不认，原样透传
    const below = supported.filter((e) => EFFORT_LADDER.indexOf(e) < wi);
    return below.length ? below[below.length - 1] : supported[0];
}

// 把 body 里客户端显式给的档位夹到该 provider|model 支持的档位（没配、没探测过＝完全不干预）
// 先查 model_effort_overrides 的厂商自有映射（例：medium→high、xhigh→max），再就近降级
function clampBodyEffort(body, rt) {
    const sup = effortsFor(rt.provider, rt.upstream_model);
    if (!sup) return body;
    const ov = (CFG.model_effort_overrides || {})[effortKeyOf(rt.provider, rt.upstream_model)] || null;
    const pick = (cur) => {
        const mapped = ov ? ov[String(cur).toLowerCase()] : null;
        const c = (mapped && sup.includes(mapped)) ? mapped : clampEffort(cur, sup);
        return String(c).toLowerCase() === String(cur).toLowerCase() ? null : c;
    };
    const out = { ...body };
    let from = null, to = null, c, mapped = false;
    const pickMapped = (cur) => {
        const r = pick(cur);
        if (r && ov && ov[String(cur).toLowerCase()] === r) mapped = true;
        return r;
    };
    if (out.reasoning_effort != null && (c = pickMapped(out.reasoning_effort))) { from = from || out.reasoning_effort; to = to || c; out.reasoning_effort = c; }
    if (out.reasoning && typeof out.reasoning === 'object' && out.reasoning.effort != null && (c = pickMapped(out.reasoning.effort))) {
        from = from || out.reasoning.effort; to = to || c; out.reasoning = { ...out.reasoning, effort: c };
    }
    if (out.output_config && typeof out.output_config === 'object' && out.output_config.effort != null && (c = pickMapped(out.output_config.effort))) {
        from = from || out.output_config.effort; to = to || c; out.output_config = { ...out.output_config, effort: c };
    }
    if (from) log(mapped ? 'EFFORT 映射' : 'EFFORT 降级', rt.provider + '|' + rt.upstream_model, `${from} -> ${to}`, `(支持 ${sup.join('/')})`);
    return out;
}

// ---------------- 档位探测：对某个对外模型逐档打最小请求，看上游接受哪些档 ----------------
const EFFORT_PROBE_PROMPT = '算一下 17×23 等于多少，只回数字';
function probePayloadFor(fmt, model, lvl, thinkingStyle) {
    if (fmt === 'chat') return { path: '/chat/completions', body: { model, messages: [{ role: 'user', content: EFFORT_PROBE_PROMPT }], max_tokens: 64, reasoning_effort: lvl } };
    if (fmt === 'responses') return { path: '/responses', body: { model, input: EFFORT_PROBE_PROMPT, max_output_tokens: 64, reasoning: { effort: lvl } } };
    if (fmt === 'gemini') {                     // 原生 Google：模型在路径里，档位走 generationConfig.thinkingConfig
        const gl = geminiThinkingLevel(lvl);
        return {
            path: `/models/${encodeURIComponent(model)}:generateContent`,
            body: {
                contents: [{ role: 'user', parts: [{ text: EFFORT_PROBE_PROMPT }] }],
                generationConfig: { maxOutputTokens: 200, ...(gl ? { thinkingConfig: { includeThoughts: true, thinkingLevel: gl } } : {}) },
            },
        };
    }
    if (thinkingStyle === 'adaptive') return { path: '/messages', body: { model, max_tokens: 128, messages: [{ role: 'user', content: EFFORT_PROBE_PROMPT }], thinking: { type: 'adaptive' }, output_config: { effort: lvl } } };
    return { path: '/messages', body: { model, max_tokens: EFFORT_BUDGET[lvl] + 4096, messages: [{ role: 'user', content: EFFORT_PROBE_PROMPT }], thinking: { type: 'enabled', budget_tokens: EFFORT_BUDGET[lvl] } } };
}
function sawReasoningIn(fmt, txt) {                 // 该档位是否真的产出了推理内容（null＝判不了）
    try {
        if (fmt === 'chat') { const j = JSON.parse(txt); return String((j.choices || [])[0]?.message?.reasoning_content || '').length > 0; }
        if (fmt === 'responses') { const j = JSON.parse(txt); return (j.output || []).some((it) => it.type === 'reasoning' && ((it.content || []).length || (it.summary || []).length)); }
        if (fmt === 'gemini') { const j = JSON.parse(txt); return ((j.candidates || [])[0]?.content?.parts || []).some((p) => p.thought); }
        const j = JSON.parse(txt); return (j.content || []).some((b) => b.type === 'thinking');
    } catch { return null; }
}
async function probeModelEfforts(id, opts = {}) {
    const rt = routeFor(String(id || ''));
    if (!rt) throw new Error('模型未路由: ' + id);
    const p = rt.providerCfg || {};
    const fmt = resolveFormat(rt);
    const style = p.thinking_style || 'none';
    const levels = normEfforts(opts.levels && opts.levels.length ? opts.levels : EFFORT_LADDER);
    const base = String(p.base_url || '').replace(/\/+$/, '');
    const out = {
        id: String(id), provider: rt.provider, upstream_model: rt.upstream_model,
        format: fmt, thinking_style: style, levels: [], rejected: {}, saw_reasoning: {},
        at: new Date().toISOString(), note: '',
    };
    if (fmt === 'messages' && style === 'none') {
        out.note = '该供应商 thinking_style=none（messages 端点不接收档位参数），无法探测';
        return out;
    }
    const sessionId = ((CFG.keys || [])[0] || {}).session_id;
    for (const lvl of levels) {
        const { path: up, body } = probePayloadFor(fmt, rt.upstream_model, lvl, style);
        const headers = upstreamHeaders(p, sessionId, 'application/json', fmt);
        let status = 0, txt = '';
        try {
            const r = await fetch(base + up, { method: 'POST', headers, body: JSON.stringify(body), signal: AbortSignal.timeout(opts.timeout || 30000) });
            status = r.status; txt = await r.text();
        } catch (e) { status = 0; txt = 'fetch failed: ' + e.message; }
        if (status === 200) {
            out.levels.push(lvl);
            const saw = sawReasoningIn(fmt, txt);
            if (saw !== null) out.saw_reasoning[lvl] = saw;
        } else {
            out.rejected[lvl] = status + ' ' + txt.slice(0, 200).replace(/\s+/g, ' ');
        }
    }
    if (out.levels.length === levels.length && !Object.values(out.saw_reasoning).some(Boolean)) {
        out.note = '所有档位都被接受，但没观察到推理输出 —— 可能不是推理模型，档位表对它无实际作用';
    } else if (!out.levels.length) {
        out.note = '所有档位都被上游拒绝，先确认协议（format）是否选对';
    }
    if (opts.persist !== false) {
        const cfg = JSON.parse(fs.readFileSync(CONFIG_PATH, 'utf8'));
        cfg.effort_probes = cfg.effort_probes || {};
        cfg.effort_probes[effortKeyOf(rt.provider, rt.upstream_model)] = {
            levels: out.levels, rejected: out.rejected, saw_reasoning: out.saw_reasoning,
            format: fmt, thinking_style: style, at: out.at, note: out.note,
        };
        saveConfig(cfg); reloadConfig();
    }
    log('PROBE efforts', effortKeyOf(rt.provider, rt.upstream_model), `${out.levels.length}/${levels.length} 档接受`, out.note ? '（' + out.note + '）' : '');
    return out;
}
function buildUpstream(rt, format, body, sessionId, ctx) {
    body = clampBodyEffort(body, rt);              // 客户端给了不支持的档位 → 就近降级（没配置过＝不动）
    const headers = upstreamHeaders(rt.providerCfg, sessionId, body.stream ? 'text/event-stream' : 'application/json', format);
    if (format === 'responses') {
        let reasonInj = 0;
        const payload = chatToResponses(body, { echoReason: !!rt.providerCfg.reasoning_echo, provider: rt.provider, onInject: (n) => { reasonInj += n; } });
        if (ctx) ctx.reasonInj = reasonInj;
        return { url: rt.providerCfg.base_url + '/responses', payload: { ...payload, model: rt.upstream_model }, headers };
    }
    if (format === 'messages') {
        return { url: rt.providerCfg.base_url + '/messages', payload: chatToMessages(body, rt.upstream_model, rt.providerCfg), headers };
    }
    if (format === 'gemini') {
        // 原生 Google 端点：模型名在路径里，不在 body 里
        const path = `/models/${encodeURIComponent(rt.upstream_model)}` + (body.stream ? ':streamGenerateContent?alt=sse' : ':generateContent');
        return { url: rt.providerCfg.base_url + path, payload: chatToGemini(body, rt.upstream_model, rt.providerCfg), headers };
    }
    const payload = { ...body, model: rt.upstream_model };
    // 实测部分上游的 /chat/completions 不认 OpenAI 新式的 role:"developer"，会回 422 "Endpoint is unavailable."
    // （还骗过 PROTOCOL_HINT 让网关误换协议）。developer 与 system 等价，统一降级成 system 最稳；
    // responses / messages 两条路各自已把 developer 处理掉了，这里只管 chat。
    if (Array.isArray(payload.messages)) payload.messages = payload.messages.map((m) => (m && m.role === 'developer') ? { ...m, role: 'system' } : m);
    if (body.stream) payload.stream_options = { include_usage: true, ...(body.stream_options || {}) };
    return { url: rt.providerCfg.base_url + '/chat/completions', payload, headers };
}

// ---------------- usage 换算 ----------------
const usageFromResponses = (u) => u ? {
    in: u.input_tokens || 0, out: u.output_tokens || 0, total: u.total_tokens || 0,
    cached: u.input_tokens_details?.cached_tokens || 0,
    reasoning: u.output_tokens_details?.reasoning_tokens || 0,
} : {};
const usageFromChat = (u) => u ? {
    in: u.prompt_tokens || 0, out: u.completion_tokens || 0, total: u.total_tokens || 0,
    cached: u.prompt_tokens_details?.cached_tokens || 0,
    reasoning: u.completion_tokens_details?.reasoning_tokens || 0,
} : {};
const usageToChat = (us) => ({
    prompt_tokens: us.in || 0, completion_tokens: us.out || 0,
    total_tokens: us.total || ((us.in || 0) + (us.out || 0)),
    prompt_tokens_details: { cached_tokens: us.cached || 0 },
    completion_tokens_details: { reasoning_tokens: us.reasoning || 0 },
});

// ---------------- 推理链回灌（responses 路径）----------------
// chat 客户端装不下 Responses 的 reasoning item（带 encrypted_content），每轮都会丢思维链 →
// 模型从零重推、表现为复读/打转。做法：把上游返回的 reasoning item 按它产出的 function_call 的
// call_id 归档（call_id 由客户端原样回传，天然是跨轮的锚点），下一轮翻译时再插回该 call 之前。
// 只在供应商开了 providers.<name>.reasoning_echo 时启用（否则不改任何行为）。
const REASON_CACHE = new Map();          // call_id -> { items, t }
const REASON_TTL_MS = 6 * 60 * 60 * 1000;
const REASON_MAX = 3000;
// 归档必须**扛重启**：一旦丢了，后续轮次插不回 reasoning item，请求前缀就和上游已缓存的不一致，
// 该会话从分歧点开始整段 cache miss（表现为命中率“忽高忽 0”）。所以追加落盘 + 启动时恢复。
const REASON_STATE_DIR = path.join(ROOT, 'state');
const REASON_FILE = path.join(REASON_STATE_DIR, 'reason-archive.jsonl');
let reasonAppends = 0;
try { fs.mkdirSync(REASON_STATE_DIR, { recursive: true }); } catch { /* ignore */ }
(function loadReasonArchive() {
    try {
        const now = Date.now();
        for (const line of fs.readFileSync(REASON_FILE, 'utf8').split('\n')) {
            if (!line) continue;
            let e; try { e = JSON.parse(line); } catch { continue; }
            if (!e || !e.k || !Array.isArray(e.items) || !e.items.length) continue;
            if (e.t && now - e.t > REASON_TTL_MS) continue;
            REASON_CACHE.delete(e.k);                  // 同 key 后写的覆盖先写的
            REASON_CACHE.set(e.k, { items: e.items, t: e.t || now });
        }
        while (REASON_CACHE.size > REASON_MAX) REASON_CACHE.delete(REASON_CACHE.keys().next().value);
        console.log(`[reason] 归档恢复 ${REASON_CACHE.size} 条`);
    } catch (e) { if (e.code !== 'ENOENT') console.warn('[reason] 归档读取失败:', e.message); }
})();
function compactReasonArchive() {          // 原子重写：去掉过期/被挤掉的条目，控制文件体积
    try {
        const tmp = REASON_FILE + '.tmp';
        const buf = [];
        for (const [k, v] of REASON_CACHE) buf.push(JSON.stringify({ k, t: v.t, items: v.items }));
        fs.writeFileSync(tmp, buf.join('\n') + (buf.length ? '\n' : ''));
        fs.renameSync(tmp, REASON_FILE);
        reasonAppends = 0;
    } catch (e) { console.warn('[reason] 归档压实失败:', e.message); }
}
function reasonStore(callId, items) {
    if (!callId || !Array.isArray(items) || !items.length) return;
    const t = Date.now();
    REASON_CACHE.set(callId, { items, t });
    try { fs.appendFileSync(REASON_FILE, JSON.stringify({ k: callId, t, items }) + '\n'); }
    catch { /* 落盘失败不影响本次请求 */ }
    if (REASON_CACHE.size > REASON_MAX) {
        const drop = REASON_CACHE.size - REASON_MAX;
        let i = 0;
        for (const k of REASON_CACHE.keys()) { REASON_CACHE.delete(k); if (++i >= drop) break; }
        compactReasonArchive();
    } else if (++reasonAppends >= 500) compactReasonArchive();
}
function reasonTake(callId, provider) {
    const k = reasonKey(provider, callId);
    const hit = REASON_CACHE.get(k);
    if (!hit) return [];
    if (Date.now() - hit.t > REASON_TTL_MS) { REASON_CACHE.delete(k); return []; }
    return hit.items;
}
function reasonCapture(ctx, item) {          // 流式中收到 reasoning item（只留带 encrypted_content 的）
    if (!item || !item.encrypted_content) return;
    ctx.reasonPending = ctx.reasonPending || [];
    if (!ctx.reasonPending.some((x) => x && x.id && x.id === item.id)) ctx.reasonPending.push(item);
}
function reasonKey(provider, callId) { return (provider ? provider + '|' : '') + callId; }   // 推理链跟供应商绑定（加密内容只有原供应商能解），跨供应商不能复用
function reasonAttach(ctx, callId) {         // 该 call 之前累积的推理链归档到它名下
    if (!ctx.reasonPending || !ctx.reasonPending.length) return;
    reasonStore(reasonKey(ctx.provider, callId), ctx.reasonPending);
}
function injectReasonsIntoInput(input, provider) {   // 直通路：客户端没回传时，由网关把归档的 reasoning item 插回 function_call 之前（只补缺的）
    if (!Array.isArray(input) || !input.length) return { input, injected: 0 };
    const have = new Set();
    for (const it of input) if (it && it.type === 'reasoning' && it.id) have.add(it.id);
    const out = []; let injected = 0; const seen = new Set();
    for (const it of input) {
        if (it && it.type === 'function_call' && it.call_id && !seen.has(it.call_id)) {
            seen.add(it.call_id);
            for (const r of reasonTake(it.call_id, provider)) {
                if (!r || (r.id && have.has(r.id))) continue;
                out.push(r); injected++;
            }
        }
        out.push(it);
    }
    return { input: out, injected };
}

// ---------------- chat -> responses 请求翻译 ----------------
const textOf = (c) => typeof c === 'string' ? c : Array.isArray(c) ? c.map((p) => p?.text ?? '').join('') : (c == null ? '' : jdump(c));
function chatToResponses(body, opts) {
    const echoReason = !!(opts && opts.echoReason);   // 把上一轮归档的 reasoning item 插回对应 function_call 之前
    const onInject = opts && opts.onInject;           // 回报本次注入了多少条（用于统计/诊断缓存变化）
    const injected = new Set();
    const msgs = body.messages || [];
    const instr = msgs.filter((m) => m.role === 'system' || m.role === 'developer').map((m) => textOf(m.content)).filter(Boolean).join('\n\n');
    const input = [];
    for (const m of msgs) {
        if (m.role === 'system' || m.role === 'developer') continue;
        if (m.role === 'tool') {
            input.push({ type: 'function_call_output', call_id: m.tool_call_id, output: textOf(m.content) });
            continue;
        }
        if (m.role === 'assistant' && Array.isArray(m.tool_calls) && m.tool_calls.length) {
            if (m.content) input.push({ role: 'assistant', content: textOf(m.content) });
            for (const tc of m.tool_calls) {
                if (echoReason && tc.id && !injected.has(tc.id)) {
                    injected.add(tc.id);
                    const its = reasonTake(tc.id, opts.provider);
                    if (its.length) {
                        if (onInject) onInject(its.length);   // 统计/诊断：本次注入条数（记进 stats 的 reason_inj）
                        console.log(`[reason] 注入 ${its.length} 条 reasoning -> ${tc.id}`);
                        for (const it of its) input.push(it);
                    }   // 同一 call_id 只插一次，且插在它自己的 function_call 之前
                }
                input.push({ type: 'function_call', call_id: tc.id, name: tc.function?.name || '', arguments: tc.function?.arguments || '{}' });
            }
            continue;
        }
        input.push({ role: m.role, content: textOf(m.content) });
    }
    const out = { model: body.model, input, stream: !!body.stream };
    if (instr) out.instructions = instr;
    if (body.max_tokens != null) out.max_output_tokens = body.max_tokens;
    if (body.max_completion_tokens != null) out.max_output_tokens = body.max_completion_tokens;
    if (body.temperature != null) out.temperature = body.temperature;
    if (body.top_p != null) out.top_p = body.top_p;
    if (body.stop != null) out.stop = body.stop;
    if (Array.isArray(body.tools) && body.tools.length) {
        out.tools = body.tools.map((t) => t.type === 'function'
            ? { type: 'function', name: t.function?.name, description: t.function?.description, parameters: t.function?.parameters }
            : t);
        if (body.tool_choice != null) out.tool_choice = body.tool_choice;
    }
    // 推理档位 / 存储语义透传（客户端显式给了才带，别加默认值）
    if (body.reasoning && typeof body.reasoning === 'object') out.reasoning = body.reasoning;
    else if (body.reasoning_effort != null) out.reasoning = { effort: body.reasoning_effort };
    if (body.store != null) out.store = body.store;
    if (echoReason) {
        // store:false（无状态）下，只有显式要 encrypted_content 才能把推理链原样交还上游
        const inc = Array.isArray(body.include) ? [...body.include] : [];
        if (!inc.includes('reasoning.encrypted_content')) inc.push('reasoning.encrypted_content');
        out.include = inc;
    }
    return out;
}

// ---------------- responses SSE -> chat SSE ----------------
function sseHandle(raw, ctx, emit) {
    let ev = '', data = '';
    for (const line of raw.split('\n')) {
        if (line.startsWith('event:')) ev = line.slice(6).trim();
        else if (line.startsWith('data:')) data += line.slice(5).trim();
    }
    if (!data) return;
    let d; try { d = JSON.parse(data); } catch { return; }
    const type = d.type || ev;
    switch (type) {
        case 'response.created':
            ctx.reasonPending = [];                       // 新一轮响应：清掉上一轮的待归档推理链
            emit.start(); break;
        case 'response.reasoning_summary_text.delta':
        case 'response.reasoning_text.delta':
            emit.start(); emit.delta({ reasoning_content: d.delta || '' }); break;
        case 'response.output_text.delta':
            emit.start(); emit.delta({ content: d.delta || '' }); break;
        case 'response.output_item.added': {
            const it = d.item || {};
            if (it.type === 'reasoning') { reasonCapture(ctx, it); break; }
            if (it.type === 'function_call') {
                reasonAttach(ctx, it.call_id || it.id);   // 归档到 call_id：下一轮客户端回传同一个 call_id 时插回
                emit.start(); emit.tool({ id: it.call_id || it.id, name: it.name || '', argsDelta: '' }); ctx.hasTool = true;
                if (typeof it.arguments === 'string' && it.arguments) emit.tool({ argsDelta: it.arguments });   // 少数上游在 added 里就给全参数
            }
            break;
        }
        case 'response.output_item.done': {
            const it = d.item || {};
            if (it.type === 'reasoning') { reasonCapture(ctx, it); break; }
            // 有些上游不发 delta，只在 done 的 item 里带完整 arguments
            if (it.type === 'function_call') {
                reasonAttach(ctx, it.call_id || it.id);
                if (typeof it.arguments === 'string' && it.arguments && emit.toolSent() === 0) emit.tool({ argsDelta: it.arguments });
            }
            break;
        }
        case 'response.function_call_arguments.delta': {
            const dl = (typeof d.delta === 'string' ? d.delta : '') || (typeof d.arguments === 'string' ? d.arguments : '');
            if (dl) emit.tool({ argsDelta: dl });
            break;
        }
        case 'response.function_call_arguments.done': {
            const full = typeof d.arguments === 'string' ? d.arguments : '';
            if (full && emit.toolSent() === 0) emit.tool({ argsDelta: full });   // 只在 done 给全参数的上游
            break;
        }
        case 'response.completed':
            ctx.usage = usageFromResponses(d.response?.usage);
            if (d.response?.status && d.response.status !== 'completed') ctx.upstreamError = `upstream status=${d.response.status}`;
            break;
        case 'response.failed':
        case 'error': {
            const msg = d.error?.message || d.response?.error?.message || 'upstream error';
            ctx.upstreamError = msg;
            break;
        }
        default: break;
    }
}

function responsesToChatJson(d, ctx) {
    const texts = [], reasoning = [], toolCalls = [];
    const pending = [];
    for (const item of d.output || []) {
        if (item.type === 'message') {
            for (const c of item.content || []) if (c.type === 'output_text') texts.push(c.text || '');
        } else if (item.type === 'reasoning') {
            for (const s of item.summary || []) if (s.text) reasoning.push(s.text);
            if (Array.isArray(item.content)) for (const c of item.content) if (c.text) reasoning.push(c.text);
            if (item.encrypted_content) pending.push(item);
        } else if (item.type === 'function_call') {
            if (pending.length) reasonStore(reasonKey(ctx.provider, item.call_id || item.id), pending.slice());   // 非流式同样归档推理链
            toolCalls.push({ id: item.call_id || item.id, type: 'function', function: { name: item.name, arguments: item.arguments || '{}' } });
        }
    }
    const msg = { role: 'assistant', content: texts.join('') || null };
    if (reasoning.length) msg.reasoning_content = reasoning.join('\n');
    if (toolCalls.length) msg.tool_calls = toolCalls;
    return {
        id: ctx.id, object: 'chat.completion', created: ctx.created, model: ctx.model,
        choices: [{ index: 0, message: msg, finish_reason: toolCalls.length ? 'tool_calls' : 'stop' }],
        usage: usageToChat(ctx.usage = usageFromResponses(d.usage)),
    };
}

// ---------------- 原生协议直通的 usage 提取（尽量只扫尾部，避免额外开销）----------------
function lastEventType(tail) {          // 诊断：流里最后一个事件类型，用来区分“没收到 completed” vs “completed 里没 usage”
    const lines = tail.split('\n');
    for (let i = lines.length - 1; i >= 0; i--) {
        const l = lines[i];
        if (!l.startsWith('data:')) continue;
        try {
            const d = JSON.parse(l.slice(5).trim());
            if (d && d.type) return d.type;
            return d ? '(无 type)' : '';
        } catch { return '(该行 JSON 解析失败)'; }
    }
    return '';
}
function usageFromResponsesTail(tail) {
    let us = {};
    for (const line of tail.split('\n')) {
        if (!line.startsWith('data:')) continue;
        let d; try { d = JSON.parse(line.slice(5).trim()); } catch { continue; }
        if (!d || typeof d !== 'object') continue;
        const t = d.type || '';
        const u = (d.response && d.response.usage) || d.usage || null;      // 有的中转站把 usage 放在顶层
        if (u && (t === '' || /^response\.(completed|done|incomplete|failed)$/.test(t))) us = usageFromResponses(u);
    }
    return us;
}
function usageFromMessagesTail(tail) {
    const us = {};
    for (const line of tail.split('\n')) {
        if (!line.startsWith('data:')) continue;
        let d; try { d = JSON.parse(line.slice(5).trim()); } catch { continue; }
        const t = d && d.type;
        if (t === 'message_start' && d.message && d.message.usage) {
            const u = d.message.usage;
            us.in = u.input_tokens || 0;
            us.cached = u.cache_read_input_tokens || 0;
            us.out = u.output_tokens || 0;
        } else if (t === 'message_delta' && d.usage) {
            if (typeof d.usage.output_tokens === 'number') us.out = d.usage.output_tokens;
            if (typeof d.usage.input_tokens === 'number') us.in = d.usage.input_tokens;
            if (typeof d.usage.cache_read_input_tokens === 'number') us.cached = d.usage.cache_read_input_tokens;
        }
    }
    return us;
}
function usageFromLine(kind, d) {        // 单行事件 → usage（responses / messages 两种形态）
    if (kind === 'responses') {
        const t = d.type || '';
        const u = (d.response && d.response.usage) || d.usage || null;
        if (!u) return null;
        if (t && !/^response\.(completed|done|incomplete|failed)$/.test(t)) return null;
        return usageFromResponses(u);
    }
    if (d.type === 'message_start' && d.message && d.message.usage) return usageFromMessages(d.message.usage);
    if (d.type === 'message_delta' && d.usage) return usageFromMessages(d.usage);
    return null;
}
function usageFromTail(kind, tail) {
    return kind === 'responses' ? usageFromResponsesTail(tail) : usageFromMessagesTail(tail);
}

// ---------------- chat 请求翻译 -> Anthropic Messages ----------------
function safeJson(s) { try { return JSON.parse(s || '{}'); } catch { return {}; } }
function chatToMessages(body, model, providerCfg) {
    const msgs = body.messages || [];
    const system = msgs.filter((m) => m.role === 'system' || m.role === 'developer').map((m) => textOf(m.content)).filter(Boolean).join('\n\n');
    const out = [];
    for (const m of msgs) {
        if (m.role === 'system' || m.role === 'developer') continue;
        if (m.role === 'tool') {
            out.push({ role: 'user', content: [{ type: 'tool_result', tool_use_id: m.tool_call_id, content: textOf(m.content) }] });
            continue;
        }
        if (m.role === 'assistant' && Array.isArray(m.tool_calls) && m.tool_calls.length) {
            const parts = [];
            if (m.content) parts.push({ type: 'text', text: textOf(m.content) });
            for (const tc of m.tool_calls) parts.push({ type: 'tool_use', id: tc.id, name: tc.function?.name || '', input: safeJson(tc.function?.arguments) });
            out.push({ role: 'assistant', content: parts });
            continue;
        }
        out.push({ role: m.role === 'assistant' ? 'assistant' : 'user', content: textOf(m.content) });
    }
    const payload = {
        model,
        messages: out,
        max_tokens: body.max_tokens ?? body.max_completion_tokens ?? 4096,   // Anthropic 必填
        stream: !!body.stream,
    };
    if (system) payload.system = system;
    if (body.temperature != null) payload.temperature = body.temperature;
    if (body.top_p != null) payload.top_p = body.top_p;
    if (body.stop != null) payload.stop_sequences = Array.isArray(body.stop) ? body.stop : [body.stop];
    if (Array.isArray(body.tools) && body.tools.length) {
        payload.tools = body.tools.filter((t) => t.type === 'function').map((t) => ({
            name: t.function?.name, description: t.function?.description, input_schema: t.function?.parameters || { type: 'object', properties: {} },
        }));
    }
    // 推理档位 → messages 端点。形状因上游而异：实测有的上游只认 thinking.type=adaptive + output_config.effort，
    // 发 thinking.type=enabled 直接 400。所以按 provider.thinking_style 走，缺省 none＝完全不发（宁可不带，也不 400）。
    if (body.thinking && typeof body.thinking === 'object') payload.thinking = body.thinking;
    if (body.output_config && typeof body.output_config === 'object') payload.output_config = body.output_config;
    const style = (providerCfg || {}).thinking_style || 'none';
    const eff = body.reasoning_effort;
    if (eff != null && style !== 'none') {
        if (style === 'adaptive') {                      // 实测形状（adaptive 上游的 /messages）
            if (!payload.thinking) payload.thinking = { type: 'adaptive' };
            if (!payload.output_config) payload.output_config = { effort: eff };
        } else if (style === 'enabled') {                 // Anthropic 原生：max_tokens 必须 > budget_tokens
            // 档位 → 思考预算走全局 EFFORT_BUDGET（唯一来源，见「推理档位能力层」）。
            // high 以上不能缺：max/ultra 漏在表外会静默按 medium 兜底（预算 8192），
            // 客户端以为在「最强档」其实是最弱的一档。ultra 是 Hermes 内部档位，语义等同 max
            // （对齐 agent/anthropic_adapter.py 的 ADAPTIVE_EFFORT_MAP: ultra→max）。
            const effKey = String(eff).toLowerCase();
            if (!(effKey in EFFORT_BUDGET)) log('推理档位未知，按 medium(8192) 兜底:', eff);
            const budget = EFFORT_BUDGET[effKey] || 8192;
            if (payload.max_tokens <= budget) payload.max_tokens = budget + 4096;
            if (!payload.thinking) payload.thinking = { type: 'enabled', budget_tokens: budget };
        }
    }
    return payload;
}

// ---------------- Gemini（Google Generative Language）协议 ----------------
// 用途：部分上游的 gemini-* 模型只认原生 Google 协议（/chat/completions、/responses、/messages 全回
// 400 ModelProtocolUnsupported），必须按 contents/parts 发。这里只做「上游一侧」的翻译：
//   请求  chat → generateContent 载荷        chatToGemini()
//   流式  generateContent SSE → chat 事件     pumpGemini()
//   非流   generateContent JSON → chat JSON   geminiToChatJson()
// 客户端那侧仍是 chat（网关的 chat 中枢），所以 responses/messages 客户端也能间接受益，不需要额外实现。
const GEMINI_THINKING_LEVELS = ['low', 'medium', 'high'];   // 实测：minimal/xhigh 直接 400，thinkingBudget 也不支持
// 阶梯就近降级：gemini 只有三档，低了往上报、高了往下压（只降不升是上游侧原则，但 minimal 必须抬到 low 才不 400）
function geminiThinkingLevel(effort) {
    const e = String(effort || '').trim().toLowerCase();
    if (!e) return null;
    if (GEMINI_THINKING_LEVELS.includes(e)) return e;
    const wi = EFFORT_LADDER.indexOf(e);
    if (wi < 0) return null;                                   // 自定义档位名：不发，交给上游默认
    const below = GEMINI_THINKING_LEVELS.filter((l) => EFFORT_LADDER.indexOf(l) <= wi);
    return below.length ? below[below.length - 1] : GEMINI_THINKING_LEVELS[0];   // minimal → low
}
// chart 图片块 → inlineData（chat 的 image_url 可能是 data: URL，也可能是 http(s)，后者 Gemini 不收，直接丢）
function geminiPartsOf(content) {
    if (typeof content === 'string') return content ? [{ text: content }] : [];
    if (!Array.isArray(content)) return content == null ? [] : [{ text: jdump(content) }];
    const parts = [];
    for (const p of content) {
        if (p == null) continue;
        if (typeof p === 'string') { if (p) parts.push({ text: p }); continue; }
        if (p.type === 'text' && p.text) { parts.push({ text: p.text }); continue; }
        const url = p.type === 'image_url' ? (p.image_url?.url || p.image_url) : p.type === 'image' ? (p.source?.data ? null : p.source?.url) : null;
        const dataUrl = typeof url === 'string' && url.startsWith('data:') ? url : (p.source?.data ? `data:${p.source.media_type || 'image/png'};base64,${p.source.data}` : null);
        if (dataUrl) {
            const m = /^data:([^;,]+);base64,(.*)$/s.exec(dataUrl);
            if (m) { parts.push({ inlineData: { mimeType: m[1], data: m[2] } }); continue; }
        }
        // 非 data: 的远程图片 URL：Gemini 原生只认 fileData（需先上传），这里退化成文本提示，避免整条请求 400
        if (typeof url === 'string' && url) parts.push({ text: `[图片: ${url}]` });
    }
    return parts;
}
function chatToGemini(body, model, providerCfg) {
    const msgs = body.messages || [];
    const sysParts = [];
    const contents = [];
    // tool_call_id → 函数名：Gemini 的 functionResponse 必须带 name（不像 OpenAI 只认 call_id）
    const nameByCall = new Map();
    for (const m of msgs) for (const tc of m?.tool_calls || []) if (tc?.id) nameByCall.set(tc.id, tc.function?.name || '');
    for (const m of msgs) {
        if (!m || !m.role) continue;
        if (m.role === 'system' || m.role === 'developer') { const t = textOf(m.content); if (t) sysParts.push({ text: t }); continue; }
        if (m.role === 'tool') {                                    // 工具结果 → user 里的 functionResponse
            const name = nameByCall.get(m.tool_call_id) || m.name || 'tool';
            const resp = { name, response: { output: textOf(m.content) } };
            if (m.tool_call_id) resp.id = m.tool_call_id;
            contents.push({ role: 'user', parts: [{ functionResponse: resp }] });
            continue;
        }
        if (m.role === 'assistant') {
            const parts = [];
            const t = textOf(m.content);
            if (t) parts.push({ text: t });
            for (const tc of m.tool_calls || []) {
                if (!tc) continue;
                const args = typeof tc.function?.arguments === 'string' ? safeJson(tc.function.arguments) : (tc.function?.arguments || {});
                const fc = { name: tc.function?.name || '', args: args && typeof args === 'object' ? args : {} };
                if (tc.id) fc.id = tc.id;
                parts.push({ functionCall: fc });
            }
            if (parts.length) contents.push({ role: 'model', parts });
            continue;
        }
        const parts = geminiPartsOf(m.content);                      // user
        if (parts.length) contents.push({ role: 'user', parts });
    }
    const gen = {};
    const maxTok = body.max_tokens ?? body.max_completion_tokens;
    if (maxTok != null) gen.maxOutputTokens = maxTok;               // 实测思考 token 也计入这个上限
    if (body.temperature != null) gen.temperature = body.temperature;
    if (body.top_p != null) gen.topP = body.top_p;
    if (body.stop != null) { const s = Array.isArray(body.stop) ? body.stop : [body.stop]; if (s.length) gen.stopSequences = s.slice(0, 5); }
    const payload = { contents };
    if (sysParts.length) payload.systemInstruction = { parts: sysParts };
    if (Array.isArray(body.tools) && body.tools.length) {
        const decls = body.tools.filter((t) => t.type === 'function' && t.function?.name).map((t) => ({
            name: t.function.name,
            ...(t.function.description ? { description: t.function.description } : {}),
            // 实测 parameters 直接吃小写 JSON Schema（OpenAI 风），不用转大写 Type
            parameters: t.function.parameters || { type: 'object', properties: {} },
        }));
        if (decls.length) payload.tools = [{ functionDeclarations: decls }];
        const tc = body.tool_choice;
        if (tc != null) {
            if (tc === 'none') payload.toolConfig = { functionCallingConfig: { mode: 'NONE' } };
            else if (tc === 'required' || tc === 'any') payload.toolConfig = { functionCallingConfig: { mode: 'ANY' } };
            else if (tc === 'auto') payload.toolConfig = { functionCallingConfig: { mode: 'AUTO' } };
            else if (typeof tc === 'object') {
                const nm = tc.function?.name || tc.name;
                if (nm) payload.toolConfig = { functionCallingConfig: { mode: 'ANY', allowedFunctionNames: [nm] } };
            }
        }
    }
    // 推理档位：只有 provider.thinking_style 显式开（'gemini' / 'adaptive'）才发，缺省 none＝不发（宁可不带，也不 400）
    const style = (providerCfg || {}).thinking_style || 'none';
    const lvl = style === 'none' ? null : geminiThinkingLevel(body.reasoning_effort);
    if (lvl) gen.thinkingConfig = { includeThoughts: true, thinkingLevel: lvl };
    if (Object.keys(gen).length) payload.generationConfig = gen;
    return payload;
}
const usageFromGemini = (u) => u ? {
    in: u.promptTokenCount || 0,
    out: u.candidatesTokenCount || 0,
    total: u.totalTokenCount || ((u.promptTokenCount || 0) + (u.candidatesTokenCount || 0)),
    cached: u.cachedContentTokenCount || 0,
    reasoning: u.thoughtsTokenCount || 0,          // 思考 token 单独计（不算进 candidatesTokenCount）
} : {};
// 一帧 generateContent 响应 → 文本/思考/工具调用（流式与非流式共用）
function geminiFrame(d, ctx, sink) {
    if (!d || typeof d !== 'object') return;
    if (d.error) { ctx.upstreamError = d.error.message || d.error.status || 'gemini upstream error'; return; }
    if (d.usageMetadata) ctx.usage = usageFromGemini(d.usageMetadata);
    if (d.promptFeedback?.blockReason) ctx.upstreamError = `内容被上游拦截: ${d.promptFeedback.blockReason}`;
    for (const c of d.candidates || []) {
        for (const p of c.content?.parts || []) {
            if (typeof p.text === 'string' && p.text) sink.delta(p.thought ? { reasoning_content: p.text } : { content: p.text });
            if (p.functionCall) {
                const f = p.functionCall;
                // Gemini 的 args 是对象、且整块到达（无增量）。这里必须分两次调用 sink：
                // 三个 sink 的 tool() 都是 if(t.id)…else if(t.argsDelta)，一帧同时带 id 和参数会被丢掉（实测踩过）。
                sink.tool({ id: f.id || randId('call_'), name: f.name || '' });
                const argStr = jdump(f.args || {});
                if (argStr) sink.tool({ argsDelta: argStr });   // args 是对象（OpenAI chat 要 JSON 字符串）
            }
        }
        if (c.finishReason) ctx.gFinish = c.finishReason;
    }
}
async function pumpGemini(upRes, ctx, sink) {          // :streamGenerateContent?alt=sse → sink
    const reader = upRes.body.getReader(); const dec = new TextDecoder();
    let buf = '';
    const eat = (raw) => {
        for (const line of raw.split('\n')) {
            const s = line.trim();
            if (!s || s.startsWith(':')) continue;
            const payload = s.startsWith('data:') ? s.slice(5).trim() : (s.charAt(0) === '{' ? s : '');
            if (!payload || payload === '[DONE]') continue;
            let d; try { d = JSON.parse(payload); } catch { continue; }
            geminiFrame(d, ctx, sink);
        }
    };
    for (; ;) {
        const { done, value } = await reader.read(); if (done) break;
        if (value) ctx.up_res_bytes = (ctx.up_res_bytes || 0) + value.length;
        buf += dec.decode(value, { stream: true });
        let i;
        while ((i = buf.indexOf('\n\n')) >= 0) { const raw = buf.slice(0, i); buf = buf.slice(i + 2); eat(raw); }
    }
    if (buf.trim()) eat(buf);
    if (ctx.gFinish === 'MAX_TOKENS') ctx.finishReason = 'length';
}
function geminiToChatJson(d, ctx) {                    // 非流式：整包 → chat completion
    const texts = [], reasoning = [], toolCalls = [];
    for (const c of d?.candidates || []) {
        for (const p of c.content?.parts || []) {
            if (typeof p.text === 'string' && p.text) (p.thought ? reasoning : texts).push(p.text);
            if (p.functionCall) toolCalls.push({
                id: p.functionCall.id || randId('call_'), type: 'function',
                function: { name: p.functionCall.name || '', arguments: jdump(p.functionCall.args || {}) },
            });
        }
        if (c.finishReason) ctx.gFinish = c.finishReason;
    }
    ctx.usage = usageFromGemini(d?.usageMetadata);
    const blocked = d?.promptFeedback?.blockReason;
    const msg = { role: 'assistant', content: texts.join('') || (toolCalls.length || blocked ? null : '') };
    if (reasoning.length) msg.reasoning_content = reasoning.join('');
    if (toolCalls.length) msg.tool_calls = toolCalls;
    const fr = toolCalls.length ? 'tool_calls' : (ctx.gFinish === 'MAX_TOKENS' ? 'length' : 'stop');
    return {
        id: ctx.id, object: 'chat.completion', created: ctx.created, model: ctx.model,
        choices: [{ index: 0, message: msg, finish_reason: fr }],
        usage: usageToChat(ctx.usage),
    };
}

// ---------------- Anthropic Messages -> chat ----------------
const usageFromMessages = (u) => u ? {
    in: (u.input_tokens || 0) + (u.cache_read_input_tokens || 0) + (u.cache_creation_input_tokens || 0),
    out: u.output_tokens || 0,
    cached: u.cache_read_input_tokens || 0,
    reasoning: 0,
} : {};

function messagesToChatJson(d, ctx) {
    const texts = [], reasoning = [], toolCalls = [];
    for (const b of d.content || []) {
        if (b.type === 'text') texts.push(b.text || '');
        else if (b.type === 'thinking') reasoning.push(b.thinking || '');
        else if (b.type === 'tool_use') toolCalls.push({ id: b.id, type: 'function', function: { name: b.name, arguments: jdump(b.input || {}) } });
    }
    ctx.usage = usageFromMessages(d.usage);
    const msg = { role: 'assistant', content: texts.join('') || null };
    if (reasoning.length) msg.reasoning_content = reasoning.join('\n');
    if (toolCalls.length) msg.tool_calls = toolCalls;
    return {
        id: ctx.id, object: 'chat.completion', created: ctx.created, model: ctx.model,
        choices: [{ index: 0, message: msg, finish_reason: toolCalls.length ? 'tool_calls' : (d.stop_reason === 'max_tokens' ? 'length' : 'stop') }],
        usage: usageToChat(ctx.usage),
    };
}

// ---------------- 协议互转：三种客户端协议 × 三种上游协议 ----------------
// 中枢＝chat。非 chat 客户端先把请求翻成 chat（responsesToChatReq / messagesToChatReq），
// 复用既有的 chat→上游 路由与翻译（buildUpstream）；上游响应先归一到 chat 事件（pump*），
// 再由对应 sink 渲染成客户端自己的协议（chatSink / responsesSink / messagesSink）。
// 同协议（responses↔responses、messages↔messages）仍走原生直通，保真度最高；本层只在协议不匹配时启用。
// 口径参考 LiteLLM 的 v1/messages→/responses 映射表；responses 事件顺序按 OpenAI/Azure 官方流：
//   response.created → in_progress → output_item.added → content_part.added → output_text.delta
//   → output_text.done → content_part.done → output_item.done → response.completed
// —— 缺 content_part.added 会让 Codex 报 "OutputTextDelta without active item" 并**丢弃全部正文**（实测先例）。
const SSE_HEADERS = { 'Content-Type': 'text/event-stream; charset=utf-8', 'Cache-Control': 'no-cache', 'Connection': 'keep-alive', 'X-Accel-Buffering': 'no' };
const randId = (p) => p + crypto.randomBytes(12).toString('hex');
const usageToResponses = (u) => (u && Object.keys(u).length) ? {
    input_tokens: u.in || 0, output_tokens: u.out || 0, total_tokens: u.total || ((u.in || 0) + (u.out || 0)),
    input_tokens_details: { cached_tokens: u.cached || 0 },
    output_tokens_details: { reasoning_tokens: u.reasoning || 0 },
} : null;
const finishOf = (ctx) => ctx.finishReason || (ctx.hasTool ? 'tool_calls' : 'stop');
const normToolChoice = (tc, from) => {
    if (tc == null || typeof tc === 'string') return tc;
    if (from === 'anthropic') {
        if (tc.type === 'any') return 'required';
        if (tc.type === 'tool') return { type: 'function', function: { name: tc.name } };
        return tc.type;
    }
    if (tc.type === 'function') return { type: 'function', function: { name: tc.name || tc.function?.name } };
    return tc;
};

// ---- sink: chat SSE（与旧 relay* 直接写出的形状等价）----
function chatSink(res, ctx) {
    let started = false, toolIdx = -1, curIdx = null;
    const byUp = new Map(), sentByIdx = new Map();
    const mark = () => { if (ctx.ttft == null) ctx.ttft = Date.now() - ctx.t0; };
    const send = (o) => res.write('data: ' + jdump(o) + '\n\n');
    const chunk = (delta, fr = null) => send({ id: ctx.id, object: 'chat.completion.chunk', created: ctx.created, model: ctx.model, choices: [{ index: 0, delta, finish_reason: fr }] });
    const s = {
        start() { if (!started) { started = true; chunk({ role: 'assistant' }); } },
        delta(d) { if (!d) return; mark(); s.start(); chunk(d); },
        tool(t) {
            s.start();
            if (t.id) {
                const idx = ++toolIdx; curIdx = idx; byUp.set(t.id, idx);
                if (t.upIndex !== undefined) byUp.set(t.upIndex, idx);
                sentByIdx.set(idx, 0); ctx.hasTool = true; mark();
                chunk({ tool_calls: [{ index: idx, id: t.id, type: 'function', function: { name: t.name || '', arguments: '' } }] });
            } else if (t.argsDelta) {
                const idx = (t.upIndex !== undefined && byUp.has(t.upIndex)) ? byUp.get(t.upIndex) : curIdx;
                if (idx == null) return;
                sentByIdx.set(idx, (sentByIdx.get(idx) || 0) + t.argsDelta.length);
                chunk({ tool_calls: [{ index: idx, function: { arguments: t.argsDelta } }] });
            }
        },
        toolSent() { return curIdx == null ? 0 : (sentByIdx.get(curIdx) || 0); },
        error(msg) { try { send({ error: { message: msg, type: 'upstream_error' } }); } catch { } },
        finalize(fr, usage) {
            s.start();
            chunk({}, fr || 'stop');
            if (usage && Object.keys(usage).length) send({ id: ctx.id, object: 'chat.completion.chunk', created: ctx.created, model: ctx.model, choices: [], usage: usageToChat(usage) });
            res.write('data: [DONE]\n\n');
        },
    };
    return s;
}

// ---- sink: OpenAI Responses SSE（严格按官方事件顺序，Codex 依赖 content_part.added）----
function responsesSink(res, ctx) {
    const respId = 'resp_' + ctx.id.replace(/^chatcmpl-/, '');
    const base = { id: respId, object: 'response', created_at: ctx.created, model: ctx.model };
    const items = [];
    let started = false, seq = 0, curIdx = null;
    let reason = null, msg = null;
    const byUp = new Map(), toolItems = new Map();
    const mark = () => { if (ctx.ttft == null) ctx.ttft = Date.now() - ctx.t0; };
    const ev = (type, o) => res.write(`event: ${type}\ndata: ${jdump({ type, ...o })}\n\n`);
    const snap = (status) => ({ ...base, status, output: items.slice(), usage: null });
    const ensure = () => { if (!started) { started = true; ev('response.created', { response: snap('in_progress') }); ev('response.in_progress', { response: snap('in_progress') }); } };
    const closeReason = () => {
        if (!reason || reason.closed) return;
        reason.closed = true;
        ev('response.reasoning_summary_text.done', { item_id: reason.id, output_index: reason.idx, summary_index: 0, text: reason.text });
        ev('response.reasoning_summary_part.done', { item_id: reason.id, output_index: reason.idx, summary_index: 0, part: { type: 'summary_text', text: reason.text } });
        const it = { id: reason.id, type: 'reasoning', summary: [{ type: 'summary_text', text: reason.text }] };
        ev('response.output_item.done', { output_index: reason.idx, item: { ...it, status: 'completed' } });
        items.push(it);
    };
    const closeMsg = () => {
        if (!msg || msg.closed) return;
        msg.closed = true;
        ev('response.output_text.done', { item_id: msg.id, output_index: msg.idx, content_index: 0, text: msg.text });
        ev('response.content_part.done', { item_id: msg.id, output_index: msg.idx, content_index: 0, part: { type: 'output_text', text: msg.text, annotations: [] } });
        const it = { id: msg.id, type: 'message', status: 'completed', role: 'assistant', content: [{ type: 'output_text', text: msg.text, annotations: [] }] };
        ev('response.output_item.done', { output_index: msg.idx, item: it });
        items.push(it);
    };
    const s = {
        start() { ensure(); },
        delta(d) {
            if (!d) return; ensure(); mark();
            if (d.reasoning_content) {
                if (!reason) {
                    reason = { idx: seq++, id: randId('rs_'), text: '' };
                    ev('response.output_item.added', { output_index: reason.idx, item: { id: reason.id, type: 'reasoning', summary: [] } });
                    ev('response.reasoning_summary_part.added', { item_id: reason.id, output_index: reason.idx, summary_index: 0, part: { type: 'summary_text', text: '' } });
                }
                reason.text += d.reasoning_content;
                ev('response.reasoning_summary_text.delta', { item_id: reason.id, output_index: reason.idx, summary_index: 0, delta: d.reasoning_content });
            }
            if (d.content) {
                closeReason();
                if (!msg) {
                    msg = { idx: seq++, id: randId('msg_'), text: '' };
                    ev('response.output_item.added', { output_index: msg.idx, item: { id: msg.id, type: 'message', status: 'in_progress', role: 'assistant', content: [] } });
                    ev('response.content_part.added', { item_id: msg.id, output_index: msg.idx, content_index: 0, part: { type: 'output_text', text: '', annotations: [] } });
                }
                msg.text += d.content;
                ev('response.output_text.delta', { item_id: msg.id, output_index: msg.idx, content_index: 0, delta: d.content });
            }
        },
        tool(t) {
            ensure(); closeReason(); closeMsg(); mark();
            if (t.id) {
                const idx = seq++; curIdx = idx;
                byUp.set(t.id, idx); if (t.upIndex !== undefined) byUp.set(t.upIndex, idx);
                const item = { id: 'fc_' + crypto.randomBytes(12).toString('hex'), type: 'function_call', status: 'in_progress', call_id: t.id, name: t.name || '', arguments: '' };
                toolItems.set(idx, item); ctx.hasTool = true;
                ev('response.output_item.added', { output_index: idx, item: { ...item } });
            } else if (t.argsDelta) {
                const idx = (t.upIndex !== undefined && byUp.has(t.upIndex)) ? byUp.get(t.upIndex) : curIdx;
                const item = idx != null ? toolItems.get(idx) : null;
                if (!item) return;
                item.arguments += t.argsDelta;
                ev('response.function_call_arguments.delta', { item_id: item.id, output_index: idx, delta: t.argsDelta });
            }
        },
        toolSent() { const it = curIdx != null ? toolItems.get(curIdx) : null; return it ? it.arguments.length : 0; },
        error(m) { ensure(); ev('response.failed', { response: { ...base, status: 'failed', output: items.slice(), error: { message: m, code: 'upstream_error' } } }); },
        finalize(fr, usage) {
            ensure(); closeReason(); closeMsg();
            for (const [idx, item] of toolItems) {
                if (item.closed) continue;
                item.closed = true;
                const it = { id: item.id, type: 'function_call', status: 'completed', call_id: item.call_id, name: item.name, arguments: item.arguments || '{}' };
                ev('response.function_call_arguments.done', { item_id: item.id, output_index: idx, arguments: it.arguments });
                ev('response.output_item.done', { output_index: idx, item: it });
                items.push(it);
            }
            ev('response.completed', { response: { ...base, status: 'completed', output: items.slice(), usage: usageToResponses(usage) } });
        },
    };
    return s;
}

// ---- sink: Anthropic Messages SSE ----
function messagesSink(res, ctx) {
    const msgId = 'msg_' + ctx.id.replace(/^chatcmpl-/, '');
    let started = false, seq = -1, curIdx = null;
    let reason = null, text = null;
    const byUp = new Map(), toolItems = new Map();
    const mark = () => { if (ctx.ttft == null) ctx.ttft = Date.now() - ctx.t0; };
    const ev = (type, o) => res.write(`event: ${type}\ndata: ${jdump({ type, ...o })}\n\n`);
    const ensure = () => { if (!started) { started = true; ev('message_start', { message: { id: msgId, type: 'message', role: 'assistant', model: ctx.model, content: [], stop_reason: null, stop_sequence: null, usage: { input_tokens: 0, output_tokens: 0 } } }); } };
    const closeReason = () => { if (reason && !reason.closed) { reason.closed = true; ev('content_block_stop', { index: reason.idx }); } };
    const closeText = () => { if (text && !text.closed) { text.closed = true; ev('content_block_stop', { index: text.idx }); } };
    const closeTools = () => { for (const [idx, it] of toolItems) if (!it.closed) { it.closed = true; ev('content_block_stop', { index: idx }); } };
    const s = {
        start() { ensure(); },
        delta(d) {
            if (!d) return; ensure(); mark();
            if (d.reasoning_content) {
                if (!reason) { reason = { idx: ++seq }; ev('content_block_start', { index: reason.idx, content_block: { type: 'thinking', thinking: '' } }); }
                ev('content_block_delta', { index: reason.idx, delta: { type: 'thinking_delta', thinking: d.reasoning_content } });
            }
            if (d.content) {
                closeReason();
                if (!text) { text = { idx: ++seq }; ev('content_block_start', { index: text.idx, content_block: { type: 'text', text: '' } }); }
                ev('content_block_delta', { index: text.idx, delta: { type: 'text_delta', text: d.content } });
            }
        },
        tool(t) {
            ensure(); closeReason(); closeText(); mark();
            if (t.id) {
                const idx = ++seq; curIdx = idx;
                byUp.set(t.id, idx); if (t.upIndex !== undefined) byUp.set(t.upIndex, idx);
                toolItems.set(idx, { idx, id: t.id, name: t.name || '', args: '' }); ctx.hasTool = true;
                ev('content_block_start', { index: idx, content_block: { type: 'tool_use', id: t.id, name: t.name || '', input: {} } });
            } else if (t.argsDelta) {
                const idx = (t.upIndex !== undefined && byUp.has(t.upIndex)) ? byUp.get(t.upIndex) : curIdx;
                const it = idx != null ? toolItems.get(idx) : null;
                if (!it) return;
                it.args += t.argsDelta;
                ev('content_block_delta', { index: idx, delta: { type: 'input_json_delta', partial_json: t.argsDelta } });
            }
        },
        toolSent() { const it = curIdx != null ? toolItems.get(curIdx) : null; return it ? it.args.length : 0; },
        error(m) { ensure(); ev('error', { error: { type: 'api_error', message: m } }); },
        finalize(fr, usage) {
            ensure(); closeReason(); closeText(); closeTools();
            const stop = fr === 'tool_calls' ? 'tool_use' : fr === 'length' ? 'max_tokens' : 'end_turn';
            const u = usage || {};
            ev('message_delta', { delta: { stop_reason: stop, stop_sequence: null }, usage: { input_tokens: u.in || 0, output_tokens: u.out || 0, cache_read_input_tokens: u.cached || 0 } });
            ev('message_stop', {});
        },
    };
    return s;
}

// ---- 上游流 → chat 事件（sink）----
async function pumpResponses(upRes, ctx, sink) {   // responses SSE → sink（sseHandle 直接驱动 sink）
    const reader = upRes.body.getReader();
    const dec = new TextDecoder();
    let buf = '';
    for (; ;) {
        const { done, value } = await reader.read();
        if (done) break;
        if (value) ctx.up_res_bytes = (ctx.up_res_bytes || 0) + value.length;
        buf += dec.decode(value, { stream: true });
        let i;
        while ((i = buf.indexOf('\n\n')) >= 0) {
            const raw = buf.slice(0, i); buf = buf.slice(i + 2);
            if (raw.trim()) sseHandle(raw, ctx, sink);
        }
    }
    if (buf.trim()) sseHandle(buf, ctx, sink);
}
async function pumpMessages(upRes, ctx, sink) {    // Anthropic messages SSE → sink
    const reader = upRes.body.getReader(); const dec = new TextDecoder();
    let buf = '', cur = null, stopReason = 'end_turn';
    for (; ;) {
        const { done, value } = await reader.read(); if (done) break;
        if (value) ctx.up_res_bytes = (ctx.up_res_bytes || 0) + value.length;
        buf += dec.decode(value, { stream: true });
        let i;
        while ((i = buf.indexOf('\n\n')) >= 0) {
            const raw = buf.slice(0, i); buf = buf.slice(i + 2);
            const line = raw.split('\n').find((l) => l.startsWith('data:'));
            if (!line) continue;
            let d; try { d = JSON.parse(line.slice(5).trim()); } catch { continue; }
            if (d.type === 'message_start') { ctx.usage = usageFromMessages(d.message?.usage); }
            else if (d.type === 'content_block_start') {
                const cb = d.content_block || {};
                if (cb.type === 'tool_use') {
                    cur = { id: cb.id, sent: 0 };
                    sink.tool({ id: cb.id, name: cb.name || '', argsDelta: '' });
                    if (cb.input && typeof cb.input === 'object' && Object.keys(cb.input).length) { const str = jdump(cb.input); cur.sent += str.length; sink.tool({ argsDelta: str }); }
                } else if (cb.type === 'thinking' && cb.thinking) sink.delta({ reasoning_content: cb.thinking });
                else if (cb.type === 'text' && cb.text) sink.delta({ content: cb.text });
            } else if (d.type === 'content_block_delta') {
                const dl = d.delta || {};
                if (dl.type === 'text_delta') sink.delta({ content: dl.text || '' });
                else if (dl.type === 'thinking_delta') sink.delta({ reasoning_content: dl.thinking || '' });
                else if (dl.type === 'input_json_delta' && cur) { const p = dl.partial_json || ''; if (p) { cur.sent += p.length; sink.tool({ argsDelta: p }); } }
            } else if (d.type === 'content_block_stop') {
                const cb = d.content_block || {};
                if (cur && cur.sent === 0 && cb.type === 'tool_use' && cb.input && Object.keys(cb.input).length) { const str = jdump(cb.input); cur.sent += str.length; sink.tool({ argsDelta: str }); }
                if (cb.type === 'tool_use') cur = null;
            } else if (d.type === 'message_delta') {
                if (d.usage?.output_tokens != null) { ctx.usage = ctx.usage || {}; ctx.usage.out = d.usage.output_tokens; }
                if (d.usage?.input_tokens != null) { ctx.usage = ctx.usage || {}; ctx.usage.in = d.usage.input_tokens; }
                if (d.delta?.stop_reason) stopReason = d.delta.stop_reason;
            } else if (d.type === 'error') ctx.upstreamError = d.error?.message || 'messages stream error';
        }
    }
    if (stopReason === 'tool_use') ctx.hasTool = true;
    if (stopReason === 'max_tokens') ctx.finishReason = 'length';
    ctx.stopReason = stopReason;
}
async function pumpChat(upRes, ctx, sink) {        // chat SSE → sink（转换路径用；chat→chat 仍走原样透传）
    const reader = upRes.body.getReader(); const dec = new TextDecoder();
    let buf = '', fr = null;
    const handle = (payload) => {
        if (!payload || payload === '[DONE]') return;
        let d; try { d = JSON.parse(payload); } catch { return; }
        if (d.usage) ctx.usage = usageFromChat(d.usage);
        if (d.error) { ctx.upstreamError = d.error.message || 'upstream error'; return; }
        const ch = d.choices && d.choices[0];
        if (!ch) return;
        const dl = ch.delta || {};
        if (dl.content) sink.delta({ content: dl.content });
        if (dl.reasoning_content) sink.delta({ reasoning_content: dl.reasoning_content });
        if (Array.isArray(dl.tool_calls)) {
            for (const tc of dl.tool_calls) {
                if (tc.id) sink.tool({ id: tc.id, name: tc.function?.name || '', upIndex: tc.index });
                if (tc.function && typeof tc.function.arguments === 'string' && tc.function.arguments) sink.tool({ argsDelta: tc.function.arguments, upIndex: tc.index });
            }
        }
        if (ch.finish_reason) fr = ch.finish_reason;
    };
    for (; ;) {
        const { done, value } = await reader.read(); if (done) break;
        if (value) ctx.up_res_bytes = (ctx.up_res_bytes || 0) + value.length;
        buf += dec.decode(value, { stream: true });
        let i;
        while ((i = buf.indexOf('\n')) >= 0) {
            const line = buf.slice(0, i).replace(/\r$/, ''); buf = buf.slice(i + 1);
            if (line.startsWith('data:')) handle(line.slice(5).trim());
            else if (line.charAt(0) === '{') handle(line.trim());   // 有的中转站吐裸 JSON 行
        }
    }
    if (buf.trim()) { const l = buf.replace(/\r$/, ''); if (l.startsWith('data:')) handle(l.slice(5).trim()); }
    ctx.finishReason = fr;
}

// ---- 请求翻译：客户端协议 → chat ----
function responsesToChatReq(body) {
    const msgs = [];
    const instr = typeof body.instructions === 'string' ? body.instructions
        : Array.isArray(body.instructions) ? body.instructions.map((p) => p?.text ?? '').join('\n') : '';
    if (instr) msgs.push({ role: 'system', content: instr });
    const input = body.input;
    const items = typeof input === 'string' ? [{ type: 'message', role: 'user', content: input }]
        : Array.isArray(input) ? input : [];
    let calls = [];
    const flush = () => { if (calls.length) { msgs.push({ role: 'assistant', content: null, tool_calls: calls }); calls = []; } };
    for (const it of items) {
        if (!it || typeof it !== 'object') continue;
        const t = it.type || (it.role ? 'message' : '');
        if (t === 'function_call') {
            calls.push({ id: it.call_id || it.id, type: 'function', function: { name: it.name || '', arguments: typeof it.arguments === 'string' ? it.arguments : jdump(it.arguments || {}) } });
            continue;
        }
        flush();
        if (t === 'function_call_output') {
            msgs.push({ role: 'tool', tool_call_id: it.call_id, content: typeof it.output === 'string' ? it.output : jdump(it.output ?? '') });
        } else if (t === 'reasoning') {
            // chat 装不下 Responses 的推理项（加密 blob），丢弃
        } else if (t === 'message' || it.role) {
            const role = (it.role === 'developer' || it.role === 'system') ? 'system' : (it.role || 'user');
            msgs.push({ role, content: textOf(it.content) });
        }
    }
    flush();
    const out = { model: body.model, messages: msgs, stream: !!body.stream };
    if (body.max_output_tokens != null) out.max_tokens = body.max_output_tokens;
    if (body.temperature != null) out.temperature = body.temperature;
    if (body.top_p != null) out.top_p = body.top_p;
    if (body.reasoning && typeof body.reasoning === 'object' && body.reasoning.effort != null) out.reasoning_effort = body.reasoning.effort;
    if (body.metadata != null) out.metadata = body.metadata;
    if (Array.isArray(body.tools) && body.tools.length) {
        out.tools = body.tools.filter((t) => t && t.type === 'function').map((t) => ({
            type: 'function',
            function: { name: t.name || t.function?.name, description: t.description ?? t.function?.description, parameters: t.parameters || t.function?.parameters || { type: 'object', properties: {} } },
        }));
        if (body.tool_choice != null) out.tool_choice = normToolChoice(body.tool_choice);
    }
    return out;
}
function messagesToChatReq(body) {
    const msgs = [];
    const sys = typeof body.system === 'string' ? body.system
        : Array.isArray(body.system) ? body.system.map((p) => p?.text ?? '').join('\n') : '';
    if (sys) msgs.push({ role: 'system', content: sys });
    for (const m of body.messages || []) {
        if (!m || typeof m !== 'object') continue;
        const role = m.role === 'assistant' ? 'assistant' : 'user';
        if (typeof m.content === 'string') { msgs.push({ role, content: m.content }); continue; }
        const parts = Array.isArray(m.content) ? m.content : [];
        const texts = [], thinking = [], toolCalls = [], toolResults = [];
        for (const p of parts) {
            if (!p || typeof p !== 'object') continue;
            if (p.type === 'text') texts.push(p.text || '');
            else if (p.type === 'thinking') thinking.push(p.thinking || '');
            else if (p.type === 'tool_use') toolCalls.push({ id: p.id, type: 'function', function: { name: p.name || '', arguments: typeof p.input === 'string' ? p.input : jdump(p.input || {}) } });
            else if (p.type === 'tool_result') toolResults.push({ id: p.tool_use_id, content: textOf(p.content) });
        }
        if (toolResults.length) {              // Anthropic 把 tool_result 放在 user 消息里 → chat 要拆成独立 role:tool
            for (const tr of toolResults) msgs.push({ role: 'tool', tool_call_id: tr.id, content: tr.content });
            if (texts.length) msgs.push({ role, content: texts.join('') });
            continue;
        }
        if (toolCalls.length) {
            const msg = { role: 'assistant', content: texts.join('') || null, tool_calls: toolCalls };
            if (thinking.length) msg.reasoning_content = thinking.join('\n');
            msgs.push(msg);
            continue;
        }
        if (thinking.length && role === 'assistant') { msgs.push({ role: 'assistant', content: texts.join('') || null, reasoning_content: thinking.join('\n') }); continue; }
        msgs.push({ role, content: texts.join('') });
    }
    const out = { model: body.model, messages: msgs, stream: !!body.stream };
    if (body.max_tokens != null) out.max_tokens = body.max_tokens;
    if (body.temperature != null) out.temperature = body.temperature;
    if (body.top_p != null) out.top_p = body.top_p;
    if (body.stop_sequences != null) out.stop = body.stop_sequences;
    if (Array.isArray(body.tools) && body.tools.length) {
        out.tools = body.tools.filter((t) => t && (t.name || t.function)).map((t) => ({
            type: 'function',
            function: { name: t.name || t.function?.name, description: t.description ?? t.function?.description, parameters: t.input_schema || t.function?.parameters || { type: 'object', properties: {} } },
        }));
        if (body.tool_choice != null) out.tool_choice = normToolChoice(body.tool_choice, 'anthropic');
    }
    return out;
}

// ---- 响应渲染：chat JSON → 客户端协议 JSON（非流式）----
function chatJsonToResponses(chat, ctx) {
    const msg = (chat.choices && chat.choices[0] && chat.choices[0].message) || {};
    const output = [];
    if (msg.reasoning_content) output.push({ id: randId('rs_'), type: 'reasoning', summary: [{ type: 'summary_text', text: String(msg.reasoning_content) }] });
    if (msg.content) output.push({ id: randId('msg_'), type: 'message', status: 'completed', role: 'assistant', content: [{ type: 'output_text', text: String(msg.content), annotations: [] }] });
    for (const tc of msg.tool_calls || []) output.push({ id: 'fc_' + crypto.randomBytes(12).toString('hex'), type: 'function_call', status: 'completed', call_id: tc.id, name: tc.function?.name || '', arguments: tc.function?.arguments || '{}' });
    return {
        id: 'resp_' + ctx.id.replace(/^chatcmpl-/, ''), object: 'response', created_at: ctx.created, status: 'completed', model: ctx.model,
        output, output_text: String(msg.content || ''), usage: usageToResponses(ctx.usage),
    };
}
function chatJsonToMessages(chat, ctx) {
    const msg = (chat.choices && chat.choices[0] && chat.choices[0].message) || {};
    const content = [];
    if (msg.reasoning_content) content.push({ type: 'thinking', thinking: String(msg.reasoning_content) });
    if (msg.content) content.push({ type: 'text', text: String(msg.content) });
    for (const tc of msg.tool_calls || []) content.push({ type: 'tool_use', id: tc.id, name: tc.function?.name || '', input: safeJson(tc.function?.arguments) });
    const fr = chat.choices && chat.choices[0] && chat.choices[0].finish_reason;
    return {
        id: 'msg_' + ctx.id.replace(/^chatcmpl-/, ''), type: 'message', role: 'assistant', model: ctx.model, content,
        stop_reason: fr === 'tool_calls' ? 'tool_use' : fr === 'length' ? 'max_tokens' : 'end_turn', stop_sequence: null,
        usage: { input_tokens: ctx.usage.in || 0, output_tokens: ctx.usage.out || 0, cache_read_input_tokens: ctx.usage.cached || 0 },
    };
}
function renderJson(res, clientProto, chat, ctx) {
    if (clientProto === 'chat') return sendJson(res, 200, chat);
    if (clientProto === 'responses') return sendJson(res, 200, chatJsonToResponses(chat, ctx));
    return sendJson(res, 200, chatJsonToMessages(chat, ctx));
}

// ---- 统一执行：候选协议探测 → 上游 → 按客户端协议渲染 ----
async function serveChatPipeline(req, res, opts) {
    const { auth, rt, t0, clientProto } = opts;
    const body = opts.body;
    const chatBody = clientProto === 'chat' ? body
        : clientProto === 'responses' ? responsesToChatReq(body)
            : messagesToChatReq(body);
    const model = body.model, ip = clientIp(req);
    const base = { ts: t0, key: auth.name, model, stream: !!body.stream, ip, cproto: clientProto, conv: convFingerprint(chatBody), req_bytes: Buffer.byteLength(opts.rawBody || ''), res_bytes: 0 };
    countOut(res, base);
    capture('client_req', { cproto: clientProto, ip, key: auth.name, model, stream: !!body.stream, body });
    const ctx = { id: 'chatcmpl-' + crypto.randomBytes(10).toString('hex'), created: Math.floor(t0 / 1000), model, t0, provider: rt.provider, usage: {}, hasTool: false, ttft: null, reasonPending: [], reasonInj: 0 };
    const sink = clientProto === 'chat' ? chatSink(res, ctx) : clientProto === 'responses' ? responsesSink(res, ctx) : messagesSink(res, ctx);
    let status = 200, format = resolveFormat(rt), piped = false;
    const hold = startFirstByteHold(res, !!body.stream);
    try {
        const ac = new AbortController();
        const onClose = () => { if (!res.writableEnded) { try { ac.abort(); } catch { } } };
        req.on('close', onClose);
        let upRes, lastTxt = '';
        for (const cand of protocolCandidates(rt)) {
            format = cand;
            const { url, payload, headers } = buildUpstream(rt, format, chatBody, auth.session_id, ctx);
            capture('upstream_req', { candidate: cand, cproto: clientProto, url, key: auth.name, model, upstream_model: rt.upstream_model, payload });
            const payloadStr = jdump(payload);
            ctx.up_len = payloadStr.length;
            ctx.pre4k = preHash(payloadStr, 4096); ctx.pre16k = preHash(payloadStr, 16384); ctx.pre64k = preHash(payloadStr, 65536);
            upRes = await fetch(url, { method: 'POST', headers, body: payloadStr, signal: ac.signal });
            capture('upstream_status', { candidate: cand, status: upRes.status, ok: upRes.ok });
            if (upRes.ok) break;
            lastTxt = await upRes.text(); status = upRes.status;
            const auto = !rt.format || rt.format === 'auto';
            if (auto && protocolMismatch(status, lastTxt)) { log('PROTOCOL SWITCH', rt.provider, rt.upstream_model, `${format} 不支持, 试下一个`, `(${status})`); continue; }
            break;
        }
        req.off('close', onClose);
        if (upRes.ok) { const fk = rt.provider + '|' + rt.upstream_model; if ((!rt.format || rt.format === 'auto') && FORMAT_OVERRIDES[fk] !== format) { FORMAT_OVERRIDES[fk] = format; saveOverrides(); } }
        if (!upRes.ok) {
            let parsed = null; try { parsed = JSON.parse(lastTxt); } catch { }
            log('UPSTREAM ERROR', status, rt.provider, format, model, lastTxt.slice(0, 300));
            if (hold.sent) {   // 保活已提前发出 200 响应头 → 改发 SSE 错误事件收尾
                record({ ...base, status, ms: Date.now() - t0, provider: rt.provider, upstream_model: rt.upstream_model, format, error: lastTxt.slice(0, 500) });
                log('HOLD-ERROR', `上游 ${status} 且已提前开流，改发 SSE 错误事件`);
                sink.error(parsed?.error?.message || lastTxt.slice(0, 800) || 'upstream error');
                sink.finalize('stop', {});
                return res.end();
            }
            record({ ...base, status, ms: Date.now() - t0, provider: rt.provider, upstream_model: rt.upstream_model, format, error: lastTxt.slice(0, 500) });
            return sendJson(res, status, parsed?.error ? { error: parsed.error } : { error: { message: lastTxt.slice(0, 800) || 'upstream error', type: 'upstream_error' } });
        }
        if (body.stream) hold.open();
        if (format === 'responses') {
            if (body.stream) { if (!res.headersSent) res.writeHead(200, SSE_HEADERS); await pumpResponses(upRes, ctx, sink); }
            else { const d = await upRes.json(); renderJson(res, clientProto, responsesToChatJson(d, ctx), ctx); }
        } else if (format === 'messages') {
            if (body.stream) { if (!res.headersSent) res.writeHead(200, SSE_HEADERS); await pumpMessages(upRes, ctx, sink); }
            else { const d = await upRes.json(); renderJson(res, clientProto, messagesToChatJson(d, ctx), ctx); }
        } else if (format === 'gemini') {
            if (body.stream) { if (!res.headersSent) res.writeHead(200, SSE_HEADERS); await pumpGemini(upRes, ctx, sink); }
            else { const d = await upRes.json(); ctx.up_res_bytes = Buffer.byteLength(jdump(d)); renderJson(res, clientProto, geminiToChatJson(d, ctx), ctx); }
        } else if (body.stream) {
            if (!res.headersSent) res.writeHead(200, SSE_HEADERS);
            if (clientProto === 'chat') {                 // chat→chat：原样字节透传（保持既有行为）
                const reader = upRes.body.getReader(); const dec = new TextDecoder(); let buf = '', ttft = 0;
                for (; ;) {
                    const { done, value } = await reader.read(); if (done) break;
                    if (value) ctx.up_res_bytes = (ctx.up_res_bytes || 0) + value.length;
                    if (!ttft) ttft = Date.now() - t0;
                    const text = dec.decode(value, { stream: true });
                    buf += text;
                    for (const mm of [...buf.matchAll(/data:\s*(\{.*?\})\s*\n/g)]) { try { const j = JSON.parse(mm[1]); if (j.usage) ctx.usage = usageFromChat(j.usage); if (j.choices?.[0]?.delta?.tool_calls) ctx.hasTool = true; } catch { } }
                    res.write(value);
                }
                res.end(); ctx.ttft = ttft; piped = true;
            } else {
                await pumpChat(upRes, ctx, sink);
            }
        } else {
            const d = await upRes.json();
            ctx.usage = usageFromChat(d.usage);
            ctx.up_res_bytes = Buffer.byteLength(jdump(d));
            renderJson(res, clientProto, d, ctx);
        }
        if (body.stream && !piped) {
            if (ctx.upstreamError && ctx.ttft == null) { log('UPSTREAM STREAM ERROR', ctx.upstreamError); sink.error(ctx.upstreamError); }
            sink.finalize(finishOf(ctx), ctx.usage);
            if (!res.writableEnded) res.end();
        }
        record({ ...base, status, ms: Date.now() - t0, provider: rt.provider, upstream_model: rt.upstream_model, format, usage: ctx.usage, ttft_ms: ctx.ttft || 0, has_tool: ctx.hasTool, reason_inj: ctx.reasonInj || 0, up_len: ctx.up_len, up_res_bytes: ctx.up_res_bytes || 0, pre4k: ctx.pre4k, pre16k: ctx.pre16k, pre64k: ctx.pre64k });
        log('OK', clientProto + '->' + format, rt.upstream_model, `in=${ctx.usage.in || 0} out=${ctx.usage.out || 0} cached=${ctx.usage.cached || 0}`, `${Date.now() - t0}ms`);
    } catch (e) {
        hold.stop();
        if (e.name === 'AbortError') {
            log('CLIENT ABORT', model, `${Date.now() - t0}ms`);
            record({ ...base, status: 499, ms: Date.now() - t0, provider: rt.provider, error: 'client aborted' });
            try { res.end(); } catch { }
            return;
        }
        log('GATEWAY ERROR', e.message);
        record({ ...base, status: 500, ms: Date.now() - t0, provider: rt.provider, error: e.message });
        if (!res.headersSent) sendJson(res, 500, { error: { message: e.message, type: 'gateway_error' } });
        else try { res.end(); } catch { }
    }
}

// ---------------- 拉取上游模型列表 ----------------
async function fetchProviderModels(b) {
    const prefix = String(b.prefix || '').trim().replace(/\/+$/, '');
    if (!prefix) throw new Error('缺少前缀');
    const route = (CFG.routes || []).find((r) => r.strip === prefix + '/');
    if (!route) throw new Error(`没有前缀为 ${prefix} 的供应商`);
    const prov = (CFG.providers || {})[route.provider];
    if (!prov) throw new Error('供应商配置缺失');
    const key = process.env[prov.api_key_env] || '';
    const base = String(prov.base_url || '').replace(/\/+$/, '');
    const extra = {};
    if (prov.user_agent) extra['User-Agent'] = prov.user_agent;
    for (const [k, v] of Object.entries(prov.extra_headers || {})) extra[k] = v;

    const attempt = async (mode) => {
        const headers = { 'Accept': 'application/json', ...extra };
        if (mode === 'bearer') headers['Authorization'] = 'Bearer ' + key;
        else headers['x-api-key'] = key;
        const r = await fetch(base + '/models', { headers, signal: AbortSignal.timeout(20000) });
        const txt = await r.text();
        if (!r.ok) return { ok: false, status: r.status, text: txt.slice(0, 200) };
        let j;
        try { j = JSON.parse(txt); } catch { return { ok: false, status: r.status, text: '返回不是 JSON' }; }
        const arr = Array.isArray(j) ? j : (j.data || j.models || j.result || j.items || []);
        const ids = (Array.isArray(arr) ? arr : []).map((x) => typeof x === 'string' ? x : (x.id || x.name || x.model)).filter(Boolean);
        return { ok: true, ids };
    };

    let res = await attempt('bearer');
    if (!res.ok && [401, 403].includes(res.status)) res = await attempt('x-api-key');
    if (!res.ok) throw new Error(`上游 /models 拉取失败（HTTP ${res.status}）：${res.text}`);
    const have = new Set((CFG.models || []).filter((m) => m.startsWith(prefix + '/')));
    return {
        provider: route.provider, prefix, endpoint: base + '/models', count: res.ids.length,
        models: res.ids.map((id) => ({ id, added: have.has(prefix + '/' + id) })),
    };
}

// ---------------- 配置视图与变更（面板用） ----------------
// 「测试供应商」标记：config 里显式写了 test 就听它的；没写则按 base_url 是否指向本机推断
// （与看板「参与统计」的默认口径一致：本机上游只用于 mock / 测试）
function isLocalBaseUrl(url) {
    const h = String(url || '').replace(/^https?:\/\//i, '').split('/')[0].replace(/:\d+$/, '');
    return h === '127.0.0.1' || h === 'localhost' || h === '::1' || h === '[::1]';
}
function providerIsTest(p) {
    if (p.test === true) return true;
    if (p.test === false) return false;
    return isLocalBaseUrl(p.base_url);
}
function effortInfo(id) {
    const rt = routeFor(String(id || ''));
    if (!rt) return { effective: null, manual: null, manual_provider: null, probed: null, at: '', note: '', rejected: {} };
    const k = effortKeyOf(rt.provider, rt.upstream_model);
    const probe = (CFG.effort_probes || {})[k] || {};
    const direct = normEfforts((CFG.model_efforts || {})[k]);
    const wide = normEfforts((CFG.model_efforts || {})[effortKeyOf(rt.provider, '*')]);
    const probed = normEfforts(probe.levels);
    return {
        effective: effortsFor(rt.provider, rt.upstream_model),
        manual: direct.length ? direct : null,
        manual_provider: wide.length ? wide : null,
        probed: probed.length ? probed : null,
        at: probe.at || '', note: probe.note || '', rejected: probe.rejected || {},
    };
}
function configView() {
    const providers = Object.entries(CFG.providers || {}).map(([name, p]) => {
        const route = (CFG.routes || []).find((r) => r.provider === name && r.strip);
        return {
            name,
            base_url: p.base_url,
            api_key_env: p.api_key_env,
            has_key: !!(process.env[p.api_key_env] || '').length,
            user_agent: !!p.user_agent,
            reasoning_echo: !!p.reasoning_echo,
            extra_headers: Object.keys(p.extra_headers || {}),
            prefix: route ? route.strip.replace(/\/$/, '') : '',
            models: (CFG.models || []).filter((m) => route && m.startsWith(route.strip)).length,
            test: providerIsTest(p),                       // 是否测试供应商（显式标记优先，否则按 base_url 推断）
            test_explicit: p.test !== undefined,           // 是否已显式写进 config.json
            efforts: normEfforts((CFG.model_efforts || {})[effortKeyOf(name, '*')]),   // 该供应商的默认推理档位（模型级没配时生效）
        };
    });
    const MT = {};                                            // 每个模型的累计量（模型列表页显示用）
    for (const x of DB.prepare('SELECT model, SUM(requests) n, SUM(in_tok) i, SUM(out_tok) o, SUM(cached) c FROM rollup_hour GROUP BY model').all())
        MT[x.model] = { requests: x.n, in: x.i || 0, out: x.o || 0, cached: x.c || 0 };
    const models = (CFG.models || []).map((id) => {
        const rt = routeFor(id);
        const st = MT[id] || {};
        return {
            id,
            provider: rt ? rt.provider : null,
            upstream: rt ? rt.upstream_model : null,
            format: rt ? resolveFormat(rt) : null,
            auto: rt ? !!FORMAT_OVERRIDES[rt.provider + '|' + rt.upstream_model] : false,
            configured: rt ? !!(CFG.model_formats || {})[rt.provider + '|' + rt.upstream_model] : false,
            requests: st.requests || 0, in: st.in || 0, out: st.out || 0, cached: st.cached || 0,
            effort: effortInfo(id),                    // 推理档位：生效/手工/探测/拒绝原因
        };
    });
    return {
        enabled: isEnabled(),
        providers,
        routes: (CFG.routes || []).map((r) => ({ model_regex: r.model_regex, provider: r.provider, strip: r.strip || '', format: r.format || 'auto' })),
        models,
        keys: (CFG.keys || []).map((k) => ({ name: k.name, protocol: k.protocol || '', interop: !!k.protocol_interop, models: Array.isArray(k.models) ? k.models : [] })),
        formats: Object.keys(CFG.model_formats || {}).length,
        listen: CFG.listen,
        dashboard_hosts: (CFG.dashboard_auth || {}).hosts || [],
    };
}
// 客户端 key 的模型白名单校验：去空、去重、必须存在于对外模型清单。
// 返回 [] 表示「全部模型」（等于不限制）；传 null/undefined 也当 []。
function normKeyModels(input) {
    if (input === null || input === undefined) return [];
    if (!Array.isArray(input)) throw new Error('models 必须是模型名数组（空数组＝全部模型）');
    const all = new Set(CFG.models || []);
    const out = [];
    for (const raw of input) {
        const m = String(raw || '').trim();
        if (!m) continue;
        if (!all.has(m)) throw new Error('模型不存在: ' + m);
        if (!out.includes(m)) out.push(m);
    }
    return out;
}
function applyConfigChange(p, b) {
    const cfg = JSON.parse(fs.readFileSync(CONFIG_PATH, 'utf8'));
    const prefixRe = (pf) => '^' + pf + '/';
    if (p === '/api/config/provider') {
        const name = String(b.name || '').trim();
        if (!/^[a-z0-9][a-z0-9_-]{1,30}$/i.test(name)) throw new Error('供应商名只能字母数字-_（2–31 字符）');
        if (!/^https?:\/\//.test(String(b.base_url || ''))) throw new Error('base_url 必须以 http:// 或 https:// 开头');
        const envName = String(b.api_key_env || (name.toUpperCase().replace(/[^A-Z0-9]/g, '_') + '_API_KEY')).trim();
        cfg.providers = cfg.providers || {};
        cfg.providers[name] = {
            base_url: String(b.base_url).replace(/\/+$/, ''),
            api_key_env: envName,
            ...(b.user_agent ? { user_agent: String(b.user_agent) } : {}),
            ...(b.extra_headers && typeof b.extra_headers === 'object' ? { extra_headers: b.extra_headers } : {}),
            ...(b.reasoning_echo ? { reasoning_echo: true } : {}),
            ...(b.test !== undefined ? { test: !!b.test } : {}),
        };
        if (b.api_key) setEnvVar(envName, String(b.api_key).trim());
        if (b.efforts !== undefined || b.reasoning_efforts !== undefined) {   // 供应商级默认推理档位（可选）
            const lv = parseEffortsInput(b.efforts !== undefined ? b.efforts : b.reasoning_efforts);
            if (lv.length) { cfg.model_efforts = cfg.model_efforts || {}; cfg.model_efforts[effortKeyOf(name, '*')] = lv; }
        }
        const prefix = String(b.prefix || name).trim().replace(/\/+$/, '');
        cfg.routes = cfg.routes || [];
        if (!cfg.routes.some((r) => r.model_regex === prefixRe(prefix))) {
            cfg.routes.unshift({ model_regex: prefixRe(prefix), provider: name, strip: prefix + '/', format: 'auto', responses_regex: '^(gpt-|grok-)' });
        }
        saveConfig(cfg); reloadConfig();
        return { provider: name, prefix, api_key_env: envName };
    }
    if (p === '/api/config/provider/update') {          // 编辑已有供应商的属性（不改名/前缀，改名＝删除后重建）
        const name = String(b.name || '').trim();
        const prov = (cfg.providers || {})[name];
        if (!prov) throw new Error('供应商不存在: ' + name);
        const changed = [];
        if (b.base_url !== undefined) {
            const url = String(b.base_url).trim();
            if (!/^https?:\/\//.test(url)) throw new Error('base_url 必须以 http:// 或 https:// 开头');
            const clean = url.replace(/\/+$/, '');
            if (clean !== prov.base_url) { prov.base_url = clean; changed.push('base_url'); }
        }
        if (b.user_agent !== undefined) {
            const ua = String(b.user_agent).trim();
            if (ua) { if (ua !== prov.user_agent) { prov.user_agent = ua; changed.push('user_agent'); } }
            else if (prov.user_agent) { delete prov.user_agent; changed.push('user_agent(已清除)'); }
        }
        if (b.extra_headers !== undefined) {
            const cur = prov.extra_headers || {};
            const next = (b.extra_headers && typeof b.extra_headers === 'object') ? b.extra_headers : {};
            const same = jdump(cur) === jdump(next);
            if (!same) {
                if (Object.keys(next).length) { prov.extra_headers = next; changed.push('extra_headers'); }
                else { delete prov.extra_headers; changed.push('extra_headers(已清除)'); }
            }
        }
        if (b.api_key_env !== undefined) {
            const env = String(b.api_key_env).trim();
            if (!/^[A-Za-z][A-Za-z0-9_]*$/.test(env)) throw new Error('key 环境变量名只能是字母数字下划线');
            if (env !== prov.api_key_env) {
                const old = prov.api_key_env;
                const value = process.env[old] || '';
                const stillUsed = Object.entries(cfg.providers || {}).some(([n, pr]) => n !== name && pr && pr.api_key_env === old);
                prov.api_key_env = env;
                if (value && !stillUsed) { setEnvVar(env, value); try { removeEnvVar(old); } catch { } }
                changed.push('api_key_env');
            }
        }
        if (b.reasoning_echo !== undefined) {
            if (b.reasoning_echo) { if (!prov.reasoning_echo) { prov.reasoning_echo = true; changed.push('reasoning_echo(开)'); } }
            else if (prov.reasoning_echo) { delete prov.reasoning_echo; changed.push('reasoning_echo(关)'); }
        }
        if (b.efforts !== undefined || b.reasoning_efforts !== undefined) {      // 供应商级默认推理档位（该供应商所有模型，模型级可覆盖）
            const lv = parseEffortsInput(b.efforts !== undefined ? b.efforts : b.reasoning_efforts);
            cfg.model_efforts = cfg.model_efforts || {};
            const ek = effortKeyOf(name, '*');
            const prev = normEfforts(cfg.model_efforts[ek]);
            if (lv.length) {
                cfg.model_efforts[ek] = lv;
                if (jdump(prev) !== jdump(lv)) changed.push('推理档位 ' + lv.join('/'));
            } else if (prev.length) { delete cfg.model_efforts[ek]; changed.push('推理档位(已清除)'); }
        }
        if (b.test !== undefined) {
            const want = !!b.test;
            if (prov.test !== want) { prov.test = want; changed.push(want ? 'test(标记为测试)' : 'test(标记为正式)'); }
        }
        if (b.api_key) { setEnvVar(prov.api_key_env, String(b.api_key).trim()); changed.push('key'); }
        saveConfig(cfg); reloadConfig();
        log('CONFIG update provider', name, changed.join(',') || '(无变化)');
        return { provider: name, updated: true, changed };
    }
    if (p === '/api/config/model') {            // 加模型 / 覆盖已有模型：幂等 upsert，已有就按新配置替换
        const id = String(b.id || '').trim();
        const prefix = String(b.prefix || '').trim().replace(/\/+$/, '');
        if (!id) throw new Error('模型 ID 不能为空');
        if (!prefix) throw new Error('缺少 prefix（用于路由）');
        const rt = (cfg.routes || []).find((r) => r.model_regex === prefixRe(prefix));
        if (!rt) throw new Error(`没有 prefix 为 ${prefix} 的路由，请先添加供应商`);
        const full = prefix + '/' + id;
        cfg.models = cfg.models || [];
        const existed = cfg.models.includes(full);
        if (!existed) cfg.models.push(full);
        const fmtKey = rt.provider + '|' + id;
        const want = ['chat', 'responses', 'messages', 'gemini'].includes(b.format) ? b.format : '';
        cfg.model_formats = cfg.model_formats || {};
        const prev = cfg.model_formats[fmtKey] || '';
        if (want) cfg.model_formats[fmtKey] = want; else delete cfg.model_formats[fmtKey];
        if (b.efforts !== undefined || b.reasoning_efforts !== undefined) {   // 顺手指定该模型的推理档位（留空＝清除）
            const lv = parseEffortsInput(b.efforts !== undefined ? b.efforts : b.reasoning_efforts);
            cfg.model_efforts = cfg.model_efforts || {};
            if (lv.length) cfg.model_efforts[fmtKey] = lv; else delete cfg.model_efforts[fmtKey];
        }
        // 显式配置必须压过「首请求探测」的结果（resolveFormat 里探测优先级更高），否则改不动已有模型
        const hadProbe = !!FORMAT_OVERRIDES[fmtKey];
        if (hadProbe) { delete FORMAT_OVERRIDES[fmtKey]; saveOverrides(); }
        saveConfig(cfg); reloadConfig();
        log('CONFIG model upsert', full, existed ? `覆盖（协议 ${prev || 'auto'} → ${want || 'auto'}）` : '新增');
        return {
            model: full, provider: rt.provider, existed,
            added: existed ? 0 : 1, updated: existed ? 1 : 0,
            format: want || 'auto', prev_format: prev || 'auto', cleared_probe: hadProbe,
        };
    }
    if (p === '/api/config/models/bulk') {
        const prefix = String(b.prefix || '').trim().replace(/\/+$/, '');
        const ids = Array.isArray(b.ids) ? b.ids.map((x) => String(x).trim()).filter(Boolean) : [];
        if (!prefix) throw new Error('缺少前缀');
        if (!ids.length) throw new Error('没有勾选任何模型');
        const rt = (cfg.routes || []).find((r) => r.model_regex === prefixRe(prefix));
        if (!rt) throw new Error(`没有 prefix 为 ${prefix} 的路由，请先添加供应商`);
        cfg.models = cfg.models || [];
        const want = ['chat', 'responses', 'messages', 'gemini'].includes(b.format) ? b.format : '';
        const effortsProvided = b.efforts !== undefined || b.reasoning_efforts !== undefined;
        const effortsParsed = effortsProvided ? parseEffortsInput(b.efforts !== undefined ? b.efforts : b.reasoning_efforts) : [];
        let added = 0, updated = 0, clearedProbe = 0;
        for (const id of ids) {
            const full = prefix + '/' + id;
            if (!cfg.models.includes(full)) { cfg.models.push(full); added++; } else updated++;
            const fmtKey = rt.provider + '|' + id;
            cfg.model_formats = cfg.model_formats || {};
            if (want) cfg.model_formats[fmtKey] = want; else delete cfg.model_formats[fmtKey];
            if (FORMAT_OVERRIDES[fmtKey]) { delete FORMAT_OVERRIDES[fmtKey]; clearedProbe++; }
            if (effortsProvided) {                       // 批量指定同一档位（留空＝清除）
                cfg.model_efforts = cfg.model_efforts || {};
                if (effortsParsed.length) cfg.model_efforts[fmtKey] = effortsParsed; else delete cfg.model_efforts[fmtKey];
            }
        }
        if (clearedProbe) saveOverrides();
        saveConfig(cfg); reloadConfig();
        log('CONFIG models bulk', prefix, `新增 ${added} / 覆盖 ${updated} / 清探测 ${clearedProbe}`);
        return { added, updated, skipped: updated, cleared_probe: clearedProbe, total: cfg.models.length, prefix };
    }
    if (p === '/api/config/model/efforts') {           // 手工指定某模型的推理档位（留空＝清除，回到探测/透传）
        const id = String(b.id || '').trim();
        const rt = routeFor(id);
        if (!rt) throw new Error('模型未路由: ' + id);
        const lv = parseEffortsInput(b.levels !== undefined ? b.levels : b.efforts);
        cfg.model_efforts = cfg.model_efforts || {};
        const ek = effortKeyOf(rt.provider, rt.upstream_model);
        if (lv.length) cfg.model_efforts[ek] = lv; else delete cfg.model_efforts[ek];
        saveConfig(cfg); reloadConfig();
        log('CONFIG model efforts', id, lv.length ? lv.join('/') : '清除');
        return { id, provider: rt.provider, upstream_model: rt.upstream_model, efforts: lv.length ? lv : null };
    }
    if (p === '/api/config/provider/efforts') {        // 供应商级默认推理档位（该供应商所有模型，模型级可覆盖）
        const name = String(b.name || '').trim();
        if (!(cfg.providers || {})[name]) throw new Error('供应商不存在: ' + name);
        const lv = parseEffortsInput(b.levels !== undefined ? b.levels : b.efforts);
        cfg.model_efforts = cfg.model_efforts || {};
        const ek = effortKeyOf(name, '*');
        if (lv.length) cfg.model_efforts[ek] = lv; else delete cfg.model_efforts[ek];
        saveConfig(cfg); reloadConfig();
        log('CONFIG provider efforts', name, lv.length ? lv.join('/') : '清除');
        return { provider: name, efforts: lv.length ? lv : null };
    }
    if (p === '/api/config/provider/delete') {
        const name = String(b.name || '').trim();
        if (!cfg.providers || !cfg.providers[name]) throw new Error('供应商不存在: ' + name);
        const routes = (cfg.routes || []).filter((r) => r.provider === name);
        const prefixes = routes.map((r) => r.strip || '').filter(Boolean);
        const before = (cfg.models || []).length;
        cfg.models = (cfg.models || []).filter((m) => !prefixes.some((pf) => m.startsWith(pf)));
        const removedModels = before - cfg.models.length;
        cfg.routes = (cfg.routes || []).filter((r) => r.provider !== name);
        const mf = cfg.model_formats || {};
        let removedFmt = 0;
        for (const k of Object.keys(mf)) if (k.startsWith(name + '|')) { delete mf[k]; removedFmt++; }
        let removedEfforts = 0;                        // 档位：手工指定 + 探测缓存，都按 provider| 前缀清掉
        for (const map of [cfg.model_efforts || {}, cfg.effort_probes || {}]) {
            for (const k of Object.keys(map)) if (k.startsWith(name + '|')) { delete map[k]; removedEfforts++; }
        }
        const envName = cfg.providers[name].api_key_env;
        delete cfg.providers[name];
        const stillUsed = Object.values(cfg.providers).some((pr) => pr && pr.api_key_env === envName);
        let envRemoved = false;
        if (envName && !stillUsed) { try { removeEnvVar(envName); envRemoved = true; } catch { } }
        saveConfig(cfg); reloadConfig();
        return { removed_provider: name, removed_models: removedModels, removed_routes: routes.length, removed_formats: removedFmt, removed_efforts: removedEfforts, env_removed: envRemoved };
    }
    if (p === '/api/config/model/delete') {
        const id = String(b.id || '').trim();
        const rt = routeFor(id);
        cfg.models = (cfg.models || []).filter((m) => m !== id);
        if (rt) {                                      // 顺带清掉这个模型的档位配置与探测缓存
            const ek = effortKeyOf(rt.provider, rt.upstream_model);
            if (cfg.model_efforts) delete cfg.model_efforts[ek];
            if (cfg.effort_probes) delete cfg.effort_probes[ek];
        }
        saveConfig(cfg); reloadConfig();
        return { removed: id };
    }
    if (p === '/api/config/key') {
        const name = String(b.name || '').trim();
        if (!/^[a-z0-9][a-z0-9_-]{0,30}$/i.test(name)) throw new Error('key 名只能字母数字-_（≤31 字符）');
        if ((cfg.keys || []).some((k) => k.name === name)) throw new Error('同名 key 已存在');
        const key = 'sk-gw-' + crypto.randomBytes(18).toString('base64url');
        const models = normKeyModels(b.models);              // 空＝全部模型（默认）
        cfg.keys = cfg.keys || [];
        cfg.keys.push({ name, key, session_id: crypto.randomUUID(), ...(models.length ? { models } : {}) });
        saveConfig(cfg); reloadConfig();
        return { name, key, models };
    }
    if (p === '/api/config/key/models') {              // 该 key 可访问的模型白名单：空数组/省略＝全部（默认）
        const name = String(b.name || '').trim();
        const k = (cfg.keys || []).find((x) => x.name === name);
        if (!k) throw new Error('key 不存在: ' + name);
        const models = normKeyModels(b.models);
        if (models.length) k.models = models; else delete k.models;
        saveConfig(cfg); reloadConfig();
        log('CONFIG key models', name, models.length ? `${models.length} 个` : '全部');
        return { key: name, models, config: configView() };
    }
    if (p === '/api/config/key/delete') {
        const name = String(b.name || '').trim();
        if (!(cfg.keys || []).some((k) => k.name === name)) throw new Error('key 不存在: ' + name);
        const rest = (cfg.keys || []).filter((k) => k.name !== name);
        if (!rest.length) throw new Error('这是最后一个 key，删掉就没人能用了');
        cfg.keys = rest;
        saveConfig(cfg); reloadConfig();
        return { removed_key: name, left: rest.length };
    }
    if (p === '/api/config/key/protocol') {           // 给客户端 key 绑定协议：/v1/models 据此只回该协议的模型（留空＝自动）
        const name = String(b.name || '').trim();
        const k = (cfg.keys || []).find((x) => x.name === name);
        if (!k) throw new Error('key 不存在: ' + name);
        const proto = String(b.protocol || '').toLowerCase();
        if (proto && !['chat', 'responses', 'messages'].includes(proto)) throw new Error('协议只能是 chat / responses / messages，或留空＝自动');
        if (proto) k.protocol = proto; else delete k.protocol;
        saveConfig(cfg); reloadConfig();
        log('CONFIG key protocol', name, proto || '(自动)');
        return { key: name, protocol: proto };
    }
    if (p === '/api/config/key/interop') {            // 协议互转（实验性，按 key，默认关）：允许该 key 的 responses/messages 客户端跨协议用其它协议的模型
        const name = String(b.name || '').trim();
        const k = (cfg.keys || []).find((x) => x.name === name);
        if (!k) throw new Error('key 不存在: ' + name);
        if (b.enabled) k.protocol_interop = true; else delete k.protocol_interop;
        saveConfig(cfg); reloadConfig();
        log('CONFIG key interop', name, b.enabled ? 'ON' : 'OFF');
        return { key: name, interop: !!k.protocol_interop };
    }
    if (p === '/api/config/toggle') {
        cfg.enabled = b.enabled !== false;
        saveConfig(cfg); reloadConfig();
        log('TOGGLE gateway', cfg.enabled ? 'ON' : 'OFF');
        return { enabled: cfg.enabled };
    }
    if (p === '/api/config/toggle_capture') {        // 抓包开关：热生效，不用重启
        cfg.capture = b.enabled !== false;
        saveConfig(cfg); reloadConfig();
        log('TOGGLE capture', cfg.capture ? 'ON' : 'OFF');
        return { capture: cfg.capture };
    }
    if (p === '/api/config/reload') { reloadConfig(); return { reloaded: true }; }
    throw new Error('未知操作: ' + p);
}

// ---------------- HTTP ----------------
// 每个 key 最近一次用到的协议（chat / responses / messages 都记）：
// /v1/models 在没显式绑定时据此判断——最近用 chat 就不过滤（chat 客户端能用全部模型），
// 最近只用某个原生协议（responses/messages）才过滤。切换 Pi 的 api 类型后会自动跟着变。
const KEY_PROTO = new Map();               // keyName -> { protocol, t }
const KEY_PROTO_TTL_MS = 24 * 60 * 60 * 1000;
function noteKeyProtocol(keyName, kind) {
    if (keyName && ['chat', 'responses', 'messages'].includes(kind)) KEY_PROTO.set(keyName, { protocol: kind, t: Date.now() });
}
function keyProtocolHint(keyName) {
    const hit = KEY_PROTO.get(keyName);
    if (!hit) return null;
    if (Date.now() - hit.t > KEY_PROTO_TTL_MS) { KEY_PROTO.delete(keyName); return null; }
    return hit.protocol;
}

// ---------------- 上游额度/用量 ----------------
// 上游额度：余额接口 / 用量接口 / 单价表全部从 config.json 读（返回形状按你上游的实际情况改这两个函数）。
// 单价单位＝每 1M tokens（USD）；cap＝该模型月上限（美元），没有上限填 null。
const GO_PRICES = (CFG.prices && typeof CFG.prices === 'object') ? CFG.prices : {};
const GO_PEAK = (CFG.peak_prices && typeof CFG.peak_prices === 'object') ? CFG.peak_prices : {};
function goUnit(model, atMs) {
    if (!model) return null;
    if (/-free$/.test(model)) return { in: 0, out: 0, cached: 0, cap: null };   // 官方标注 Unlimited 的免费档
    if (GO_PRICES[model]) return GO_PRICES[model];
    const pk = GO_PEAK[model];
    if (!pk) return null;
    const d = new Date(atMs || Date.now());
    const h = d.getUTCHours(), wd = d.getUTCDay();
    const peak = wd >= 1 && wd <= 5 && ((h >= 1 && h < 4) || (h >= 6 && h < 10));
    const [i, o, c] = peak ? pk.peak : pk.off;
    return { in: i, out: o, cached: c, cap: pk.cap };
}
function goModelCost(model, usage, atMs) {      // 非缓存输入按 (in - cached) 计，避免缓存部分被双算
    const p = goUnit(model, atMs);
    const u = usage || {};
    if (!p) return { cost: null, cap: null };
    const cached = Math.min(u.cached || 0, u.in || 0);
    const fresh = Math.max(0, (u.in || 0) - cached);
    return { cost: fresh / 1e6 * p.in + (u.out || 0) / 1e6 * p.out + cached / 1e6 * p.cached, cap: p.cap };
}
function computeGoUsage() {
    const now = Date.now();
    const h5Cut = now - 5 * 3600e3, w7Cut = now - 7 * 86400e3, d30Cut = now - 30 * 86400e3;
    const per = new Map(), total = { h5: 0, w7: 0, d30: 0, reqs: { h5: 0, w7: 0, d30: 0 } };
    // 只对 config.quota.provider_prefixes 里列的前缀计费（留空＝全部记录）；价格按峰谷时段逐条算
    const pfx = (CFG.quota || {}).provider_prefixes || [];
    const cond = pfx.length ? ' AND (' + pfx.map(() => '(provider LIKE ? OR model LIKE ?)').join(' OR ') + ')' : '';
    const rows = DB.prepare(`SELECT ts, model, provider, upstream_model, in_tok, out_tok, cached FROM requests
        WHERE ts >= ?${cond}`).all(d30Cut, ...pfx.flatMap((p) => [p + '%', p + '%']));
    for (const r of rows) {
        const ts = r.ts || 0;
        const model0 = String(r.model || '');
        const model = String(r.upstream_model || model0.replace(/^[^/]*\//, ''));
        if (!model) continue;
        const { cost, cap } = goModelCost(model, { in: r.in_tok || 0, out: r.out_tok || 0, cached: r.cached || 0 }, ts);
        let e = per.get(model);
        if (!e) { e = { model, h5: 0, w7: 0, d30: 0, cap, n5: 0, n7: 0, n30: 0, priced: true }; per.set(model, e); }
        if (cost == null) e.priced = false;
        else { e.d30 += cost; if (ts >= w7Cut) e.w7 += cost; if (ts >= h5Cut) e.h5 += cost; }
        e.n30++; if (ts >= w7Cut) e.n7++; if (ts >= h5Cut) e.n5++;
        if (cost != null) { total.d30 += cost; if (ts >= w7Cut) total.w7 += cost; if (ts >= h5Cut) total.h5 += cost; }
        total.reqs.d30++; if (ts >= w7Cut) total.reqs.w7++; if (ts >= h5Cut) total.reqs.h5++;
    }
    return { windows: total, models: [...per.values()].sort((a, b) => b.d30 - a.d30) };
}
async function upstreamUsage() {           // 上游用量接口（url / key 环境变量在 config.json 的 quota.usage 里配）
    const qc = (CFG.quota || {}).usage || {};
    if (!qc.url) return { ok: false, error: 'quota.usage.url 未配置' };
    const key = process.env[qc.api_key_env || ''] || '';
    if (!key) return { ok: false, error: `${qc.api_key_env || 'API key'} 未配置` };
    try {
        const r = await fetch(qc.url, {
            headers: {
                Authorization: 'Bearer ' + key, Accept: 'application/json',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            },
            signal: AbortSignal.timeout(15000),
        });
        const t = await r.text();
        if (!r.ok) return { ok: false, error: `HTTP ${r.status}: ${t.slice(0, 140)}` };
        const u = (JSON.parse(t).usage) || {};
        const pick = (x) => (x && typeof x === 'object') ? { percent: Number(x.percent || 0), status: x.status || '', resets_at: x.resetsAt || null } : null;
        return { ok: true, rolling: pick(u.rolling), weekly: pick(u.weekly), monthly: pick(u.monthly) };
    } catch (e) { return { ok: false, error: String((e && e.message) || e).slice(0, 160) }; }
}
async function upstreamBalance() {         // 上游余额接口（url / key 环境变量在 config.json 的 quota.balance 里配）
    const qc = (CFG.quota || {}).balance || {};
    if (!qc.url) return { ok: false, error: 'quota.balance.url 未配置' };
    const key = process.env[qc.api_key_env || ''] || '';
    if (!key) return { ok: false, error: `${qc.api_key_env || 'API key'} 未配置` };
    try {
        const r = await fetch(qc.url, {
            headers: { Authorization: 'Bearer ' + key, Accept: 'application/json' }, signal: AbortSignal.timeout(15000),
        });
        const t = await r.text();
        if (!r.ok) return { ok: false, error: `HTTP ${r.status}: ${t.slice(0, 140)}` };
        const d = JSON.parse(t), b = (d.balance_infos || [])[0] || {};
        return { ok: true, available: !!d.is_available, currency: b.currency || 'CNY', total: Number(b.total_balance || 0), granted: Number(b.granted_balance || 0), topped_up: Number(b.topped_up_balance || 0) };
    } catch (e) { return { ok: false, error: String((e && e.message) || e).slice(0, 160) }; }
}
// ---------------- 订阅周期（按月续费的额度窗口） ----------------
// 口径：周期＝以「下次续费时刻」为锚点往前推的日历月。锚点优先取 quota.usage 返回的 monthly.resets_at，
// 取不到就按记过的锚点顺推，再退回 quota.cycle_day（默认 1 号）。
// allowance 口径＝实际成本 × base_usd ÷ 该模型月上限，所以「归一化额度 ÷ base_usd」与上游面板百分比同口径。
const GO_BASE_USD = Number((CFG.quota && CFG.quota.base_usd) || 60) || 60;
const GO_CYCLE_COUNT = Number((CFG.quota && CFG.quota.cycle_count) || 6) || 6;
function monthShift(ms, n) {               // 日历月平移，月末溢出回退到目标月最后一天
    const d = new Date(ms), day = d.getUTCDate();
    d.setUTCDate(1);
    d.setUTCMonth(d.getUTCMonth() + n);
    const last = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth() + 1, 0)).getUTCDate();
    d.setUTCDate(Math.min(day, last));
    return d.getTime();
}
function usageCycleAnchor(quota) {         // 下次续费时刻（ms）；上游取不到就顺推上次记下的锚点
    const iso = quota && quota.ok && quota.monthly && quota.monthly.resets_at;
    const t = iso ? Date.parse(iso) : NaN;
    if (Number.isFinite(t)) {
        try { DB.prepare("INSERT OR REPLACE INTO meta(k,v) VALUES('quota_cycle_anchor',?)").run(String(t)); } catch { /* ignore */ }
        return t;
    }
    const row = DB.prepare("SELECT v FROM meta WHERE k='quota_cycle_anchor'").get();
    const prev = row ? Number(row.v) : NaN;
    if (Number.isFinite(prev) && prev > 0) { let a = prev; while (a <= Date.now()) a = monthShift(a, 1); return a; }
    const day = Math.min(28, Math.max(1, Number((CFG.quota || {}).cycle_day) || 1));   // 兜底：每月固定日
    const now = new Date(), y = now.getUTCFullYear(), m = now.getUTCMonth();
    const cand = Date.UTC(y, m, day);
    return cand > now.getTime() ? cand : Date.UTC(y, m + 1, day);
}
function computeUsageCycles(anchorMs, count = GO_CYCLE_COUNT) {
    const bounds = [];
    for (let i = 0; i < count; i++) bounds.push({ lo: monthShift(anchorMs, -i - 1), hi: monthShift(anchorMs, -i) });
    const oldest = bounds[bounds.length - 1].lo;
    const out = bounds.map((b) => ({ start: b.lo, end: b.hi, reqs: 0, in: 0, out: 0, cached: 0, cost: 0, allow: 0, priced: 0, unpriced: 0, models: new Map() }));
    const pfx = (CFG.quota || {}).provider_prefixes || [];
    const cond = pfx.length ? ' AND (' + pfx.map(() => '(provider LIKE ? OR model LIKE ?)').join(' OR ') + ')' : '';
    const rows = DB.prepare(`SELECT ts, model, provider, upstream_model, in_tok, out_tok, cached FROM requests
        WHERE ts >= ?${cond}`).all(oldest, ...pfx.flatMap((p) => [p + '%', p + '%']));
    for (const r of rows) {
        const ts = r.ts || 0;
        const e = out.find((c) => ts >= c.start && ts < c.end);
        if (!e) continue;
        const model = String(r.upstream_model || String(r.model || '').replace(/^[^/]*\//, ''));
        const { cost, cap } = goModelCost(model, { in: r.in_tok || 0, out: r.out_tok || 0, cached: r.cached || 0 }, ts);
        e.reqs++; e.in += r.in_tok || 0; e.out += r.out_tok || 0; e.cached += r.cached || 0;
        if (cost == null) { e.unpriced++; continue; }          // 价格表里没有的模型只计请求数
        e.priced++; e.cost += cost;
        const allow = cap ? cost * GO_BASE_USD / cap : 0;      // cap=null → 不限额度
        e.allow += allow;
        const m = e.models.get(model) || { model, reqs: 0, in: 0, out: 0, cached: 0, cost: 0, allow: 0, cap };
        m.reqs++; m.in += r.in_tok || 0; m.out += r.out_tok || 0; m.cached += r.cached || 0;
        m.cost += cost; m.allow += allow; e.models.set(model, m);
    }
    return out.map((c) => ({
        start: c.start, end: c.end, reqs: c.reqs, in: c.in, out: c.out, cached: c.cached,
        cost: +c.cost.toFixed(4), allow: +c.allow.toFixed(4), pct: +(c.allow / GO_BASE_USD * 100).toFixed(2),
        priced: c.priced, unpriced: c.unpriced,
        models: [...c.models.values()].sort((a, b) => b.cost - a.cost).slice(0, 12)
            .map((m) => ({
                model: m.model, reqs: m.reqs, in: m.in, out: m.out, cached: m.cached,
                cost: +m.cost.toFixed(4), allow: +m.allow.toFixed(4),
                pct: m.cap ? +(m.cost / m.cap * 100).toFixed(2) : null,     // 占该模型自身月上限
                cap: m.cap || null,
            })),
    }));
}
const QUOTA_TTL_MS = 5 * 60 * 1000;
let QUOTA_CACHE = { t: 0, data: null };

const preHash = (s, n) => crypto.createHash('sha1').update(s.slice(0, n)).digest('hex').slice(0, 10);
function convFingerprint(body) {   // 会话指纹：系统提示 + 第一条 user 消息的哈希；判断缓存好坏必须按会话分组
    try {
        const msgs = Array.isArray(body?.messages) ? body.messages : Array.isArray(body?.input) ? body.input : [];
        let sys = '', first = '';
        for (const m of msgs) {
            if (!m || typeof m !== 'object') continue;
            const role = m.role || (m.type === 'function_call_output' ? 'tool' : '');
            const txt = typeof m.content === 'string' ? m.content
                : Array.isArray(m.content) ? m.content.map((p) => (p && p.text) || '').join('') : '';
            if (!sys && (role === 'system' || role === 'developer')) sys = txt;
            else if (!first && role === 'user' && txt) first = txt;
            if (sys && first) break;
        }
        if (!sys && !first) return null;
        return crypto.createHash('sha1').update(sys + '\u0000' + first).digest('hex').slice(0, 8);
    } catch { return null; }
}
function clientIp(req) {
    const h = req.headers || {};
    const cf = String(h['cf-connecting-ip'] || '').trim();     // CF 隧道下这是真实客户端 IP
    if (cf) return cf;
    const xff = String(h['x-forwarded-for'] || '').split(',')[0].trim();
    if (xff) return xff;
    return String(req.socket?.remoteAddress || '').replace(/^::ffff:/, '');
}
function sendJson(res, code, obj) { const b = jdump(obj); res.writeHead(code, { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(b) }); res.end(b); }
function readBody(req) { return new Promise((resolve, reject) => { const c = []; req.on('data', (d) => c.push(d)); req.on('end', () => resolve(Buffer.concat(c).toString('utf8'))); req.on('error', reject); }); }

const server = http.createServer(async (req, res) => {
    const url = new URL(req.url, 'http://x');
    const host = (req.headers.host || '').split(':')[0];
    // 面板域名单独密码（Basic；只对配置里的域名生效，本机/容器内不受影响）
    const da = CFG.dashboard_auth || {};
    let dashOk = false;
    if (da.enabled && Array.isArray(da.hosts) && da.hosts.includes(host)) {
        const hdr = req.headers['authorization'] || '';
        let ok = false;
        if (hdr.startsWith('Basic ')) {
            try {
                const [u, p] = Buffer.from(hdr.slice(6), 'base64').toString('utf8').split(':');
                const h = crypto.createHash('sha256').update((da.salt || '') + (p || '')).digest('hex');
                ok = (u === da.username) && (h === da.password_sha256);
            } catch { }
        }
        if (!ok) {
            res.writeHead(401, { 'WWW-Authenticate': 'Basic realm="LLM Gateway", charset="UTF-8"', 'Content-Type': 'text/plain; charset=utf-8' });
            return res.end('需要密码\n');
        }
        dashOk = true;
    }
    if (url.pathname === '/healthz') return sendJson(res, 200, { ok: true, enabled: isEnabled(), capture: captureOn(), total: TOTAL, models: (CFG.models || []).length });
    if (url.pathname === '/v1/models') {
        const mA = authenticate(req);
        if (!mA) return sendJson(res, 401, { error: { message: 'Invalid API key', type: 'invalid_request_error', code: 'invalid_api_key' } });
        if (!isEnabled()) return sendJson(res, 503, { error: { message: GATEWAY_OFF_MSG, type: 'service_unavailable', code: 'gateway_disabled' } });
        // 协议感知：优先级 = ?protocol= > key 绑定的协议 > Anthropic 客户端头 > 该 key 最近的原生协议用法 > 不过滤
        // 只有 responses/messages 这类“原生客户端”才过滤；chat 客户端（网关会翻译）永远看全量。
        const want = String(url.searchParams.get('protocol') || '').toLowerCase();
        const bound = String(mA.protocol || '').toLowerCase();
        const isAnthropic = !!req.headers['anthropic-version'];
        // 协议互转已全通：每种客户端都能用每种模型。默认仍按协议过滤（保持旧行为）；
        // 置 CFG.model_filter_protocol = false 即对所有客户端展示全量模型（?protocol= 显式指定始终优先）。
        const only = ['chat', 'responses', 'messages', 'gemini'].includes(want) ? want
            : CFG.model_filter_protocol === false ? null
                : ['chat', 'responses', 'messages', 'gemini'].includes(bound) ? bound
                    : isAnthropic ? 'messages'
                        : (keyProtocolHint(mA.name) === 'chat' ? null : keyProtocolHint(mA.name));   // 最近用 chat = 不过滤
        // 模型白名单先过滤（key 级别）：被限制的 key 连「支持哪些模型」都只看得到自己的那几个。
        const pool = (CFG.models || []).filter((m) => modelAllowed(mA, m));
        const entries = pool.map((m) => {
            const { protocol, detected } = modelProtocol(m);
            return { id: m, protocol, detected, efforts: effortsForModel(m) };
        }).filter((e) => !only || e.protocol === only);
        if (isAnthropic) {                                    // Anthropic 规范的模型列表形状
            return sendJson(res, 200, {
                data: entries.map((e) => ({
                    type: 'model', id: e.id, display_name: e.id, created_at: '2026-01-01T00:00:00Z',
                    ...(e.efforts ? { reasoning_efforts: e.efforts } : {}),
                })),
                has_more: false,
                first_id: entries.length ? entries[0].id : null,
                last_id: entries.length ? entries[entries.length - 1].id : null,
            });
        }
        return sendJson(res, 200, {
            object: 'list',
            data: entries.map((e) => ({
                id: e.id, object: 'model', owned_by: 'gateway', protocol: e.protocol, detected: e.detected,
                ...(e.efforts ? { reasoning_efforts: e.efforts } : {}),
            })),
        });
    }
    if (url.pathname === '/' || url.pathname === '/dashboard') {
        try {
            const html = fs.readFileSync(path.join(ROOT, 'dashboard.html'), 'utf8');
            res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
            return res.end(html);
        } catch (e) { return sendJson(res, 500, { error: { message: 'dashboard.html 缺失: ' + e.message } }); }
    }
    if (url.pathname === '/api/stats/requests') {
        if (!dashOk && !authenticate(req)) return sendJson(res, 401, { error: { message: 'Invalid API key', type: 'invalid_request_error', code: 'invalid_api_key' } });
        let q = {};
        if (req.method === 'POST') { try { q = JSON.parse((await readBody(req)) || '{}'); } catch { return sendJson(res, 400, { error: { message: 'invalid json' } }); } }
        else for (const [k, v] of url.searchParams.entries()) q[k] = v;
        try { return sendJson(res, 200, queryRequests(q)); } catch (e) { return sendJson(res, 500, { error: { message: e.message } }); }
    }
    if (url.pathname === '/api/upstream/quota') {          // 上游额度：余额接口 + 用量估算（都按 config.quota 配置）
        if (!dashOk && !authenticate(req)) return sendJson(res, 401, { error: { message: 'Invalid API key', type: 'invalid_request_error', code: 'invalid_api_key' } });
        const now = Date.now();
        const force = url.searchParams.get('refresh') === '1';
        if (force || !QUOTA_CACHE.data || now - QUOTA_CACHE.t > QUOTA_TTL_MS) {
            const ds = await upstreamBalance();
            const goQuota = await upstreamUsage();
            const go = computeGoUsage();
            const anchor = usageCycleAnchor(goQuota);
            const cycles = anchor ? computeUsageCycles(anchor) : null;
            QUOTA_CACHE = {
                t: now,
                data: {
                    ts: now,
                    balance: { ...ds, at: now },
                    usage: {
                        quota: goQuota,
                        detail_note: '网关侧明细：按 config.prices 里的单价 × 本网关记录的 token 估算，只覆盖走本网关的流量',
                        cycle_stats: cycles ? { anchor, base: GO_BASE_USD, cycles } : null,
                        ...go,
                    },
                },
            };
        }
        return sendJson(res, 200, QUOTA_CACHE.data);
    }
    if (url.pathname === '/api/stats') {
        if (!dashOk && !authenticate(req)) return sendJson(res, 401, { error: { message: 'Invalid API key', type: 'invalid_request_error', code: 'invalid_api_key' } });
        // 概览页时间口径：?since=&until=（epoch ms，可只给一个）+ ?tz=（浏览器时区偏移分钟）
        // ?unit=hour|day 指定分桶口径（前端按区间给：今天/昨天/24h → hour，本周/本月/上月/全部 → day）
        const numParam = (k) => {
            const v = url.searchParams.get(k);
            if (v === null || v === '') return null;
            const n = Number(v);
            return Number.isFinite(n) ? n : null;
        };
        const unitParam = () => {
            const v = url.searchParams.get('unit');
            return v === 'hour' || v === 'day' ? v : undefined;
        };
        const dimParam = () => {
            const v = url.searchParams.get('dim');
            return v && DIM_EXPR[v] ? v : undefined;      // 只认 model / provider / key；其它值＝不分色（旧行为）
        };
        return sendJson(res, 200, statsSnapshot({ since: numParam('since'), until: numParam('until'), tz: numParam('tz'), unit: unitParam(), dim: dimParam() }));
    }

    // ---------------- 推理档位探测（面板） ----------------
    if (url.pathname === '/api/probe/efforts' || url.pathname === '/api/probe/efforts/bulk') {
        if (!dashOk && !authenticate(req)) return sendJson(res, 401, { error: { message: 'unauthorized', type: 'invalid_request_error' } });
        if (req.method !== 'POST') return sendJson(res, 405, { error: { message: 'POST only' } });
        let b = {};
        try { b = JSON.parse((await readBody(req)) || '{}'); } catch { return sendJson(res, 400, { error: { message: 'invalid json' } }); }
        try {
            const lv = b.levels ? parseEffortsInput(b.levels) : undefined;
            if (url.pathname === '/api/probe/efforts') {
                const out = await probeModelEfforts(b.id, { levels: lv, timeout: b.timeout });
                return sendJson(res, 200, { ok: true, ...out, config: configView() });
            }
            // 批量：一次最多 20 个模型，串行探测（不并发打上游），最后一次写盘（免得刷一堆 config 备份）
            const provider = String(b.provider || '').trim();
            const onlyUnprobed = b.only_unprobed !== false;
            const limit = Math.min(Math.max(Number(b.limit) || 5, 1), 20);
            let ids = Array.isArray(b.ids) && b.ids.length ? b.ids.map(String) : (CFG.models || []);
            ids = ids.filter((id) => {
                const rt = routeFor(id);
                if (!rt) return false;
                if (provider && rt.provider !== provider) return false;
                if (onlyUnprobed) {
                    const k = effortKeyOf(rt.provider, rt.upstream_model);
                    if (normEfforts((CFG.effort_probes || {})[k] && (CFG.effort_probes || {})[k].levels).length) return false;
                    if (normEfforts((CFG.model_efforts || {})[k]).length) return false;
                }
                return true;
            });
            const picked = ids.slice(0, limit);
            const results = [];
            for (const id of picked) {
                try { results.push(await probeModelEfforts(id, { persist: false, levels: lv, timeout: b.timeout })); }
                catch (e) { results.push({ id, error: e.message }); }
            }
            const cfg = JSON.parse(fs.readFileSync(CONFIG_PATH, 'utf8'));
            cfg.effort_probes = cfg.effort_probes || {};
            let saved = 0;
            for (const r of results) {
                if (r.error) continue;
                cfg.effort_probes[effortKeyOf(r.provider, r.upstream_model)] = {
                    levels: r.levels, rejected: r.rejected, saw_reasoning: r.saw_reasoning,
                    format: r.format, thinking_style: r.thinking_style, at: r.at, note: r.note,
                };
                saved++;
            }
            if (saved) { saveConfig(cfg); reloadConfig(); }
            return sendJson(res, 200, { ok: true, probed: results.length, saved, remaining: Math.max(0, ids.length - picked.length), results, config: configView() });
        } catch (e) { return sendJson(res, 400, { error: { message: e.message } }); }
    }

    // ---------------- 配置读写（面板密码通过 或 虚拟 key） ----------------
    if (url.pathname.startsWith('/api/config')) {
        if (!dashOk && !authenticate(req)) return sendJson(res, 401, { error: { message: 'unauthorized', type: 'invalid_request_error' } });
        if (req.method === 'GET') return sendJson(res, 200, configView());
        let b = {};
        try { b = JSON.parse((await readBody(req)) || '{}'); } catch { return sendJson(res, 400, { error: { message: 'invalid json' } }); }
        try {
            if (url.pathname === '/api/config/key/reveal') {        // 回显 key：只允许看板域名（已登录）或本机
                const localHost = ['127.0.0.1', 'localhost', '::1', 'gateway'].includes(host) || host.startsWith('172.28.');
                if (!dashOk && !localHost) return sendJson(res, 403, { error: { message: '显示 key 只能在看板域名（登录后）或本机执行' } });
                const name = String(b.name || '').trim();
                const k = (CFG.keys || []).find((x) => x.name === name);
                if (!k) return sendJson(res, 404, { error: { message: 'key 不存在: ' + name } });
                log('CONFIG reveal key', name);
                return sendJson(res, 200, { name, key: k.key });
            }
            if (url.pathname === '/api/config/provider/detail') {   // 编辑用：回显供应商当前属性（含 UA/额外头），只允许看板域名（已登录）或本机
                const localHost = ['127.0.0.1', 'localhost', '::1', 'gateway'].includes(host) || host.startsWith('172.28.');
                if (!dashOk && !localHost) return sendJson(res, 403, { error: { message: '编辑供应商只能在看板域名（登录后）或本机执行' } });
                const name = String(b.name || '').trim();
                const p = (CFG.providers || {})[name];
                if (!p) return sendJson(res, 404, { error: { message: '供应商不存在: ' + name } });
                const rt = (CFG.routes || []).find((r) => r.provider === name && r.strip);
                return sendJson(res, 200, {
                    name,
                    prefix: rt ? String(rt.strip || '').replace(/\/$/, '') : '',
                    base_url: p.base_url || '',
                    api_key_env: p.api_key_env || '',
                    has_key: !!(process.env[p.api_key_env] || '').length,
                    user_agent: p.user_agent || '',
                    reasoning_echo: !!p.reasoning_echo,
                    efforts: normEfforts((CFG.model_efforts || {})[effortKeyOf(name, '*')]),
                    test: providerIsTest(p),
                    extra_headers: p.extra_headers || {},
                    models: (CFG.models || []).filter((m) => rt && m.startsWith(rt.strip)).length,
                });
            }
            if (url.pathname === '/api/config/provider/models') {   // 拉取上游模型列表（不打网关统计）
                const out = await fetchProviderModels(b);
                return sendJson(res, 200, { ok: true, ...out });
            }
            const out = applyConfigChange(url.pathname, b);
            log('CONFIG', url.pathname, JSON.stringify(out).slice(0, 200));
            return sendJson(res, 200, { ok: true, ...out, config: configView() });
        } catch (e) { return sendJson(res, 400, { error: { message: e.message } }); }
    }
    // ---------- 原生 Gemini 直通：客户端自带 Google 协议时原样转发（不做翻译）----------
    // 形状：/v1beta/models/<model>:generateContent 与 :streamGenerateContent[?alt=sse]
    // （同时接受 /v1/models/... 前缀，Google SDK 两种 base_url 都见过）
    // 模型名可能自带前缀斜杠（对外名形如 ocg/gemini-...），所以这里不能用 [^/:]+ 排除斜杠
    const gemHit = /^\/(?:v1beta|v1)\/models\/(.+):(generateContent|streamGenerateContent)$/.exec(url.pathname);
    if (gemHit) {
        if (req.method !== 'POST') {
            log('404', req.method, url.pathname, 'host=' + host);
            return sendJson(res, 404, { error: { message: 'not found', type: 'invalid_request_error' } });
        }
        if (!isEnabled()) return sendJson(res, 503, { error: { message: GATEWAY_OFF_MSG, type: 'service_unavailable', code: 'gateway_disabled' } });
        const t0 = Date.now();
        const gAuth = authenticate(req);        // 两种头都认：Bearer / x-api-key（Google SDK 通常发 x-goog-api-key，见下）
        if (!gAuth) return sendJson(res, 401, { error: { code: 401, status: 'UNAUTHENTICATED', message: 'Invalid API key' } });
        let gRaw = '', gBody = {};
        try {
            gRaw = await readBody(req);
            gBody = gRaw ? JSON.parse(gRaw) : {};
        } catch { return sendJson(res, 400, { error: { code: 400, status: 'INVALID_ARGUMENT', message: 'invalid json body' } }); }
        const gModel = decodeURIComponent(gemHit[1]);
        if (!modelAllowed(gAuth, gModel)) {          // key 的模型白名单
            log('DENY model', gAuth.name, gModel, '(白名单) gemini');
            record({ ts: t0, key: gAuth.name, model: gModel, stream: gemHit[2] === 'streamGenerateContent', ip: clientIp(req), passthrough: true, cproto: 'gemini', req_bytes: Buffer.byteLength(gRaw || ''), status: 403, ms: Date.now() - t0, provider: null, error: 'model_not_allowed' });
            return sendJson(res, 403, { error: { code: 403, status: 'PERMISSION_DENIED', message: `model "${gModel}" is not allowed for this key` } });
        }
        const gRt = routeFor(gModel);
        const gStream = gemHit[2] === 'streamGenerateContent';
        const gBase = { ts: t0, key: gAuth.name, model: gModel, stream: gStream, ip: clientIp(req), passthrough: true, cproto: 'gemini', conv: convFingerprint(gBody), req_bytes: Buffer.byteLength(gRaw || ''), res_bytes: 0, up_res_bytes: 0 };
        countOut(res, gBase);
        if (!gRt) {
            record({ ...gBase, status: 400, ms: Date.now() - t0, provider: null });
            return sendJson(res, 400, { error: { code: 400, status: 'INVALID_ARGUMENT', message: `model "${gModel}" not routed` } });
        }
        const gUpFmt = resolveFormat(gRt);
        // 上游不是 gemini 协议时不转：这一版只做「chat 客户端 → gemini 上游」，跨协议翻译先不做
        if (gUpFmt !== 'gemini') {
            record({ ...gBase, status: 400, ms: Date.now() - t0, provider: gRt.provider, error: 'upstream format is ' + gUpFmt });
            return sendJson(res, 400, { error: { code: 400, status: 'FAILED_PRECONDITION', message: `模型 ${gModel} 的上游协议是 ${gUpFmt}，不是 gemini；Gemini 原生客户端目前只支持打 gemini 协议的上游` } });
        }
        const gHeaders = upstreamHeaders(gRt.providerCfg, gAuth.session_id, gStream ? 'text/event-stream' : 'application/json', 'gemini');
        // 客户端带来的生成参数原样保留，只把 model 从路径里的对外名换成上游名
        const gPayload = { ...gBody };
        const gUrl = gRt.providerCfg.base_url + '/models/' + encodeURIComponent(gRt.upstream_model) + ':' + gemHit[2] + (gStream ? '?alt=sse' : '');
        capture('client_req', { cproto: 'gemini', ip: clientIp(req), key: gAuth.name, model: gModel, stream: gStream, body: gBody });
        capture('upstream_req', { cproto: 'gemini', url: gUrl, key: gAuth.name, model: gModel, upstream_model: gRt.upstream_model, payload: gPayload });
        try {
            const ac = new AbortController();
            const onClose = () => { if (!res.writableEnded) { try { ac.abort(); } catch { } } };
            req.on('close', onClose);
            const up = await fetch(gUrl, { method: 'POST', headers: gHeaders, body: jdump(gPayload), signal: ac.signal });
            capture('upstream_status', { cproto: 'gemini', status: up.status, ok: up.ok });
            req.off('close', onClose);
            if (!up.ok) {
                const txt = await up.text();
                let parsed = null; try { parsed = JSON.parse(txt); } catch { }
                log('PASSTHROUGH ERROR', up.status, gRt.provider, 'gemini', gModel, txt.slice(0, 250));
                record({ ...gBase, status: up.status, ms: Date.now() - t0, provider: gRt.provider, upstream_model: gRt.upstream_model, format: 'gemini', error: txt.slice(0, 500) });
                return sendJson(res, up.status, parsed?.error ? parsed : { error: { code: up.status, message: txt.slice(0, 800) || 'upstream error' } });
            }
            if (gStream) {
                if (!res.headersSent) res.writeHead(200, { 'Content-Type': 'text/event-stream; charset=utf-8', 'Cache-Control': 'no-cache', 'Connection': 'keep-alive', 'X-Accel-Buffering': 'no' });
                let buf = '', ttft = null, usage = {}, lastEvt = '';
                const dec = new StringDecoder('utf8');
                const reader = up.body.getReader();
                const eat = (raw) => {
                    for (const line of raw.split('\n')) {
                        const s = line.trim();
                        if (!s) continue;
                        const p = s.startsWith('data:') ? s.slice(5).trim() : s;
                        if (!p || p === '[DONE]') continue;
                        let d; try { d = JSON.parse(p); } catch { continue; }
                        if (d.usageMetadata) usage = usageFromGemini(d.usageMetadata);
                        if (d.candidates?.[0]?.finishReason) lastEvt = d.candidates[0].finishReason;
                        if (d.error) lastEvt = 'error';
                    }
                };
                for (; ;) {
                    const { done, value } = await reader.read(); if (done) break;
                    if (!value) continue;
                    gBase.up_res_bytes += value.length;
                    if (ttft == null) ttft = Date.now() - t0;
                    buf += dec.write(Buffer.from(value));      // StringDecoder 吃 Buffer，不能直接喂 Uint8Array
                    let i;
                    while ((i = buf.indexOf('\n\n')) >= 0) { const raw = buf.slice(0, i); buf = buf.slice(i + 2); eat(raw); res.write(raw + '\n\n'); }   // 逐字节回吐（保真）
                }
                buf += dec.end();
                if (buf.trim()) { eat(buf); res.write(buf); }
                res.end();
                record({ ...gBase, status: 200, ms: Date.now() - t0, provider: gRt.provider, upstream_model: gRt.upstream_model, format: 'gemini', usage, ttft_ms: ttft || 0, usage_src: Object.keys(usage).length ? 'stream' : 'missing', last_evt: lastEvt });
                log('OK', 'gemini->gemini(直通)', gRt.upstream_model, `in=${usage.in || 0} out=${usage.out || 0}`, `${Date.now() - t0}ms`);
            } else {
                const txt = await up.text();
                let usage = {};
                try { const d = JSON.parse(txt); usage = usageFromGemini(d.usageMetadata); } catch { }
                res.writeHead(200, { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(txt) });
                res.end(txt);
                record({ ...gBase, status: 200, ms: Date.now() - t0, provider: gRt.provider, upstream_model: gRt.upstream_model, format: 'gemini', usage, ttft_ms: 0, usage_src: Object.keys(usage).length ? 'body' : 'missing', up_res_bytes: Buffer.byteLength(txt) });
                log('OK', 'gemini->gemini(直通)', gRt.upstream_model, `in=${usage.in || 0} out=${usage.out || 0}`, `${Date.now() - t0}ms`);
            }
        } catch (e) {
            log('PASSTHROUGH ERROR', 'gemini', gRt.provider, gModel, e.message);
            record({ ...gBase, status: 502, ms: Date.now() - t0, provider: gRt.provider, upstream_model: gRt.upstream_model, format: 'gemini', error: 'fetch failed: ' + e.message });
            if (!res.headersSent) return sendJson(res, 502, { error: { code: 502, status: 'UNAVAILABLE', message: 'upstream fetch failed: ' + e.message } });
            try { res.end(); } catch { }
        }
        return;
    }
    // ---------- 原生协议直通：客户端自带 responses / messages 协议时原样转发（不做翻译）----------
    if (url.pathname === '/v1/responses' || url.pathname === '/v1/messages') {
        const kind = url.pathname === '/v1/responses' ? 'responses' : 'messages';
        if (req.method !== 'POST') {
            log('404', req.method, url.pathname, 'host=' + host);
            return sendJson(res, 404, { error: { message: 'not found', type: 'invalid_request_error' } });
        }
        if (!isEnabled()) return sendJson(res, 503, { error: { message: GATEWAY_OFF_MSG, type: 'service_unavailable', code: 'gateway_disabled' } });
        const t0 = Date.now();
        const pAuth = authenticate(req);
        if (!pAuth) return sendJson(res, 401, { error: { message: 'Invalid API key', type: 'invalid_request_error', code: 'invalid_api_key' } });
        noteKeyProtocol(pAuth.name, kind);            // 记下这个 key 的原生协议用法 → /v1/models 据此过滤
        let pBody;
        let pRaw = '';
        try { pRaw = await readBody(req); pBody = JSON.parse(pRaw); } catch { return sendJson(res, 400, { error: { message: 'invalid json body', type: 'invalid_request_error' } }); }
        if (!modelAllowed(pAuth, pBody.model)) {      // key 的模型白名单（原生直通 + 互转都走这里）
            log('DENY model', pAuth.name, pBody.model, '(白名单)', kind);
            record({ ts: t0, key: pAuth.name, model: pBody.model, stream: !!pBody.stream, ip: clientIp(req), passthrough: true, cproto: kind, conv: convFingerprint(pBody), req_bytes: Buffer.byteLength(pRaw || ''), status: 403, ms: Date.now() - t0, provider: null, error: 'model_not_allowed' });
            return sendJson(res, 403, { error: { message: `model "${pBody.model}" is not allowed for this key`, type: 'invalid_request_error', code: 'model_not_allowed' } });
        }
        const pRt = routeFor(pBody.model);
        const pBase = { ts: t0, key: pAuth.name, model: pBody.model, stream: !!pBody.stream, ip: clientIp(req), passthrough: true, cproto: kind, conv: convFingerprint(pBody), req_bytes: Buffer.byteLength(pRaw || ''), res_bytes: 0, up_res_bytes: 0 };
        countOut(res, pBase);
        if (!pRt) { record({ ...pBase, status: 400, ms: Date.now() - t0, provider: null }); return sendJson(res, 400, { error: { message: `model "${pBody.model}" not routed`, type: 'invalid_request_error' } }); }
        // 协议互转（按 key 开关 keys[].protocol_interop，默认关；看板「客户端 key」页可勾）：开启时
        // 「上游协议 != 客户端协议」走 chat 中枢互转；关闭时一律走下面的原生直通，由上游报协议不支持。
        const upFmt = resolveFormat(pRt);
        if (upFmt !== kind && pAuth.interop) {
            log('CONVERT', kind, '->', upFmt, pBody.model);
            return serveChatPipeline(req, res, { auth: pAuth, rt: pRt, body: pBody, rawBody: pRaw, t0, clientProto: kind });
        }
        const pHeaders = upstreamHeaders(pRt.providerCfg, pAuth.session_id, pBody.stream ? 'text/event-stream' : 'application/json', kind);
        for (const hn of Object.keys(req.headers)) {                 // 把客户端的协议头原样带上去（anthropic-beta / openai-beta / x-stainless-*）
            if (/^(anthropic-|openai-beta|x-stainless)/i.test(hn)) pHeaders[hn] = String(req.headers[hn]);
        }
        const pPayload = { ...clampBodyEffort(pBody, pRt), model: pRt.upstream_model };     // 只换模型名 + 档位就近降级，输入原样（含客户端自己回传的 reasoning item）
        if (pRt.providerCfg.reasoning_echo && kind === 'responses' && Array.isArray(pBody.input)) {
            const inj = injectReasonsIntoInput(pBody.input, pRt.provider);   // 客户端没回传的那几条，由网关补上（只补缺的、不重复、且只在同一供应商内）
            if (inj.injected) { pPayload.input = inj.input; pBase.reason_inj = inj.injected; log('[reason] 直通补回', inj.injected, '条 ->', pBody.model); }
        }
        const pUrl = pRt.providerCfg.base_url + (kind === 'responses' ? '/responses' : '/messages');
        capture('client_req', { kind, ip: clientIp(req), key: pAuth.name, model: pBody.model, stream: !!pBody.stream, body: pBody });
        capture('upstream_req', { kind, url: pUrl, key: pAuth.name, model: pBody.model, upstream_model: pRt.upstream_model, payload: pPayload });
        try {
            const ac = new AbortController();
            const onClose = () => { if (!res.writableEnded) { try { ac.abort(); } catch { } } };
            req.on('close', onClose);
            const up = await fetch(pUrl, { method: 'POST', headers: pHeaders, body: jdump(pPayload), signal: ac.signal });
            capture('upstream_status', { kind, status: up.status, ok: up.ok });
            req.off('close', onClose);
            if (!up.ok) {
                const txt = await up.text();
                let parsed = null; try { parsed = JSON.parse(txt); } catch { }
                log('PASSTHROUGH ERROR', up.status, pRt.provider, kind, pBody.model, txt.slice(0, 250));
                record({ ...pBase, status: up.status, ms: Date.now() - t0, provider: pRt.provider, upstream_model: pRt.upstream_model, format: kind, error: txt.slice(0, 500) });
                return sendJson(res, up.status, parsed?.error ? { error: parsed.error } : { error: { message: txt.slice(0, 800) || 'upstream error', type: 'upstream_error' } });
            }
            if (pBody.stream) {
                if (!res.headersSent) res.writeHead(200, { 'Content-Type': 'text/event-stream; charset=utf-8', 'Cache-Control': 'no-cache', 'Connection': 'keep-alive', 'X-Accel-Buffering': 'no' });
                let tail = '', ttft = null, lineBuf = '', lastEvt = '', usage = {}, headSample = '';
                const dec = new StringDecoder('utf8');
                const reader = up.body.getReader();
                const echoOn = !!pRt.providerCfg.reasoning_echo, rctx = { provider: pRt.provider };   // 直通路上也归档推理链，供后续请求补回
                const eatLine = (line) => {          // 增量解析：大事件（实测 response.completed 可达 24KB~数百KB）绝不能靠"尾部窗口"兜
                    let payload = null;
                    if (line.startsWith('data:')) payload = line.slice(5).trim();
                    else if (line.charAt(0) === '{' || line.charAt(0) === '[') payload = line.trim();   // 有的中转站直接吐裸 JSON 行（无 data: 前缀）
                    if (!payload || payload === '[DONE]') return;
                    let d; try { d = JSON.parse(payload); } catch { return; }
                    if (!d || typeof d !== 'object') return;
                    if (d.type) lastEvt = d.type;
                    if (echoOn && kind === 'responses' && d.type === 'response.output_item.done' && d.item) {
                        const it = d.item;
                        if (it.type === 'reasoning') reasonCapture(rctx, it);
                        else if (it.type === 'function_call') reasonAttach(rctx, it.call_id || it.id);
                    }
                    const u = usageFromLine(kind, d);
                    if (u) for (const k of Object.keys(u)) if (typeof u[k] === 'number' && (u[k] > 0 || usage[k] == null)) usage[k] = u[k];
                };
                for (; ;) {
                    const { done, value } = await reader.read();
                    if (done) break;
                    if (ttft == null) ttft = Date.now() - t0;
                    const text = dec.write(Buffer.from(value));
                    lineBuf += text;
                    for (let i = lineBuf.indexOf('\n'); i >= 0; i = lineBuf.indexOf('\n')) {
                        eatLine(lineBuf.slice(0, i).replace(/\r$/, ''));
                        lineBuf = lineBuf.slice(i + 1);
                    }
                    tail = (tail + text).slice(-8192);   // 诊断用（last_evt 已增量记录）
                    if (!headSample) headSample = text.slice(0, 300);   // 只在 usage 缺失时随记录落库，用来一眼看出上游到底发了什么格式
                    pBase.up_res_bytes += value.length;
                    res.write(value);
                }
                lineBuf += dec.end();
                if (lineBuf) eatLine(lineBuf.replace(/\r$/, ''));
                res.end();
                if (!Object.keys(usage).length) usage = usageFromTail(kind, tail);   // 兜底
                const prec = { ...pBase, status: 200, ms: Date.now() - t0, provider: pRt.provider, upstream_model: pRt.upstream_model, format: kind, usage, ttft_ms: ttft || 0, usage_src: Object.keys(usage).length ? 'stream' : 'missing', last_evt: lastEvt };
                if (!Object.keys(usage).length) prec.up_head = headSample;
                record(prec);
            } else {
                const txt = await up.text();
                let d = null; try { d = JSON.parse(txt); } catch { }
                if (pRt.providerCfg.reasoning_echo && kind === 'responses' && d && Array.isArray(d.output)) {   // 非流式同样归档
                    const rctx2 = { provider: pRt.provider };
                    for (const it of d.output) {
                        if (!it || typeof it !== 'object') continue;
                        if (it.type === 'reasoning') reasonCapture(rctx2, it);
                        else if (it.type === 'function_call') reasonAttach(rctx2, it.call_id || it.id);
                    }
                }
                let usage = {};
                if (d && d.usage) {
                    usage = kind === 'responses' ? usageFromResponses(d.usage)
                        : { in: d.usage.input_tokens || 0, out: d.usage.output_tokens || 0, cached: d.usage.cache_read_input_tokens || 0, reasoning: 0 };
                }
                res.writeHead(200, { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(txt) });
                res.end(txt);
                record({ ...pBase, status: 200, ms: Date.now() - t0, provider: pRt.provider, upstream_model: pRt.upstream_model, format: kind, usage, ttft_ms: 0, usage_src: Object.keys(usage).length ? 'body' : 'missing', up_res_bytes: Buffer.byteLength(txt) });
            }
        } catch (e) {
            log('PASSTHROUGH ERROR', kind, pRt.provider, pBody.model, e.message);
            record({ ...pBase, status: 502, ms: Date.now() - t0, provider: pRt.provider, upstream_model: pRt.upstream_model, format: kind, error: 'fetch failed: ' + e.message });
            if (!res.headersSent) return sendJson(res, 502, { error: { message: 'upstream fetch failed: ' + e.message, type: 'upstream_error' } });
            try { res.end(); } catch { }
        }
        return;
    }
    if (url.pathname.startsWith('/v1/models/')) {                 // 单模型查询（客户端常见探活）
        const id = decodeURIComponent(url.pathname.slice('/v1/models/'.length));
        const mAuth = authenticate(req);
        if (!mAuth) return sendJson(res, 401, { error: { message: 'Invalid API key', type: 'invalid_request_error', code: 'invalid_api_key' } });
        if (!(CFG.models || []).includes(id) || !modelAllowed(mAuth, id)) {   // 只认对外声明的模型清单（路由表有 `.` 兜底，不能用 routeFor 判断存在性）；白名单外一律当作不存在
            return sendJson(res, 404, { error: { message: 'model not found: ' + id, type: 'invalid_request_error', code: 'model_not_found' } });
        }
        const info = modelProtocol(id);
        const eff = effortsForModel(id);
        return sendJson(res, 200, { id, object: 'model', owned_by: 'gateway', protocol: info.protocol, detected: info.detected, ...(eff ? { reasoning_efforts: eff } : {}) });
    }
    if (url.pathname !== '/v1/chat/completions' || req.method !== 'POST') {
        // 404 不记统计，容易被当成“连不上”却查无实据 → 记下方法/路径/来源，便于定位客户端配错
        log('404', req.method, url.pathname, 'host=' + host, 'ua=' + String(req.headers['user-agent'] || '').slice(0, 70));
        return sendJson(res, 404, { error: { message: 'not found', type: 'invalid_request_error' } });
    }
    if (!isEnabled()) return sendJson(res, 503, { error: { message: GATEWAY_OFF_MSG, type: 'service_unavailable', code: 'gateway_disabled' } });

    const t0 = Date.now();
    const auth = authenticate(req);
    if (!auth) return sendJson(res, 401, { error: { message: 'Invalid API key', type: 'invalid_request_error', code: 'invalid_api_key' } });
    noteKeyProtocol(auth.name, 'chat');          // 这个 key 最近走的是 chat 翻译路 → /v1/models 不过滤

    let body;
    let rawBody = '';
    try { rawBody = await readBody(req); body = JSON.parse(rawBody); } catch { return sendJson(res, 400, { error: { message: 'invalid json body', type: 'invalid_request_error' } }); }
    if (!modelAllowed(auth, body.model)) {         // key 的模型白名单：不让用的模型直接 403（不走到上游，不烧额度）
        record({ ts: t0, key: auth.name, model: body.model, stream: !!body.stream, ip: clientIp(req), cproto: 'chat', req_bytes: Buffer.byteLength(rawBody || ''), status: 403, ms: Date.now() - t0, provider: null, error: 'model_not_allowed' });
        log('DENY model', auth.name, body.model, '(白名单)');
        return sendJson(res, 403, { error: { message: `model "${body.model}" is not allowed for this key`, type: 'invalid_request_error', code: 'model_not_allowed' } });
    }
    const rt = routeFor(body.model);
    if (!rt) {
        record({ ts: t0, key: auth.name, model: body.model, stream: !!body.stream, ip: clientIp(req), cproto: 'chat', req_bytes: Buffer.byteLength(rawBody || ''), status: 400, ms: Date.now() - t0, provider: null });
        return sendJson(res, 400, { error: { message: `model "${body.model}" not routed`, type: 'invalid_request_error' } });
    }
    return serveChatPipeline(req, res, { auth, rt, body, rawBody, t0, clientProto: 'chat' });
});

const { host = '0.0.0.0', port = 8010 } = CFG.listen || {};
server.listen(port, host, () => log(`gateway listening on ${host}:${port} — keys=${KEYS.size} models=${(CFG.models || []).length} env=${ENV_LOADED} providers=${Object.keys(CFG.providers || {}).length} hold=${HOLD_MS > 0 ? HOLD_MS / 1000 + 's/' + KA_MS / 1000 + 's' : 'off'}`));
