#!/usr/bin/env node
/**
 * 网关核心：对客户端暴露 OpenAI Chat Completions，按模型路由到不同上游/不同协议。
 * - 上游 formats: "chat" = 原样透传 /chat/completions；"responses" = 翻译成 /v1/responses 再翻回来
 * - 鉴权：虚拟 key（Bearer 或 x-api-key），每个 key 绑定一个稳定的 x-upstream-session
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

// ---------------- 面板用户 / 会话（分用户鉴权） ----------------
// 账号存 config.users；管理员账号可以从 dashboard_auth 迁移（虚拟用户，不写盘）。
// 归属规则：providers[].owner / keys[].owner 缺省＝管理员所有（旧配置行为不变）。
const STATE_DIR = path.join(ROOT, 'state');
fs.mkdirSync(STATE_DIR, { recursive: true });
const SESSION_PATH = path.join(STATE_DIR, 'sessions.json');
const SESSION_TTL_MS = 7 * 24 * 3600e3;
const USER_RE = /^[a-z0-9][a-z0-9_.-]{1,31}$/i;
function hashPw(salt, pw) { return crypto.createHash('sha256').update(String(salt || '') + String(pw || '')).digest('hex'); }
function pwEq(a, b) {
    const x = Buffer.from(String(a || ''), 'utf8'), y = Buffer.from(String(b || ''), 'utf8');
    return x.length === y.length && x.length > 0 && crypto.timingSafeEqual(x, y);
}
function panelUser(name) {
    const n = String(name || '').trim();
    if (!n) return null;
    const u = (CFG.users || {})[n];
    if (u && u.password_sha256) {
        return { name: n, role: u.role === 'admin' ? 'admin' : 'user', salt: u.salt || '', password_sha256: u.password_sha256, created_at: u.created_at || 0 };
    }
    const da = CFG.dashboard_auth || {};
    if (da.username === n && da.password_sha256) {
        return { name: n, role: 'admin', salt: da.salt || '', password_sha256: da.password_sha256, created_at: 0, virtual: true };
    }
    return null;
}
function userPools(name) {                        // 该用户名下供应商所在的组
    const out = [];
    for (const [pn, p] of Object.entries(CFG.pools || {})) {
        if ((poolMembers(p) || []).some((m) => (((CFG.providers || {})[m.provider] || {}).owner === name))) out.push(pn);
    }
    return out;
}
function joinablePools() {
    return Object.entries(CFG.pools || {}).filter(([, p]) => p && p.user_join === true).map(([n]) => n);
}
function panelUserList() {
    const names = new Set([...Object.keys(CFG.users || {}), String((CFG.dashboard_auth || {}).username || '')].filter(Boolean));
    return [...names].sort().map((n) => {
        const u = panelUser(n) || {};
        return {
            name: n, role: u.role || 'user', created_at: u.created_at || 0, virtual: !!u.virtual,
            providers: Object.entries(CFG.providers || {}).filter(([, p]) => p && p.owner === n).map(([x]) => x),
            keys: (CFG.keys || []).filter((k) => k.owner === n).map((k) => k.name),
            pools: userPools(n),
        };
    });
}
// 会话：内存 + state/sessions.json（重启不掉线）
let SESSIONS = {};
try { SESSIONS = JSON.parse(fs.readFileSync(SESSION_PATH, 'utf8')) || {}; } catch { SESSIONS = {}; }
function saveSessions() {
    try {
        const tmp = SESSION_PATH + '.tmp';
        fs.writeFileSync(tmp, JSON.stringify(SESSIONS));
        try { fs.chmodSync(tmp, 0o600); } catch { }
        fs.renameSync(tmp, SESSION_PATH);
    } catch (e) { log('session save failed', e.message); }
}
function pruneSessions() {
    const now = Date.now(); let n = 0;
    for (const [t, s] of Object.entries(SESSIONS)) if (!s || !s.exp || s.exp <= now) { delete SESSIONS[t]; n++; }
    if (n) saveSessions();
}
pruneSessions();
function newSession(name) {
    const token = crypto.randomBytes(32).toString('base64url');
    SESSIONS[token] = { u: name, exp: Date.now() + SESSION_TTL_MS };
    pruneSessions(); saveSessions();
    return token;
}
function sessionUser(req) {
    const t = String(req.headers['x-gw-token'] || '').trim();
    if (!t) return null;
    const s = SESSIONS[t];
    if (!s) return null;
    if (!s.exp || s.exp <= Date.now()) { delete SESSIONS[t]; saveSessions(); return null; }
    const u = panelUser(s.u);
    if (!u) { delete SESSIONS[t]; saveSessions(); return null; }
    return { ...u, token: t };
}
const LOGIN_FAILS = new Map();                    // ip -> { n, until }
function loginBlocked(ip) {
    const f = LOGIN_FAILS.get(ip);
    if (!f) return false;
    if (f.until && f.until <= Date.now()) { LOGIN_FAILS.delete(ip); return false; }
    return f.n >= 8;
}
function loginFailed(ip) {
    const f = LOGIN_FAILS.get(ip) || { n: 0, until: 0 };
    f.n += 1;
    if (f.n >= 8) f.until = Date.now() + 10 * 60e3;
    LOGIN_FAILS.set(ip, f);
    if (LOGIN_FAILS.size > 500) LOGIN_FAILS.clear();
}
// 老的无归属网关 key 仍可用于面板，但只限「本机直连」（无 CF 转发头 + 本机 Host）。
// 远程拿一个分享出去的 key 不能读/写面板；远程一律走账号登录。
function keyPanelAllowed(req) {
    const h = req.headers || {};
    if (h['cf-connecting-ip'] || h['x-forwarded-for']) return false;
    const hn = String(h.host || '').split(':')[0];
    return ['127.0.0.1', 'localhost', '::1', 'gateway'].includes(hn) || hn.startsWith('172.');
}
// 面板身份：Basic（管理员）、会话 token、或「无 owner 的旧网关 key」（仅本机直连，管理员兼容）。
// 有 owner 的网关 key 只用于 /v1/*，不能读面板 API（否则用户生成的 key 能看全量配置）。
function identityOf(req, dashOk) {
    const da = CFG.dashboard_auth || {};
    const adminId = (via) => {
        const bu = panelUser(da.username || 'admin');
        if (bu && bu.role !== 'admin') return null;        // Basic 账号被降权后不再给管理员权限
        return { name: da.username || 'admin', role: 'admin', admin: true, via, panel: true };
    };
    if (dashOk) return adminId('basic');
    const su = sessionUser(req);
    if (su) return { name: su.name, role: su.role, admin: su.role === 'admin', via: 'session', panel: true, token: su.token };
    const a = authenticate(req);
    if (!a) return null;
    const k = KEYS.get(a.key) || {};
    const owner = k.owner ? String(k.owner) : '';
    if (!owner) {
        if (!keyPanelAllowed(req)) return null;
        return adminId('key');
    }
    const u = panelUser(owner);
    if (!u) return null;
    return { name: u.name, role: u.role, admin: u.role === 'admin', via: 'key', panel: false, keyName: a.name };
}
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
    if (!cols.includes('pool')) { DB.exec('ALTER TABLE requests ADD COLUMN pool TEXT'); log('migrate: requests + pool'); }
} catch (e) { log('migrate cproto fail', e.message); }
try {   // 迁移：汇总表增加 pool 维度（上游组 / 直接转发分开统计）。老数据一律按「直接转发」归档
    const rcols = DB.prepare('PRAGMA table_info(rollup_hour)').all().map((c) => c.name);
    if (!rcols.includes('pool')) {
        DB.exec(`CREATE TABLE rollup_hour_new(
  hour INTEGER NOT NULL, model TEXT NOT NULL, provider TEXT NOT NULL, key TEXT NOT NULL, pool TEXT NOT NULL DEFAULT '',
  requests INTEGER NOT NULL DEFAULT 0, errors INTEGER NOT NULL DEFAULT 0,
  in_tok INTEGER DEFAULT 0, out_tok INTEGER DEFAULT 0, cached INTEGER DEFAULT 0, reasoning INTEGER DEFAULT 0,
  ms INTEGER DEFAULT 0, ttft INTEGER DEFAULT 0, bytes_in INTEGER DEFAULT 0, bytes_out INTEGER DEFAULT 0, bytes_res INTEGER DEFAULT 0,
  PRIMARY KEY(hour, model, provider, key, pool)) WITHOUT ROWID;
INSERT INTO rollup_hour_new(hour,model,provider,key,pool,requests,errors,in_tok,out_tok,cached,reasoning,ms,ttft,bytes_in,bytes_out,bytes_res)
 SELECT hour,model,provider,key,'',requests,errors,in_tok,out_tok,cached,reasoning,ms,ttft,bytes_in,bytes_out,bytes_res FROM rollup_hour;
DROP TABLE rollup_hour;
ALTER TABLE rollup_hour_new RENAME TO rollup_hour;`);
        log('migrate: rollup_hour + pool');
    }
} catch (e) { log('migrate rollup pool fail', e.message); }
const INS_REQ = DB.prepare(`INSERT INTO requests(ts,key,model,provider,upstream_model,format,status,ms,ttft_ms,stream,passthrough,ip,
 in_tok,out_tok,cached,reasoning,has_tool,reason_inj,req_bytes,res_bytes,up_res_bytes,usage_src,last_evt,error,conv,cproto,pool)
 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`);
const INS_ROLL = DB.prepare(`INSERT INTO rollup_hour(hour,model,provider,key,pool,requests,errors,in_tok,out_tok,cached,reasoning,ms,ttft,bytes_in,bytes_out,bytes_res)
 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
 ON CONFLICT(hour,model,provider,key,pool) DO UPDATE SET
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
        r.usage_src ?? null, r.last_evt ?? null, r.error ?? null, r.conv ?? null, r.cproto ?? null, r.pool || null);
    INS_ROLL.run(Math.floor(ts / HOUR) * HOUR, modelKey(r.model), providerKey(r.provider), keyKey(r.key), String(r.pool || ''),
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
function reqOptions(who) {                    // 筛选下拉的可选项：直接从库里取 DISTINCT（普通用户只看自己 key 的）
    const own = who && !who.admin ? (CFG.keys || []).filter((k) => k.owner === who.name).map((k) => k.name) : null;
    const kf = keyCondSql(own), kArgs = own || [];
    const opt = (col) => DB.prepare(`SELECT DISTINCT ${col} v FROM requests WHERE ${col} IS NOT NULL AND ${col} <> ''${kf} ORDER BY v`).all(...kArgs).map((x) => x.v);
    return {
        models: opt('model'), keys: own || opt('key'), ips: opt('ip'),
        statuses: DB.prepare(`SELECT DISTINCT status s FROM requests WHERE status IS NOT NULL${kf} ORDER BY s`).all(...kArgs).map((x) => x.s),
    };
}
function queryRequests(q, who) {
    const f = parseReqQuery(q || {});
    const where = [], args = [];
    if (who && !who.admin) {                  // 普通用户：只能查自己名下 key 的请求
        const own = (CFG.keys || []).filter((k) => k.owner === who.name).map((k) => k.name);
        if (!own.length) where.push('1 = 0');
        else { where.push(`key IN (${own.map(() => '?').join(',')})`); args.push(...own); }
    }
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
        options: reqOptions(who),
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
// 口径：pool＝走上游组的请求；direct＝没有组、网关直连供应商的请求（历史数据都在 direct 里）
const MODE_COND = (mode) => (mode === 'all' ? `1=1` : mode === 'direct' ? `COALESCE(pool,'') = ''` : `COALESCE(pool,'') <> ''`);
// 分用户口径：只统计该用户名下 key 的记录（普通用户看板用）。
// null/undefined＝不过滤（管理员）；空数组＝该用户还没有 key，必须 1=0（不能退化成全量）。
const keyCondSql = (keys) => (keys == null ? '' : (keys.length ? ` AND key IN (${keys.map(() => '?').join(',')})` : ' AND 1=0'));
const ROLL_SQL = (m, kf = '') => `SELECT model, provider, key, SUM(requests) n, SUM(errors) e, SUM(in_tok) i, SUM(out_tok) o, SUM(cached) c,
  SUM(reasoning) rz, SUM(ms) m, SUM(ttft) t, SUM(bytes_in) bi, SUM(bytes_out) bo, SUM(bytes_res) br
  FROM rollup_hour WHERE hour >= ? AND hour < ? AND ${MODE_COND(m)}${kf} GROUP BY model, provider, key`;
const RAW_SQL = (m, kf = '') => `SELECT COALESCE(NULLIF(model,''), '${MODEL_UNKNOWN}') model, COALESCE(NULLIF(provider,''), '(未路由)') provider,
  COALESCE(NULLIF(key,''), '(未知)') key, COUNT(*) n, SUM(status >= 400) e, SUM(in_tok) i, SUM(out_tok) o, SUM(cached) c,
  SUM(reasoning) rz, SUM(ms) m, SUM(ttft_ms) t, SUM(req_bytes) bi, SUM(up_res_bytes) bo, SUM(res_bytes) br
  FROM requests WHERE ts >= ? AND ts < ? AND ${MODE_COND(m)}${kf} GROUP BY 1, 2, 3`;
const ROLL_SERIES = (m, kf = '') => `SELECT hour h, SUM(requests) n, SUM(in_tok) i, SUM(out_tok) o, SUM(cached) c, SUM(reasoning) rz
  FROM rollup_hour WHERE hour >= ? AND hour < ? AND ${MODE_COND(m)}${kf} GROUP BY hour`;
const RAW_SERIES = (m, kf = '') => `SELECT (ts/3600000)*3600000 h, COUNT(*) n, SUM(in_tok) i, SUM(out_tok) o, SUM(cached) c, SUM(reasoning) rz
  FROM requests WHERE ts >= ? AND ts < ? AND ${MODE_COND(m)}${kf} GROUP BY h`;
// 按去向分色的序列（?dim=model|provider|key）：每桶再按该维度拆开，供看板堆叠柱状图着色。
// 只做「已选中的那一维」，避免一次吐三个维度的数据把响应撑大。
const DIM_EXPR = {
    model: `COALESCE(NULLIF(model,''), '${MODEL_UNKNOWN}')`,
    provider: `COALESCE(NULLIF(provider,''), '(未路由)')`,
    key: `COALESCE(NULLIF(key,''), '(未知)')`,
};
const ROLL_SERIES_DIM = (d, m, kf = '') => `SELECT hour h, ${DIM_EXPR[d]} k, SUM(requests) n, SUM(in_tok) i, SUM(out_tok) o, SUM(cached) c, SUM(reasoning) rz
  FROM rollup_hour WHERE hour >= ? AND hour < ? AND ${MODE_COND(m)}${kf} GROUP BY h, k`;
const RAW_SERIES_DIM = (d, m, kf = '') => `SELECT (ts/3600000)*3600000 h, ${DIM_EXPR[d]} k, COUNT(*) n, SUM(in_tok) i, SUM(out_tok) o, SUM(cached) c, SUM(reasoning) rz
  FROM requests WHERE ts >= ? AND ts < ? AND ${MODE_COND(m)}${kf} GROUP BY h, k`;
// key × 模型 交叉表：用来回答「某个 key 到底在用什么模型」
const ROLL_PAIR = (m, kf = '') => `SELECT key, model, SUM(requests) n, SUM(in_tok) i, SUM(out_tok) o, SUM(cached) c
  FROM rollup_hour WHERE hour >= ? AND hour < ? AND ${MODE_COND(m)}${kf} GROUP BY key, model`;
const RAW_PAIR = (m, kf = '') => `SELECT COALESCE(NULLIF(key,''),'(未知)') key, COALESCE(NULLIF(model,''),'${MODEL_UNKNOWN}') model,
  COUNT(*) n, SUM(in_tok) i, SUM(out_tok) o, SUM(cached) c
  FROM requests WHERE ts >= ? AND ts < ? AND ${MODE_COND(m)}${kf} GROUP BY 1, 2`;
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
function rangeAgg(since, until, tzMin, unitHint, dim, mode, keys) {
    const mm = mode === 'all' ? 'all' : mode === 'direct' ? 'direct' : 'pool';
    const kf = keyCondSql(keys), kArgs = Array.isArray(keys) && keys.length ? keys : [];
    const tzMs = (Number.isFinite(tzMin) ? tzMin : TZ_DEFAULT_MIN) * 60000;
    const sinceTs = Number.isFinite(since) && since > 0 ? since : 0;
    const untilTs = Number.isFinite(until) && until > 0 ? until : Date.now();
    // 不限起点时（「全部」）跨度必须用库里最早一条来算：以前写成 sinceTs || untilTs - HOUR，
    // 于是「全部」的跨度被当成 1 小时 → 误选小时桶
    const firstTs = sinceTs || (DB.prepare('SELECT MIN(ts) m FROM requests').get().m || untilTs - HOUR);
    const b = pickBucket(Math.max(untilTs - firstTs, HOUR), unitHint);
    const totals = emptyTotals(), byModel = {}, byProvider = {}, byKey = {}, series = new Map();
    const byPair = {};                                               // key -> model -> 聚合
    let errors = 0, matched = 0, segs = 0;
    const segList = segments(sinceTs, untilTs);
    segs = segList.length;
    const dimNames = [], dimIdx = new Map(), dimSeries = new Map();   // ?dim= 分色：桶 -> 维度 -> 数值
    for (const seg of segList) {
        for (const v of sqlAgg((seg.raw ? RAW_SQL : ROLL_SQL)(mm, kf), [seg.from, seg.to, ...kArgs])) {
            matched += v.requests; errors += v.errors;
            totals.requests += v.requests; totals.in += v.in; totals.out += v.out; totals.cached += v.cached;
            totals.reasoning += v.reasoning; totals.ms += v.ms; totals.ttft += v.ttft;
            totals.bytes_in += v.bytes_in; totals.bytes_out += v.bytes_out; totals.bytes_res += v.bytes_res;
            addRow(byModel, v.model || MODEL_UNKNOWN, v);
            addRow(byProvider, v.provider || '(未路由)', v);
            addRow(byKey, v.key || '(未知)', v);
        }
        for (const x of DB.prepare((seg.raw ? RAW_SERIES : ROLL_SERIES)(mm, kf)).all(seg.from, seg.to, ...kArgs)) {
            const bk = bucketStart(x.h, b.step, tzMs);
            const cur = series.get(bk) || { ts: bk, requests: 0, in: 0, out: 0, cached: 0, reasoning: 0 };
            cur.requests += x.n || 0; cur.in += x.i || 0; cur.out += x.o || 0; cur.cached += x.c || 0; cur.reasoning += x.rz || 0;
            series.set(bk, cur);
        }
        for (const x of DB.prepare((seg.raw ? RAW_PAIR : ROLL_PAIR)(mm, kf)).all(seg.from, seg.to, ...kArgs)) {
            const k = x.key || '(未知)', m0 = x.model || MODEL_UNKNOWN;
            const row = (byPair[k] || (byPair[k] = {}))[m0] || { requests: 0, in: 0, out: 0, cached: 0 };
            row.requests += x.n || 0; row.in += x.i || 0; row.out += x.o || 0; row.cached += x.c || 0;
            byPair[k][m0] = row;
        }
    }
    if (dim) {
        for (const seg of segList) {
            for (const x of DB.prepare((seg.raw ? RAW_SERIES_DIM : ROLL_SERIES_DIM)(dim, mm, kf)).all(seg.from, seg.to, ...kArgs)) {
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
        since: sinceTs || null, until: untilTs, bucket: b.unit, mode: mm,
        totals: { ...t, hit: t.in ? +(t.cached / t.in * 100).toFixed(1) : 0, avg_ms: t.requests ? Math.round(t.ms / t.requests) : 0, avg_ttft: t.requests ? Math.round(t.ttft / t.requests) : 0, tps: tpsOf(t.out, t.ms, t.ttft) },
        errors, matched, scanned: matched, segs, truncated: false, series: list,
        dim: dim || null, dim_names: dimNames,
        by_model: derived(byModel), by_provider: derived(byProvider), by_key: derived(byKey),
        by_key_model: Object.fromEntries(Object.entries(byPair).map(([k, mmap]) => [k,
            Object.entries(mmap).map(([model, v]) => ({ model, requests: v.requests, in: v.in, out: v.out, cached: v.cached }))
                .sort((a, b2) => b2.requests - a.requests).slice(0, 40)])),
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
// 普通用户的总览：只看自己加入的组（组内全部成员的用量，便于判断共享额度）
function poolUsageAgg(since, until, pools) {
    const out = {};
    if (!pools || !pools.length) return out;
    const s = Number.isFinite(since) && since > 0 ? since : 0;
    const e = Number.isFinite(until) && until > 0 ? until : Date.now();
    const ph = pools.map(() => '?').join(',');
    for (const seg of segments(s, e)) {
        const sql = seg.raw
            ? `SELECT COALESCE(pool,'') p, COALESCE(provider,'') pr, COUNT(*) n, SUM(in_tok) i, SUM(out_tok) o
               FROM requests WHERE ts >= ? AND ts < ? AND pool IN (${ph}) GROUP BY 1, 2`
            : `SELECT COALESCE(pool,'') p, COALESCE(provider,'') pr, SUM(requests) n, SUM(in_tok) i, SUM(out_tok) o
               FROM rollup_hour WHERE hour >= ? AND hour < ? AND pool IN (${ph}) GROUP BY 1, 2`;
        for (const x of DB.prepare(sql).all(seg.from, seg.to, ...pools)) {
            const m = out[x.p] || (out[x.p] = {});
            const cur = m[x.pr || '(未路由)'] || (m[x.pr || '(未路由)'] = { requests: 0, in: 0, out: 0 });
            cur.requests += x.n || 0; cur.in += x.i || 0; cur.out += x.o || 0;
        }
    }
    return out;
}
function statsSnapshot(opts = {}, who = null) {
    const isAdmin = !who || !!who.admin;
    const myKeys = isAdmin ? null : (CFG.keys || []).filter((k) => k.owner === who.name).map((k) => k.name);
    const myPools = isAdmin ? [] : userPools(who.name);
    const mode = isAdmin ? (opts.mode === 'direct' ? 'direct' : 'pool') : 'all';   // 用户口径：不区分 pool/direct，只看自己的 key
    const tzMin = Number.isFinite(opts.tz) ? opts.tz : TZ_DEFAULT_MIN;
    const ranged = (opts.since != null && Number.isFinite(opts.since)) || (opts.until != null && Number.isFinite(opts.until));
    const r = rangeAgg(opts.since, opts.until, tzMin, opts.unit, opts.dim, mode, myKeys);
    // 两种口径各多少条：概览里切到没数据的那一边时可以给出明确提示
    const s0 = Number.isFinite(r.since) ? r.since : 0, s1 = Number.isFinite(r.until) ? r.until : Date.now();
    let counts = { pool: 0, direct: 0 };
    try {
        const kf = keyCondSql(myKeys), kArgs = myKeys || [];
        const c = DB.prepare(`SELECT SUM(CASE WHEN COALESCE(pool,'') <> '' THEN 1 ELSE 0 END) p,
            SUM(CASE WHEN COALESCE(pool,'') = '' THEN 1 ELSE 0 END) d
            FROM requests WHERE ts >= ? AND ts < ?${kf}`).get(s0, s1, ...kArgs) || {};
        counts = { pool: c.p || 0, direct: c.d || 0 };
    } catch { }
    const cfgModels = isAdmin ? (CFG.models || []) : (CFG.models || []).filter((m) => myPools.includes((modelPoolOf(m) || {}).pool || ''));
    const cfgKeys = (CFG.keys || []).filter((k) => isAdmin || (k.owner === who.name)).map((k) => k.name);
    const cfgProviders = isAdmin ? Object.keys(CFG.providers || {})
        : [...new Set(myPools.flatMap((pn) => poolMembers((CFG.pools || {})[pn]).map((m) => m.provider)))];
    return {
        now: Date.now(), uptime_s: Math.round((Date.now() - STARTED) / 1000),
        mode: r.mode, mode_counts: counts,
        total: r.matched, errors: r.errors, totals: r.totals,
        by_model: r.by_model, by_provider: r.by_provider, by_key: r.by_key, by_key_model: r.by_key_model,
        series: r.series, bucket: r.bucket, tz_min: tzMin,
        dim: r.dim, dim_names: r.dim_names,
        range: ranged ? { since: r.since, until: r.until, matched: r.matched, segs: r.segs } : null,
        storage: storageInfo(),
        ...(isAdmin ? {} : { my_pools: myPools, pool_usage: poolUsageAgg(opts.since, opts.until, myPools) }),
        config: {
            enabled: isEnabled(),
            capture: captureInfo(),
            models: cfgModels,
            keys: cfgKeys,
            providers: cfgProviders,
            routes: isAdmin ? (CFG.routes || []).map((r) => `${r.model_regex} -> ${r.pool ? 'pool:' + r.pool : r.provider}/${r.format}`) : [],
            listen: CFG.listen,
            api_base: CFG.api_base || '',
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
    if (!hit) return null;
    // allow：该 key 的模型白名单（config.keys[].models）。null/空 = 不限制，全部模型可用（默认行为）。
    let pools = Array.isArray(hit.pools) && hit.pools.length ? hit.pools : null;
    // 用户生成的 key：动态限定在该用户加入的上游组（用户后来加入/退出组立即生效；不写死 key 上）。
    if (!pools && hit.owner) {
        const u = panelUser(hit.owner);
        if (!u || u.role !== 'admin') {
            const up = userPools(hit.owner);
            pools = up.length ? up : ['__none__'];             // 没加入任何组＝一个模型都不给
        }
    }
    return {
        name: hit.name, session_id: hit.session_id, user_agent: hit.user_agent || '', key, protocol: hit.protocol || '', interop: !!hit.protocol_interop,
        allow: Array.isArray(hit.models) && hit.models.length ? hit.models : null, pools,
        owner: hit.owner || '', role: hit.owner ? (panelUser(hit.owner) || {}).role || 'user' : 'admin',
    };
}
// 客户端 key 的模型白名单判定。白名单为空 = 全部放开（默认）。
// 匹配口径：优先按对外模型名精确比对；其次按「上游实体」（provider + upstream_model）比对，
// 这样客户端用带前缀名（pfx/model-a）或不带前缀的裸名（model-a，走兜底路由）都会被认成同一个模型。
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
// 模型归属哪个上游组：与 routeFor 同口径（按路由顺序取第一条匹配且有效的规则）
function modelPoolOf(model) {
    const m = String(model || '');
    for (const r of CFG.routes || []) {
        let hit = false;
        try { hit = new RegExp(r.model_regex).test(m); } catch { continue; }
        if (!hit) continue;
        if (r.pool) { if ((CFG.pools || {})[r.pool]) return { pool: r.pool, provider: null }; continue; }
        if ((CFG.providers || {})[r.provider]) return { pool: poolForProvider(r.provider), provider: r.provider };
    }
    return null;
}
function modelAllowed(auth, model) {
    if (!auth) return true;
    const m = String(model || '');
    // key 绑定上游组后：只认这些组里的模型（隔离）。空/缺字段＝不限制（旧行为）。
    if (auth.pools) {
        const mp = modelPoolOf(m);
        if (!mp || !mp.pool || !auth.pools.includes(mp.pool)) return false;
    }
    if (!auth.allow) return true;
    if (auth.allow.includes(m)) return true;
    const rt = routeFor(m);
    return !!rt && allowedEntities(auth.allow).has(rt.provider + '|' + rt.upstream_model);
}
// ---------------- 上游池：多上游负载均衡 / 容灾（config.pools）----------------
// 路由用 "pool": "<池名>" 代替 "provider": "<供应商>"。
// 元数据路径（/v1/models、白名单、档位查询）调 routeFor(id) 不带 ctx → 恒取主成员，结果确定、不会每次刷新都变。
// 请求路径传 ctx.stickyKey（客户端 key 的 session_id）→ 由策略决定本次成员，失败还能在成员之间切换。
const POOL_STRATEGIES = ['sticky', 'quota', 'rr', 'least', 'failover'];
// 这些状态码视为「换一家还能成」：限流、过载、上游 5xx、Cloudflare 系错误
const RETRYABLE_STATUS = new Set([408, 409, 425, 429, 500, 502, 503, 504, 520, 521, 522, 523, 524, 529]);
const POOL_HEALTH = new Map();       // "池|供应商" -> { fails, until }（纯内存，重启清零）
const POOL_RR = new Map();           // 池 -> 轮询计数
const POOL_INFLIGHT = new Map();     // "池|供应商" -> 进行中请求数（least 策略用）
const POOL_PIN = new Map();          // "池|客户端key" -> { provider, at }（quota 策略的粘性记忆）

function poolDef(name) { return name ? ((CFG.pools || {})[name] || null) : null; }
// 该供应商现在能提供哪些「上游模型名」：按它自己的前缀路由 + 模型清单推导，例如 pfx/model-a → model-a
function providerCapModels(name) {
    const out = new Set();
    for (const r of CFG.routes || []) {
        if (r.provider !== name || !r.strip) continue;
        for (const m of CFG.models || []) if (String(m).startsWith(r.strip)) out.add(String(m).slice(r.strip.length));
    }
    return out;
}
// 成员能提供的上游模型名集合；返回 null＝不限制（成员显式清单为空且推导不出，保持兼容）
function memberCaps(m) {
    if (Array.isArray(m.models) && m.models.length) return new Set(m.models.map((x) => String(x)));
    const auto = providerCapModels(m.provider);
    return auto.size ? auto : null;
}
// 供应商属于哪个组（唯一命中才算，多个组同时包含同一家 → 不自动接管，避免歧义）
// auto_providers !== false 时，组会接管成员供应商现有路由的模型——组不需要自己的前缀也能用。
function poolForProvider(prov) {
    if (!prov) return null;
    const hits = Object.keys(CFG.pools || {}).filter((n) => {
        const d = CFG.pools[n];
        if (!d || d.auto_providers === false) return false;
        return poolMembers(d).some((m) => m.provider === prov);
    });
    return hits.length === 1 ? hits[0] : null;
}
function poolMembers(def) {
    const out = [];
    for (const m of (def && def.members) || []) {
        const o = typeof m === 'string' ? { provider: m } : (m || {});
        if (!o.provider || !(CFG.providers || {})[o.provider]) continue;   // 成员指到不存在的供应商 → 跳过（不静默报错）
        const w = Number(o.weight);
        out.push({
            provider: o.provider, weight: Number.isFinite(w) && w > 0 ? w : 1, model: o.model ? String(o.model) : '',
            models: Array.isArray(o.models) ? o.models.map((x) => String(x)) : [],
        });
    }
    return out;
}
const poolHealthKey = (pool, provider) => pool + '|' + provider;
function poolUsable(pool, provider) {
    const h = POOL_HEALTH.get(poolHealthKey(pool, provider));
    return !h || !h.until || h.until <= Date.now();
}
// 成功清零；连续失败到阈值 → 进冷却期。冷却期内只要有别的可用成员就不会被挑中，全套挂了则无视冷却硬试。
function poolMark(pool, provider, ok, def) {
    const k = poolHealthKey(pool, provider);
    if (ok) { POOL_HEALTH.delete(k); return; }
    const maxFails = Number(def && def.max_fails) > 0 ? Number(def.max_fails) : 2;
    const cool = Number(def && def.cooldown_sec) > 0 ? Number(def.cooldown_sec) : 60;
    const h = POOL_HEALTH.get(k) || { fails: 0, until: 0 };
    h.fails += 1;
    if (h.fails >= maxFails) { h.fails = 0; h.until = Date.now() + cool * 1000; log('POOL COOLDOWN', pool, provider, `${cool}s`); }
    POOL_HEALTH.set(k, h);
}
function poolInflight(pool, provider, d) {
    const k = poolHealthKey(pool, provider);
    const n = (POOL_INFLIGHT.get(k) || 0) + d;
    if (n > 0) POOL_INFLIGHT.set(k, n); else POOL_INFLIGHT.delete(k);
}
// 记一次成员失败（同一成员只保留最后一次，避免换协议重试把列表撑乱）
function noteAttempt(trail, provider, status, error) {
    const row = { provider, status: status || 0, error: String(error || '').slice(0, 300) };
    const i = trail.findIndex((t) => t.provider === provider);
    if (i >= 0) trail[i] = row; else trail.push(row);
}
// 全挂时的统一错误体：人工可读提示 + 每个成员各自的失败原因（人和客户端都能看懂）。
// `pools.<名>.fail_message` 可自定义提示文本，支持 {n}（成员数）/ {detail}（「名称(状态)」列表）占位。
function poolFailBody(plan, trail, lastTxt) {
    const rows = trail && trail.length ? trail : [];
    const brief = rows.map((t) => `${t.provider}(${t.status ? t.status : '连接失败'})`).join('、') || '未知';
    const tpl = String((plan && plan.def && plan.def.fail_message) || '').trim();
    const hint = tpl
        ? tpl.replace(/\{n\}/g, String(rows.length)).replace(/\{detail\}/g, brief)
        : `上游全部不可用（${rows.length} 个）：${brief}。请稍后重试；若持续如此，请检查这组上游的订阅、额度或网络。`;
    const last = String(lastTxt || (rows[rows.length - 1] || {}).error || '').slice(0, 300).trim();
    return {
        hint,
        message: last ? `${hint}\n最后一次上游错误：${last}` : hint,
        attempts: rows.map((t) => ({ provider: t.provider, status: t.status || 0, error: String(t.error || '').slice(0, 300) })),
    };
}
// 全家桶都挂了该回什么状态码：全是因为同一个 4xx 配置类错误（如三家都 401）→ 如实回 4xx，否则 503（上游不可用）
function poolFailStatus(trail) {
    const codes = (trail || []).map((t) => t.status || 0);
    if (!codes.length) return 503;
    const bad = codes.filter((c) => !(c >= 400 && c < 500 && !RETRYABLE_STATUS.has(c)));
    if (!bad.length && codes.every((c) => c === codes[0])) return codes[0];
    return 503;
}

function fnv32(str) {                          // 稳定哈希（sticky 用；同一 key 永远落到同一成员）
    let h = 2166136261;
    for (let i = 0; i < str.length; i++) { h ^= str.charCodeAt(i); h = Math.imul(h, 16777619); }
    return h >>> 0;
}
function weightedPick(members, slot) {         // 把 [0,total) 的整数 slot 按权重映射到成员下标
    const total = members.reduce((a, m) => a + m.weight, 0);
    let s = ((slot % total) + total) % total;
    for (let i = 0; i < members.length; i++) { s -= members[i].weight; if (s < 0) return i; }
    return members.length - 1;
}
function poolRtOf(r, m, upstreamModel) {
    return {
        provider: m.provider, providerCfg: CFG.providers[m.provider], format: r.format,
        strip: r.strip || '', responses_regex: r.responses_regex || null,
        upstream_model: m.model || upstreamModel, pool: r.pool, weight: m.weight,
    };
}
// ---------------- 成员剩余额度（quota 策略用；两种来源统一成「剩余百分比」）----------------
// official：打供应商自己的 usage 接口（providers.<n>.quota_url；留空时按 config.quota.usage.url 的域名从 base_url 派生）。
//           每个账号周期起点不同没关系——比的是「已用百分比」，各自相对各自的周期。
// local   ：没有额度接口的订阅（火山引擎 plan 之类）→ 用网关自己记的用量除以 providers.<n>.quota.amount，
//           周期默认「月」（quota.cycle 可改 hour/day/week/month，quota.cycle_day 可改每月几号起算）。
//           单位 quota.unit 默认 tokens（最通用），可选 cost（需要价格表收录该模型）或 requests。
// 缓存 60s + 后台定时刷；poolPlan 只读缓存、绝不 await（额度接口慢/挂了不能拖住请求）。
const POOL_QUOTA_TTL_MS = Math.max(15e3, Number(CFG.pool_quota_ttl_sec || 60) * 1000);
const POOL_QUOTA_CACHE = new Map();     // provider -> 统一快照（见 quotaSnapshotOf）
function quotaCfgOf(p) { return (p && typeof p.quota === 'object' && p.quota) ? p.quota : {}; }
function quotaUrlOf(p) {
    if (!p) return '';
    const q = quotaCfgOf(p);
    if (q.url) return String(q.url);
    if (p.quota_url) return String(p.quota_url);
    const b = String(p.base_url || '').replace(/\/+$/, '');
    // 派生：base_url 与 config.quota.usage.url 同源时，用 base_url + '/usage'
    const useUrl = ((CFG.quota || {}).usage || {}).url || '';
    try { if (useUrl && new URL(b).origin === new URL(useUrl).origin) return b + '/usage'; } catch { }
    return '';
}
// 本周期起点（默认月、+08:00 时区、每月 1 号起算）
function quotaCycleStartMs(p) {
    const q = quotaCfgOf(p);
    const unit = ['hour', 'day', 'week', 'month'].includes(q.cycle) ? q.cycle : 'month';
    const tz = (Number.isFinite(Number(q.tz_offset_min)) ? Number(q.tz_offset_min) : 480) * 60000;
    const d = new Date(Date.now() + tz);
    const Y = d.getUTCFullYear(), M = d.getUTCMonth(), D = d.getUTCDate(), H = d.getUTCHours();
    let s;
    if (unit === 'hour') s = Date.UTC(Y, M, D, H);
    else if (unit === 'day') s = Date.UTC(Y, M, D);
    else if (unit === 'week') s = Date.UTC(Y, M, D - ((d.getUTCDay() + 6) % 7));
    else {
        const anchor = Math.min(28, Math.max(1, Number(q.cycle_day) || 1));
        s = (D >= anchor) ? Date.UTC(Y, M, anchor) : Date.UTC(Y, M - 1, anchor);
    }
    return s - tz;
}
function localQuotaSnapshot(name, p) {
    const q = quotaCfgOf(p);
    const amount = Number(q.amount);
    if (!(amount > 0)) return { ok: false, source: 'none', error: '未配置额度（quota.amount 或 quota.url）', at: Date.now() };
    const unit = ['tokens', 'cost', 'requests'].includes(q.unit) ? q.unit : 'tokens';
    const start = quotaCycleStartMs(p);
    let rows;
    try { rows = DB.prepare('SELECT ts, model, in_tok, out_tok, cached FROM requests WHERE provider = ? AND ts >= ?').all(name, start); }
    catch (e) { return { ok: false, source: 'local', error: '查询统计失败: ' + e.message, at: Date.now() }; }
    let used = 0, unpriced = 0;
    for (const r of rows) {
        if (unit === 'requests') { used += 1; continue; }
        if (unit === 'cost') {
            const c = goModelCost(r.model, { in: r.in_tok, out: r.out_tok, cached: r.cached }, r.ts).cost;
            if (c == null) { unpriced++; continue; }
            used += c;
        } else used += (r.in_tok || 0) + (r.out_tok || 0);
    }
    const percent = Math.max(0, Math.min(1000, used / amount * 100));
    return {
        ok: true, source: 'local', at: Date.now(),
        headroom: Math.max(0, Math.min(100, 100 - percent)), percent: +percent.toFixed(2),
        local: { unit, amount, used: +used.toFixed(unit === 'cost' ? 4 : 0), requests: rows.length, cycle: q.cycle || 'month', start, unpriced },
    };
}
async function officialQuotaSnapshot(name, p) {
    const url = quotaUrlOf(p);
    const at = Date.now();
    if (!url) return { ok: false, source: 'none', error: '未配置额度查询（quota.url）', at };
    const key = process.env[p.api_key_env] || '';
    if (!key) return { ok: false, source: 'official', error: `key 未配置（${p.api_key_env}）`, at };
    try {
        const r = await fetch(url, {
            headers: {
                Authorization: 'Bearer ' + key, Accept: 'application/json',
                'User-Agent': p.user_agent || 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            },
            signal: AbortSignal.timeout(12000),
        });
        const t = await r.text();
        if (!r.ok) return { ok: false, source: 'official', error: `HTTP ${r.status}: ${t.slice(0, 120)}`, at };
        const u = (JSON.parse(t).usage) || {};
        const pick = (x) => (x && typeof x === 'object') ? { percent: Number(x.percent || 0), status: x.status || '', resets_at: x.resetsAt || null } : null;
        const windows = { rolling: pick(u.rolling), weekly: pick(u.weekly), monthly: pick(u.monthly) };
        const ps = Object.values(windows).filter(Boolean).map((x) => x.percent).filter((n) => Number.isFinite(n));
        if (!ps.length) return { ok: false, source: 'official', error: '返回里没有可用的 percent', at };
        const pct = Math.max(...ps);
        return { ok: true, source: 'official', at, windows, percent: +pct.toFixed(2), headroom: Math.max(0, Math.min(100, 100 - pct)) };
    } catch (e) {
        return { ok: false, source: 'official', error: String((e && e.message) || e).slice(0, 140), at };
    }
}
async function quotaSnapshotOf(name) {
    const p = (CFG.providers || {})[name] || {};
    const q = quotaCfgOf(p);
    const url = quotaUrlOf(p);
    const mode = q.mode === 'official' || q.mode === 'local' ? q.mode : (url ? 'official' : 'local');
    return mode === 'official' ? officialQuotaSnapshot(name, p) : localQuotaSnapshot(name, p);
}
// quota 策略的池成员才有必要算额度（省得给没在用的人打接口/扫库）
function quotaProviders() {
    const set = new Set();
    for (const p of Object.values(CFG.pools || {})) {
        if (!p || p.strategy !== 'quota') continue;
        for (const m of (p.members || [])) {
            const n = typeof m === 'string' ? m : (m && m.provider);
            if (n && (CFG.providers || {})[n]) set.add(n);
        }
    }
    return [...set];
}
async function refreshQuotas() {
    for (const n of quotaProviders()) {
        const snap = await quotaSnapshotOf(n);
        const old = POOL_QUOTA_CACHE.get(n);
        POOL_QUOTA_CACHE.set(n, snap);
        if (!snap.ok && (!old || old.ok)) log('QUOTA FAIL', n, snap.error);
        else if (snap.ok && snap.source === 'local' && snap.local && snap.local.unpriced) log('QUOTA UNPRICED', n, `${snap.local.unpriced} 条请求的模型不在价格表里`);
    }
}

// 返回 { name, def, strategy, order:[rt...] }：order[0]＝本次首选，后面是失败时可换的成员（已按策略排好序）
function poolPlan(poolName, r, upstreamModel, ctx) {
    const def = poolDef(poolName);
    if (!def) return null;
    const members = poolMembers(def);
    if (!members.length) return null;
    const strategy = POOL_STRATEGIES.includes(def.strategy) ? def.strategy : 'sticky';
    // 只保留「能提供这个上游模型」的成员：某模型只在部分成员里有，就只在它们之间均衡
    const capsOf = members.map((m) => ({ m, caps: memberCaps(m) }));
    const capable = capsOf.filter((x) => !x.caps || x.caps.has(String(upstreamModel))).map((x) => x.m);
    if (!capable.length) return { name: poolName, def, strategy, order: [], upstream_model: String(upstreamModel), reason: 'no-capable-member' };
    const all = capable.map((m) => ({ m, rt: poolRtOf(r, m, upstreamModel) }));
    const live = all.filter((x) => poolUsable(poolName, x.m.provider));
    const usable = live.length ? live : all;         // 全在冷却 → 无视冷却硬试（否则没有任何可用上游）
    const weights = usable.map((x) => x.m);
    let start = 0;
    if (strategy === 'rr') {
        const n = (POOL_RR.get(poolName) || 0) + 1;
        POOL_RR.set(poolName, n);
        start = weightedPick(weights, n - 1);
    } else if (strategy === 'quota') {
        // 按「剩余额度最多」选，但同一个客户端 key 会粘在原来的成员上：
        // 只有当它比最优那家落后超过 margin 个百分点（或它挂了/进冷却）才换 —— 这样不会每个请求都换家，缓存保得住。
        const margin = Number(def.quota_margin) >= 0 ? Number(def.quota_margin) : 2;
        const heads = usable.map((x) => {
            const s = POOL_QUOTA_CACHE.get(x.m.provider);
            return (s && s.ok && Number.isFinite(s.headroom)) ? s.headroom : null;
        });
        const known = heads.filter((h) => h != null).sort((a, b) => a - b);
        const mid = known.length ? known[Math.floor(known.length / 2)] : 50;   // 额度未知 → 按已知中位数（既不吸流量也不拉黑）
        let best = 0, bestH = -Infinity;
        usable.forEach((x, i) => {
            const h = (heads[i] == null ? mid : heads[i]) + Math.min(0.9, x.m.weight / 200);  // 权重只在剩余接近时微调
            if (h > bestH) { bestH = h; best = i; }
        });
        const sk = String((ctx && ctx.stickyKey) || '');
        const pinKey = sk ? poolName + '|' + sk : '';
        const pin = pinKey ? POOL_PIN.get(pinKey) : null;
        const pinIdx = pin ? usable.findIndex((x) => x.m.provider === pin.provider) : -1;
        if (pinIdx >= 0) {
            const curH = heads[pinIdx] == null ? mid : heads[pinIdx];
            start = (curH >= bestH - margin) ? pinIdx : best;   // 还够用就继续用老的
        } else start = best;
        if (pinKey) {
            if (POOL_PIN.size > 5000) POOL_PIN.clear();          // 只是个粘性缓存，别无限涨
            POOL_PIN.set(pinKey, { provider: usable[start].m.provider, at: Date.now() });
        }
    } else if (strategy === 'least') {
        let bestN = Infinity;
        usable.forEach((x, i) => {
            const n = POOL_INFLIGHT.get(poolHealthKey(poolName, x.m.provider)) || 0;
            if (n < bestN) { bestN = n; start = i; }
        });
    } else if (strategy === 'sticky') {
        const sk = String((ctx && ctx.stickyKey) || '');
        if (sk) start = weightedPick(weights, fnv32(poolName + '|' + sk));
        // 没给 sticky 键（元数据/探针/定时任务）→ 主成员，行为可预期
    }
    const order = [];
    for (let i = 0; i < usable.length; i++) order.push(usable[(start + i) % usable.length].rt);
    if (live.length && all.length > live.length) {   // 冷却中的成员排到最后，仅在可用成员全失败后才试
        for (const x of all) if (!live.includes(x)) order.push(x.rt);
    }
    return { name: poolName, def, strategy, order };
}

function routeFor(model, ctx) {
    for (const r of CFG.routes || []) {
        if (!new RegExp(r.model_regex).test(model)) continue;
        const bare = r.strip && model.startsWith(r.strip) ? model.slice(r.strip.length) : model;
        const upstreamModel = (r.model_map || {})[model] || bare;
        if (r.pool) {                                   // 上游池：本次挑一个成员，其余作为失败候选
            const plan = poolPlan(r.pool, r, upstreamModel, ctx);
            if (!plan) continue;                        // 池不存在/成员全无效 → 当作这条路由不匹配
            if (!plan.order.length) return {            // 组里没有能提供该模型的成员
                __poolEmpty: true, pool: r.pool, poolPlan: plan, format: r.format,
                strip: r.strip || '', upstream_model: upstreamModel,
            };
            const rt = plan.order[0];
            rt.poolPlan = plan;                         // 请求路径据此换成员重试
            return rt;
        }
        const p = CFG.providers[r.provider];
        if (!p) continue;
        const autoPool = poolForProvider(r.provider);   // 该供应商被某个组接管 → 走组的均衡/容灾
        if (autoPool) {
            const plan = poolPlan(autoPool, r, upstreamModel, ctx);
            if (plan) {
                if (!plan.order.length) return { __poolEmpty: true, pool: autoPool, poolPlan: plan, format: r.format, strip: r.strip || '', upstream_model: upstreamModel };
                const art = plan.order[0];
                art.poolPlan = plan;
                return art;
            }
        }
        return {
            provider: r.provider, providerCfg: p, format: r.format,
            strip: r.strip || '', responses_regex: r.responses_regex || null,
            upstream_model: upstreamModel,
        };
    }
    return null;
}
// ---- 上游会话身份 & 替代 UA（2026-10-30） ----
// 缺会话头 → 有的上游直接 400 MissingSessionID；UA 不是浏览器式 → 前置 WAF 会回 403 error 1010。
// 约定：需要会话头的上游把 provider 的 `x-upstream-session` 写成 `__SESSION__`，运行时解析成「该客户端 key 的 session_id」，
// 所以会话身份天然与 key 绑定：新建 key 自动生效，不用逐个供应商填 UUID。
const DEFAULT_ALT_UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36';
// 需要会话头的上游：config.session_hosts 列 host 片段（留空＝不自动补头，全靠供应商显式配）
const isSessionHost = (u) => { const b = String(u || '').toLowerCase(); return (CFG.session_hosts || []).some((h) => h && b.includes(String(h).toLowerCase())); };
function hdrHas(h, name) { const n = String(name).toLowerCase(); return Object.keys(h || {}).some((k) => k.toLowerCase() === n); }
function autoUpstreamHeaders(baseUrl, extra) {
    const h = { ...(extra || {}) };
    if (isSessionHost(baseUrl) && !hdrHas(h, 'x-upstream-session')) h['x-upstream-session'] = '__SESSION__';
    return h;
}
// 启动 / 重载时补齐默认值（幂等，只补缺，不动已有值）：
// - 每个客户端 key 都要有稳定的 session_id（老配置没写过 → 补一个并落盘；绝不能每次请求新生成）
// - config.session_hosts 里的上游都要带 x-upstream-session（__SESSION__ 解析成该 key 的 session_id）与替代 UA
function ensureSessionDefaults(cfg) {
    let changed = false;
    for (const k of cfg.keys || []) {
        if (!k.session_id) { k.session_id = crypto.randomUUID(); changed = true; log('SESSION id auto', k.name, k.session_id); }
    }
    for (const [n, p] of Object.entries(cfg.providers || {})) {
        if (!isSessionHost(p.base_url)) continue;
        const h = autoUpstreamHeaders(p.base_url, p.extra_headers);
        if (JSON.stringify(h) !== JSON.stringify(p.extra_headers || {})) {
            p.extra_headers = h; changed = true; log('SESSION header auto', n, 'x-upstream-session=__SESSION__');
        }
        if (!p.user_agent) { p.user_agent = DEFAULT_ALT_UA; changed = true; log('UA auto', n, '补替代 UA'); }
    }
    return changed;
}
function upstreamHeaders(p, sessionId, accept, format, uaOverride) {
    const h = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + (process.env[p.api_key_env] || ''),
        'Accept': accept,
    };
    const ua = String(uaOverride || '').trim() || p.user_agent;   // key 上配了就用 key 的（更具体），否则用供应商的
    if (ua) h['User-Agent'] = ua;
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
// 先查 model_effort_overrides 里配的自家映射（例如某模型 medium→high、另一个 xhigh→max），再就近降级
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
// ---------------- body 字段屏蔽（providers.<名>.strip_body_fields）----------------
// 客户端把会话/身份标识塞在 JSON body 里时（如 session_id / conversation_id / prompt_cache_key），
// chat 路径与两条直通路径都是「整包转发」，会原样带给上游 → 由该配置按字段名删掉。
// 默认不配＝完全不删（保持旧行为）。结构性字段永不删，配了也忽略（删了就坏请求）。
const PROTECTED_BODY_FIELDS = new Set(['model', 'messages', 'input', 'stream', 'tools', 'system', 'instructions', 'stream_options']);
// 供应商额度配置归一化：{url, mode, unit, amount, cycle, cycle_day, tz_offset_min}
function normQuotaCfg(v) {
    if (v === null || v === undefined || v === '') return null;
    if (typeof v !== 'object' || Array.isArray(v)) throw new Error('quota 必须是对象');
    const out = {};
    if (v.url) { const u = String(v.url).trim(); if (!/^https?:\/\//.test(u)) throw new Error('quota.url 必须以 http:// 或 https:// 开头'); out.url = u; }
    if (v.mode) { const m = String(v.mode).trim(); if (!['auto', 'official', 'local'].includes(m)) throw new Error('quota.mode 只能是 auto / official / local'); out.mode = m; }
    if (v.unit) { const u = String(v.unit).trim(); if (!['tokens', 'cost', 'requests'].includes(u)) throw new Error('quota.unit 只能是 tokens / cost / requests'); out.unit = u; }
    if (v.amount !== undefined && v.amount !== '' && v.amount !== null) { const a = Number(v.amount); if (!(a > 0)) throw new Error('quota.amount 必须大于 0'); out.amount = a; }
    if (v.cycle) { const c = String(v.cycle).trim(); if (!['hour', 'day', 'week', 'month'].includes(c)) throw new Error('quota.cycle 只能是 hour / day / week / month'); out.cycle = c; }
    if (v.cycle_day !== undefined && v.cycle_day !== '' && v.cycle_day !== null) { const d = Number(v.cycle_day); out.cycle_day = Math.min(28, Math.max(1, Number.isFinite(d) ? d : 1)); }
    if (v.tz_offset_min !== undefined && v.tz_offset_min !== '' && v.tz_offset_min !== null) { const t = Number(v.tz_offset_min); out.tz_offset_min = Number.isFinite(t) ? t : 480; }
    return Object.keys(out).length ? out : null;
}
function normStripFields(v) {
    const raw = Array.isArray(v) ? v : String(v == null ? '' : v).split(/[,\s]+/);
    const out = [];
    for (const x of raw) {
        const k = String(x || '').trim();
        if (!k || out.includes(k)) continue;
        if (PROTECTED_BODY_FIELDS.has(k)) throw new Error(`不能屏蔽结构性字段: ${k}（会直接把请求搞坏）`);
        out.push(k);
        if (out.length > 50) throw new Error('屏蔽字段最多 50 个');
    }
    return out;
}
function stripBodyFields(payload, list, label) {
    if (!Array.isArray(list) || !list.length) return payload;
    const hit = [];
    for (const k of list) {
        const key = String(k || '').trim();
        if (!key || PROTECTED_BODY_FIELDS.has(key)) continue;
        if (Object.prototype.hasOwnProperty.call(payload, key)) { delete payload[key]; hit.push(key); }
    }
    if (hit.length) log('STRIP body', label || '', hit.join(','));
    return payload;
}

function buildUpstream(rt, format, body, sessionId, ctx, ua) {
    body = clampBodyEffort(body, rt);              // 客户端给了不支持的档位 → 就近降级（没配置过＝不动）
    const headers = upstreamHeaders(rt.providerCfg, sessionId, body.stream ? 'text/event-stream' : 'application/json', format, ua);
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
    stripBodyFields(payload, rt.providerCfg.strip_body_fields, rt.provider);
    // 实测某上游 /chat/completions 不认 OpenAI 新式的 role:"developer"，会回 422 "Endpoint is unavailable."
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
    // 推理档位 → messages 端点。形状因上游而异：实测某上游只认 thinking.type=adaptive + output_config.effort，
    // 发 thinking.type=enabled 直接 400。所以按 provider.thinking_style 走，缺省 none＝完全不发（宁可不带，也不 400）。
    if (body.thinking && typeof body.thinking === 'object') payload.thinking = body.thinking;
    if (body.output_config && typeof body.output_config === 'object') payload.output_config = body.output_config;
    const style = (providerCfg || {}).thinking_style || 'none';
    const eff = body.reasoning_effort;
    if (eff != null && style !== 'none') {
        if (style === 'adaptive') {                      // 实测形状（adaptive + output_config.effort）
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
// 用途：部分上游的 Google 系模型只认原生 Google 协议（/chat/completions、/responses、/messages 全回
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
    base.pool = (rt && rt.poolPlan && rt.poolPlan.name) || '';   // 记这条请求走的是哪个上游组（空＝网关直连供应商）
    countOut(res, base);
    capture('client_req', { cproto: clientProto, ip, key: auth.name, model, stream: !!body.stream, body });
    const ctx = { id: 'chatcmpl-' + crypto.randomBytes(10).toString('hex'), created: Math.floor(t0 / 1000), model, t0, provider: rt.provider, usage: {}, hasTool: false, ttft: null, reasonPending: [], reasonInj: 0 };
    const sink = clientProto === 'chat' ? chatSink(res, ctx) : clientProto === 'responses' ? responsesSink(res, ctx) : messagesSink(res, ctx);
    let status = 200, format = resolveFormat(rt), piped = false;
    const hold = startFirstByteHold(res, !!body.stream);
    let hitRt = rt;                                   // 实际命中的成员（池模式下可能已换过，记录/日志都用它）
    const trail = [];                                 // 池模式下每个成员的失败原因（全挂时回给客户端）
    try {
        const ac = new AbortController();
        const onClose = () => { if (!res.writableEnded) { try { ac.abort(); } catch { } } };
        req.on('close', onClose);
        let upRes, lastTxt = '', netErr = null;
        // 候选顺序：池模式下是「策略选中的成员 + 失败可换的其它成员」，每人对自己的协议候选各试一遍。
        const plan = rt.poolPlan || null;
        const attempts = plan ? plan.order : [rt];
        for (let ai = 0; ai < attempts.length; ai++) {
            const aRt = attempts[ai];
            if (plan) { ctx.provider = aRt.provider; poolInflight(plan.name, aRt.provider, +1); }
            let gotOk = false, switchMember = false;
            try {
                const cands = protocolCandidates(aRt);
                let triedN = 0;
                for (const cand of cands) {
                    triedN++;
                    format = cand;
                    const { url, payload, headers } = buildUpstream(aRt, format, chatBody, auth.session_id, ctx, auth.user_agent);
                    capture('upstream_req', { candidate: cand, cproto: clientProto, url, key: auth.name, model, upstream_model: aRt.upstream_model, provider: aRt.provider, payload });
                    const payloadStr = jdump(payload);
                    ctx.up_len = payloadStr.length;
                    ctx.pre4k = preHash(payloadStr, 4096); ctx.pre16k = preHash(payloadStr, 16384); ctx.pre64k = preHash(payloadStr, 65536);
                    try {
                        upRes = await fetch(url, { method: 'POST', headers, body: payloadStr, signal: ac.signal });
                    } catch (fe) {
                        if (fe.name === 'AbortError') throw fe;       // 客户端断开：不重试
                        netErr = fe; upRes = null; status = 0; lastTxt = 'fetch failed: ' + fe.message;
                        noteAttempt(trail, aRt.provider, 0, fe.message);
                        log('UPSTREAM FETCH FAIL', aRt.provider, format, aRt.upstream_model, fe.message);
                        switchMember = ai < attempts.length - 1;
                        break;
                    }
                    capture('upstream_status', { candidate: cand, provider: aRt.provider, status: upRes.status, ok: upRes.ok });
                    if (upRes.ok) { gotOk = true; break; }
                    lastTxt = await upRes.text(); status = upRes.status;
                    noteAttempt(trail, aRt.provider, status, lastTxt);
                    const auto = !aRt.format || aRt.format === 'auto';
                    if (auto && protocolMismatch(status, lastTxt)) { log('PROTOCOL SWITCH', aRt.provider, aRt.upstream_model, `${format} 不支持, 试下一个`, `(${status})`); continue; }
                    // 限流/过载/上游 5xx：同一家换协议也没用，直接换下一个成员（还有人就换）
                    if (RETRYABLE_STATUS.has(status) && ai < attempts.length - 1) { switchMember = true; break; }
                    break;
                }
                // 四种协议全被这家拒（说明该模型在它家不可用）→ 换下一个成员再试
                if (!gotOk && triedN >= cands.length && plan && ai < attempts.length - 1) switchMember = true;
            } finally {
                if (plan) poolInflight(plan.name, aRt.provider, -1);
            }
            if (gotOk) { hitRt = aRt; if (plan) { poolMark(plan.name, aRt.provider, true, plan.def); log('POOL HIT', plan.name, aRt.provider, model); } break; }
            if (plan) poolMark(plan.name, aRt.provider, false, plan.def);
            if (!switchMember) break;                 // 不可重试 → 就地按错误收尾
            log('POOL FAILOVER', plan.name, aRt.provider, '->', attempts[ai + 1].provider, `(${status || 'network'})`);
        }
        req.off('close', onClose);
        if (upRes && upRes.ok) { const fk = hitRt.provider + '|' + hitRt.upstream_model; if ((!hitRt.format || hitRt.format === 'auto') && FORMAT_OVERRIDES[fk] !== format) { FORMAT_OVERRIDES[fk] = format; saveOverrides(); } }
        if (!upRes || !upRes.ok) {
            const pFail = plan ? poolFailBody(plan, trail, lastTxt) : null;
            const outStatus = plan ? poolFailStatus(trail) : (upRes ? status : 502);
            let parsed = null; try { parsed = JSON.parse(lastTxt); } catch { }
            log('UPSTREAM ERROR', outStatus, hitRt.provider, format, model, String(pFail ? pFail.hint : lastTxt).slice(0, 300));
            if (hold.sent) {   // 保活已提前发出 200 响应头 → 改发 SSE 错误事件收尾
                record({ ...base, status: outStatus, ms: Date.now() - t0, provider: hitRt.provider, upstream_model: hitRt.upstream_model, format, error: String(pFail ? pFail.message : lastTxt).slice(0, 500) });
                log('HOLD-ERROR', `上游 ${outStatus} 且已提前开流，改发 SSE 错误事件`);
                sink.error(pFail ? pFail.message : (parsed?.error?.message || lastTxt.slice(0, 800) || 'upstream error'));
                sink.finalize('stop', {});
                return res.end();
            }
            record({ ...base, status: outStatus, ms: Date.now() - t0, provider: hitRt.provider, upstream_model: hitRt.upstream_model, format, error: String(pFail ? pFail.message : lastTxt).slice(0, 500) });
            const errBody = pFail
                ? { error: { message: pFail.message, type: 'upstream_pool_exhausted', code: 'upstream_unavailable', hint: pFail.hint, attempts: pFail.attempts } }
                : parsed?.error ? { error: parsed.error } : { error: { message: lastTxt.slice(0, 800) || 'upstream error', type: 'upstream_error' } };
            return sendJson(res, outStatus, errBody);
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
        record({ ...base, status, ms: Date.now() - t0, provider: hitRt.provider, upstream_model: hitRt.upstream_model, format, usage: ctx.usage, ttft_ms: ctx.ttft || 0, has_tool: ctx.hasTool, reason_inj: ctx.reasonInj || 0, up_len: ctx.up_len, up_res_bytes: ctx.up_res_bytes || 0, pre4k: ctx.pre4k, pre16k: ctx.pre16k, pre64k: ctx.pre64k });
        log('OK', clientProto + '->' + format, hitRt.upstream_model, `in=${ctx.usage.in || 0} out=${ctx.usage.out || 0} cached=${ctx.usage.cached || 0}`, `${Date.now() - t0}ms`);
    } catch (e) {
        hold.stop();
        if (e.name === 'AbortError') {
            log('CLIENT ABORT', model, `${Date.now() - t0}ms`);
            record({ ...base, status: 499, ms: Date.now() - t0, provider: hitRt.provider, error: 'client aborted' });
            try { res.end(); } catch { }
            return;
        }
        log('GATEWAY ERROR', e.message);
        record({ ...base, status: 500, ms: Date.now() - t0, provider: hitRt.provider, error: e.message });
        if (!res.headersSent) sendJson(res, 500, { error: { message: e.message, type: 'gateway_error' } });
        else try { res.end(); } catch { }
    }
}

// ---------------- 拉取上游模型列表 ----------------
async function fetchModelsFrom(base, key, extra = {}) {
    base = String(base || '').replace(/\/+$/, '');
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
    return res.ids;
}
async function fetchProviderModels(b) {
    const prefix = String(b.prefix || '').trim().replace(/\/+$/, '');
    if (!prefix) throw new Error('缺少前缀');
    const route = (CFG.routes || []).find((r) => r.strip === prefix + '/' && r.provider);
    if (!route) throw new Error(`没有前缀为 ${prefix} 的供应商（上游池路由不支持拉取，请直接填上游模型名）`);
    const prov = (CFG.providers || {})[route.provider];
    if (!prov) throw new Error('供应商配置缺失');
    const key = process.env[prov.api_key_env] || '';
    const base = String(prov.base_url || '').replace(/\/+$/, '');
    const extra = {};
    if (prov.user_agent) extra['User-Agent'] = prov.user_agent;
    for (const [k, v] of Object.entries(prov.extra_headers || {})) extra[k] = v;
    const ids = await fetchModelsFrom(base, key, extra);
    const have = new Set((CFG.models || []).filter((m) => m.startsWith(prefix + '/')));
    return {
        provider: route.provider, prefix, endpoint: base + '/models', count: ids.length,
        models: ids.map((id) => ({ id, added: have.has(prefix + '/' + id) })),
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
function configView(who) {
    const me = who || null;
    const isAdmin = !me || !!me.admin;
    const mine = me ? me.name : '';
    const joined = !isAdmin ? userPools(mine) : [];
    const visiblePools = isAdmin ? null : new Set([...joined, ...joinablePools()]);
    const providers = Object.entries(CFG.providers || {})
        .filter(([, p]) => isAdmin || (p && p.owner === mine))
        .map(([name, p]) => {
            const route = (CFG.routes || []).find((r) => r.provider === name && r.strip);
            return {
                name,
                base_url: p.base_url,
                api_key_env: p.api_key_env,
                has_key: !!(process.env[p.api_key_env] || '').length,
                user_agent: !!p.user_agent,
                reasoning_echo: !!p.reasoning_echo,
                strip_body_fields: Array.isArray(p.strip_body_fields) ? p.strip_body_fields.length : 0,
                extra_headers: Object.keys(p.extra_headers || {}),
                prefix: route ? route.strip.replace(/\/$/, '') : '',
                models: (CFG.models || []).filter((m) => route && m.startsWith(route.strip)).length,
                test: providerIsTest(p),                       // 是否测试供应商（显式标记优先，否则按 base_url 推断）
                test_explicit: p.test !== undefined,           // 是否已显式写进 config.json
                efforts: normEfforts((CFG.model_efforts || {})[effortKeyOf(name, '*')]),   // 该供应商的默认推理档位（模型级没配时生效）
                owner: p.owner || '',
                mine: !!p.owner && p.owner === mine,
            };
        });
    const MT = {};                                            // 每个模型的累计量（模型列表页显示用）
    for (const x of DB.prepare('SELECT model, SUM(requests) n, SUM(in_tok) i, SUM(out_tok) o, SUM(cached) c FROM rollup_hour GROUP BY model').all())
        MT[x.model] = { requests: x.n, in: x.i || 0, out: x.o || 0, cached: x.c || 0 };
    const models = (CFG.models || []).filter((id) => {
        if (isAdmin) return true;
        return joined.includes((modelPoolOf(id) || {}).pool || '');   // 普通用户只看到自己加入的组里的模型
    }).map((id) => {
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
            ...(isAdmin ? {} : { pool: (modelPoolOf(id) || {}).pool || '' }),
        };
    });
    // 普通用户拿到的路由只覆盖「可见组里的成员供应商」，不泄露其它上游配置
    const poolProv = new Set();
    if (!isAdmin) for (const pn of visiblePools) for (const m of poolMembers((CFG.pools || {})[pn])) poolProv.add(m.provider);
    return {
        enabled: isEnabled(),
        me: me ? { name: me.name, role: me.role, via: me.via, admin: isAdmin, pools: isAdmin ? Object.keys(CFG.pools || {}) : joined, joinable: isAdmin ? [] : joinablePools() } : null,
        providers,
        routes: (CFG.routes || []).filter((r) => isAdmin || (r.provider && poolProv.has(r.provider) && !r.pool))
            .map((r) => ({ model_regex: r.model_regex, provider: r.provider || null, pool: r.pool || null, strip: r.strip || '', format: r.format || 'auto' })),
        pools: Object.entries(CFG.pools || {}).filter(([n]) => isAdmin || (visiblePools && visiblePools.has(n))).map(([name, p]) => ({
            name,
            strategy: POOL_STRATEGIES.includes(p && p.strategy) ? p.strategy : 'sticky',
            cooldown_sec: Number(p && p.cooldown_sec) > 0 ? Number(p.cooldown_sec) : 60,
            max_fails: Number(p && p.max_fails) > 0 ? Number(p.max_fails) : 2,
            quota_margin: Number(p && p.quota_margin) >= 0 ? Number(p.quota_margin) : 2,
            fail_message: String((p && p.fail_message) || ''),
            auto_providers: (p && p.auto_providers) !== false,
            user_join: (p && p.user_join) === true,
            joined: isAdmin ? true : joined.includes(name),
            members: poolMembers(p).map((m) => ({ ...m, mine: !!me && ((CFG.providers || {})[m.provider] || {}).owner === mine })),
            routes: isAdmin ? (CFG.routes || []).filter((r) => r.pool === name).map((r) => r.model_regex) : [],
        })),
        models,
        keys: (CFG.keys || []).filter((k) => isAdmin || (k.owner && k.owner === mine))
            .map((k) => ({ name: k.name, owner: k.owner || '', protocol: k.protocol || '', interop: !!k.protocol_interop, models: Array.isArray(k.models) ? k.models : [], pools: Array.isArray(k.pools) ? k.pools : [], session_id: k.session_id || '', user_agent: k.user_agent || '' })),
        formats: Object.keys(CFG.model_formats || {}).length,
        listen: CFG.listen,
        api_base: CFG.api_base || '',
        dashboard_hosts: (CFG.dashboard_auth || {}).hosts || [],
        ...(isAdmin ? { users: panelUserList() } : {}),
    };
}
// 客户端 key 的模型白名单校验：去空、去重、必须存在于对外模型清单。
// 返回 [] 表示「全部模型」（等于不限制）；传 null/undefined 也当 []。
// 客户端 key 的上游组绑定校验：去空、去重、必须存在于 config.pools。
function normKeyPools(cfg, input) {
    if (input === null || input === undefined) return [];
    if (!Array.isArray(input)) throw new Error('pools 必须是上游组名数组（空数组＝不绑定）');
    const out = [];
    for (const raw of input) {
        const n = String(raw || '').trim();
        if (!n) continue;
        if (!(cfg.pools || {})[n]) throw new Error('上游组不存在: ' + n);
        if (!out.includes(n)) out.push(n);
    }
    return out;
}
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
// 删掉一个供应商在配置里的全部痕迹（路由/模型/组成员/协议记忆/环境变量）。用户自助退出组时复用。
function dropProviderFromCfg(cfg, name) {
    const prov = (cfg.providers || {})[name];
    if (!prov) return { routes: 0, models: 0 };
    const rt = (cfg.routes || []).find((r) => r.provider === name && r.strip);
    const strip = rt ? String(rt.strip) : '';
    const routesBefore = (cfg.routes || []).length;
    cfg.routes = (cfg.routes || []).filter((r) => r.provider !== name);
    let models = 0;
    if (strip) {
        const before = (cfg.models || []).length;
        cfg.models = (cfg.models || []).filter((m) => !String(m).startsWith(strip));
        models = before - cfg.models.length;
    }
    for (const p of Object.values(cfg.pools || {})) {
        if (!p || !Array.isArray(p.members)) continue;
        p.members = p.members.filter((m) => (typeof m === 'string' ? m : (m && m.provider)) !== name);
    }
    for (const key of ['model_formats', 'model_efforts', 'model_effort_overrides', 'effort_probes']) {
        for (const k of Object.keys(cfg[key] || {})) if (k.startsWith(name + '|')) delete cfg[key][k];
    }
    delete cfg.providers[name];
    const still = Object.values(cfg.providers || {}).some((pr) => pr && pr.api_key_env === prov.api_key_env);
    if (!still && prov.api_key_env) { try { removeEnvVar(prov.api_key_env); } catch { } }
    return { routes: routesBefore - (cfg.routes || []).length, models };
}
function normUpstreamModels(input) {
    const arr = Array.isArray(input) ? input : String(input || '').split(/[,，\s]+/);
    return [...new Set(arr.map((x) => String(x).trim()).filter((x) => /^[A-Za-z0-9][A-Za-z0-9._:\/-]{0,119}$/.test(x)))].slice(0, 100);
}
function applyConfigChange(p, b, who) {
    const cfg = JSON.parse(fs.readFileSync(CONFIG_PATH, 'utf8'));
    const me = who || { admin: true, name: (CFG.dashboard_auth || {}).username || 'admin', role: 'admin' };
    const isAdmin = !!me.admin;
    const membersOf = (def, c) => (((def && def.members) || []).map((m) => (typeof m === 'string' ? { provider: m } : (m || {}))))
        .filter((m) => m.provider && (c.providers || {})[m.provider]);
    // 管理员专属：组管理、供应商/模型管理、总开关、用户管理。普通用户走 /api/user/* 与自己的 key。
    const ADMIN_ONLY = new Set([
        '/api/config/pool', '/api/config/pool/delete',
        '/api/config/provider', '/api/config/provider/update', '/api/config/provider/delete',
        '/api/config/model', '/api/config/models/bulk', '/api/config/model/delete',
        '/api/config/model/efforts', '/api/config/provider/efforts',
        '/api/config/toggle', '/api/config/toggle_capture', '/api/config/reload',
        '/api/config/user', '/api/config/user/delete',
    ]);
    if (!isAdmin && ADMIN_ONLY.has(p)) throw new Error('只有管理员能执行该操作');
    const keyOf = (name) => {
        const k = (cfg.keys || []).find((x) => x.name === String(name || '').trim());
        if (!k) throw new Error('key 不存在: ' + name);
        if (!isAdmin && k.owner !== me.name) throw new Error('无权操作别人的 key: ' + name);
        return k;
    };
    const prefixRe = (pf) => '^' + pf + '/';
    // ---------------- 上游组（多上游负载均衡） ----------------
    if (p === '/api/config/pool') {
        const name = String(b.name || '').trim();
        if (!/^[a-z0-9][a-z0-9_-]{1,30}$/i.test(name)) throw new Error('组名只能字母数字-_（2–31 字符）');
        const members = [];
        for (const m of (Array.isArray(b.members) ? b.members : [])) {
            const provider = String((m && m.provider) || '').trim();
            if (!provider) continue;
            if (!cfg.providers || !cfg.providers[provider]) throw new Error('供应商不存在: ' + provider);
            if (members.some((x) => x.provider === provider)) throw new Error('同一家供应商在一个组里只能出现一次: ' + provider);
            const w = Number(m.weight);
            const model = m.model ? String(m.model).trim() : '';
            // 成员可用模型清单（上游模型名，可选）；留空＝按该供应商前缀路由自动推导
            const rawCap = Array.isArray(m.models) ? m.models : String(m.models || '').split(/[,，\s]+/);
            const caps = [...new Set(rawCap.map((x) => String(x).trim()).filter(Boolean))].slice(0, 200);
            members.push({ provider, weight: Number.isFinite(w) && w > 0 ? Math.min(100, Math.round(w)) : 1, ...(model ? { model } : {}), ...(caps.length ? { models: caps } : {}) });
        }
        const strategy = POOL_STRATEGIES.includes(b.strategy) ? b.strategy : 'sticky';
        const cool = Math.min(3600, Math.max(1, Math.round(Number(b.cooldown_sec) || 60)));
        const maxFails = Math.min(20, Math.max(1, Math.round(Number(b.max_fails) || 2)));
        const msg = String(b.fail_message || '').trim().slice(0, 300);
        cfg.pools = cfg.pools || {};
        const existed = !!cfg.pools[name];
        cfg.pools[name] = {
            strategy, cooldown_sec: cool, max_fails: maxFails,
            ...(strategy === 'quota' ? { quota_margin: Math.min(50, Math.max(0, Number.isFinite(Number(b.quota_margin)) ? Number(b.quota_margin) : 2)) } : {}),
            ...(msg ? { fail_message: msg } : {}),
            ...(b.auto_providers === false ? { auto_providers: false } : {}),
            ...(b.user_join === true ? { user_join: true } : {}),
            members,
        };
        // 组不再挂路由：成员供应商的现有路由会被自动接管（同名模型直接进均衡，不需要指定供应商）
        saveConfig(cfg); reloadConfig();
        log('CONFIG pool', name, `${existed ? 'update' : 'create'} members=${members.length}`);
        return { pool: name, existed, members: members.length };
    }
    if (p === '/api/config/pool/delete') {
        const name = String(b.name || '').trim();
        if (!cfg.pools || !cfg.pools[name]) throw new Error('上游组不存在: ' + name);
        const bound = (cfg.routes || []).filter((r) => r.pool === name);
        if (bound.length && !b.force) {
            throw new Error(`组 ${name} 还绑着 ${bound.length} 条路由（${bound.map((r) => r.model_regex).join(', ')}）——确认要连路由一起删就勾选强制删除`);
        }
        cfg.routes = (cfg.routes || []).filter((r) => r.pool !== name);
        let unbound = 0;                                   // 顺手把 key 上的这个组解绑，避免留下悬空引用
        for (const k of (cfg.keys || [])) {
            if (!Array.isArray(k.pools) || !k.pools.includes(name)) continue;
            const left = k.pools.filter((x) => x !== name);
            if (left.length) k.pools = left; else delete k.pools;
            unbound++;
        }
        delete cfg.pools[name];
        saveConfig(cfg); reloadConfig();
        log('CONFIG pool delete', name, `routes=${bound.length} keys_unbound=${unbound}`);
        return { removed_pool: name, removed_routes: bound.length, keys_unbound: unbound };
    }
    if (p === '/api/config/route/pool') {          // 已废弃：组不再挂路由（保留了明确的报错，避免老脚本静默失效）
        throw new Error('组的「绑定路由」功能已移除：组的模型 = 成员供应商现有模型的并集，同名模型自动进均衡');
    }
    if (p === '/api/config/provider') {
        const name = String(b.name || '').trim();
        if (!/^[a-z0-9][a-z0-9_-]{1,30}$/i.test(name)) throw new Error('供应商名只能字母数字-_（2–31 字符）');
        if (!/^https?:\/\//.test(String(b.base_url || ''))) throw new Error('base_url 必须以 http:// 或 https:// 开头');
        const envName = String(b.api_key_env || (name.toUpperCase().replace(/[^A-Z0-9]/g, '_') + '_API_KEY')).trim();
        cfg.providers = cfg.providers || {};
        const stripList = normStripFields(b.strip_body_fields);
        const qUrl = String(b.quota_url || '').trim();
        if (qUrl && !/^https?:\/\//.test(qUrl)) throw new Error('额度查询 URL 必须以 http:// 或 https:// 开头');
        const qCfg = normQuotaCfg(b.quota);
        const pUrl = String(b.base_url).replace(/\/+$/, '');
        const pExtra = autoUpstreamHeaders(pUrl, b.extra_headers);
        const pUA = b.user_agent ? String(b.user_agent) : (isSessionHost(pUrl) ? DEFAULT_ALT_UA : '');
        cfg.providers[name] = {
            base_url: pUrl,
            api_key_env: envName,
            ...(pUA ? { user_agent: pUA } : {}),
            ...(Object.keys(pExtra).length ? { extra_headers: pExtra } : {}),
            ...(b.reasoning_echo ? { reasoning_echo: true } : {}),
            ...(stripList.length ? { strip_body_fields: stripList } : {}),
            ...(qUrl ? { quota_url: qUrl } : {}),
            ...(qCfg ? { quota: qCfg } : {}),
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
        if (b.strip_body_fields !== undefined) {          // body 字段屏蔽（逗号串或数组，留空＝清除）
            const next = normStripFields(b.strip_body_fields);
            const cur = Array.isArray(prov.strip_body_fields) ? prov.strip_body_fields : [];
            if (jdump(next) !== jdump(cur)) {
                if (next.length) { prov.strip_body_fields = next; changed.push('strip_body_fields:' + next.join(',')); }
                else { delete prov.strip_body_fields; changed.push('strip_body_fields(已清除)'); }
            }
        }
        if (b.quota_url !== undefined) {                   // 额度查询地址（留空＝按 config.quota.usage.url 的域名从 base_url 派生；其它上游必须显式填）
            const u = String(b.quota_url).trim();
            if (u && !/^https?:\/\//.test(u)) throw new Error('额度查询 URL 必须以 http:// 或 https:// 开头');
            if (u !== String(prov.quota_url || '')) {
                if (u) { prov.quota_url = u; changed.push('quota_url'); }
                else { delete prov.quota_url; changed.push('quota_url(已清除)'); }
            }
        }
        if (b.quota !== undefined) {                       // 额度配置（official 官方接口 / local 本地统计）
            const nq = normQuotaCfg(b.quota);
            if (jdump(nq) !== jdump(prov.quota || null)) {
                if (nq) { prov.quota = nq; changed.push('quota'); }
                else { delete prov.quota; changed.push('quota(已清除)'); }
            }
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
        // 上游组成员先摘掉再用：直接删会让池里少一个人，甚至变空组（路由全挂），必须显式处理
        const inPools = Object.entries(cfg.pools || {})
            .filter(([, pp]) => (pp && Array.isArray(pp.members) ? pp.members : []).some((m) => (typeof m === 'string' ? m : m && m.provider) === name))
            .map(([pn]) => pn);
        if (inPools.length) throw new Error(`供应商 ${name} 还在上游组 ${inPools.join('、')} 里，请先把它从这些组里移除（或删掉该组）再删供应商`);
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
        let models = normKeyModels(b.models);              // 空＝全部模型（默认）
        let pools = normKeyPools(cfg, b.pools);            // 空＝不绑定上游组（默认）
        if (!isAdmin) {                                    // 普通用户：只能给自己建，且限定在自己加入的组
            const up = userPools(me.name);
            if (!up.length) throw new Error('你还没有加入任何上游组：先到「上游组」页把自己的供应商加进一个组');
            if (models.length) {
                const allowed = new Set((cfg.models || []).filter((m) => up.includes((modelPoolOf(m) || {}).pool || '')));
                const bad = models.filter((m) => !allowed.has(m));
                if (bad.length) throw new Error('这些模型不在你加入的组里: ' + bad.join(', '));
            }
            pools = [];                                    // 不写死组：鉴权时按 owner 动态限定（加入/退出组立即生效）
        }
        // 会话 id 默认生成并与 key 绑定：既是上游 prompt 缓存的命名空间（供应商里写 __SESSION__ 时解析成它），
        // 也是上游组负载均衡的粘性指纹（sticky / quota 策略的 stickyKey）。UA 同步给一个替代值，防 CF 1010。
        const kUa = b.user_agent === undefined ? DEFAULT_ALT_UA : String(b.user_agent || '').trim();
        const kObj = {
            name, key, session_id: crypto.randomUUID(),
            ...(kUa ? { user_agent: kUa } : {}),
            ...(models.length ? { models } : {}), ...(pools.length ? { pools } : {}),
            ...(isAdmin ? {} : { owner: me.name }),
        };
        cfg.keys = cfg.keys || [];
        cfg.keys.push(kObj);
        saveConfig(cfg); reloadConfig();
        return { name, key, models, pools, owner: isAdmin ? '' : me.name, session_id: kObj.session_id, user_agent: kObj.user_agent || '' };
    }
    if (p === '/api/config/key/pools') {                 // 该 key 绑定哪些上游组（空数组＝不绑定）
        if (!isAdmin) throw new Error('普通用户的 key 自动跟随自己加入的上游组，不能单独绑定');
        const k = keyOf(b.name);
        const pools = normKeyPools(cfg, b.pools);
        if (pools.length) k.pools = pools; else delete k.pools;
        saveConfig(cfg); reloadConfig();
        log('CONFIG key pools', k.name, pools.join(',') || '(全部)');
        return { key: k.name, pools };
    }
    if (p === '/api/config/key/models') {              // 该 key 可访问的模型白名单：空数组/省略＝全部（默认）
        const k = keyOf(b.name);
        const models = normKeyModels(b.models);
        if (!isAdmin && models.length) {                 // 普通用户只能在自己加入的组里挑
            const up = userPools(me.name);
            const bad = models.filter((m) => !up.includes((modelPoolOf(m) || {}).pool || ''));
            if (bad.length) throw new Error('这些模型不在你加入的组里: ' + bad.join(', '));
        }
        if (models.length) k.models = models; else delete k.models;
        saveConfig(cfg); reloadConfig();
        log('CONFIG key models', k.name, models.length ? `${models.length} 个` : '全部');
        return { key: k.name, models, config: configView(who) };
    }
    if (p === '/api/config/key/delete') {
        const k = keyOf(b.name);
        const rest = (cfg.keys || []).filter((x) => x.name !== k.name);
        if (!rest.length) throw new Error('这是最后一个 key，删掉就没人能用了');
        cfg.keys = rest;
        saveConfig(cfg); reloadConfig();
        return { removed_key: k.name, left: rest.length };
    }
    if (p === '/api/config/key/session') {            // 该 key 的上游会话身份：session_id（与 key 绑定，供应商用 __SESSION__ 时生效）+ 替代 UA
        const k = keyOf(b.name);
        if (!k.session_id) k.session_id = crypto.randomUUID();     // 老 key 补一个（稳定，别每次请求新生成）
        if (b.user_agent !== undefined) {
            const ua = String(b.user_agent || '').trim();
            if (ua) k.user_agent = ua; else delete k.user_agent;
        }
        saveConfig(cfg); reloadConfig();
        log('CONFIG key session', k.name, k.session_id, k.user_agent ? 'UA=' + k.user_agent.slice(0, 40) : 'UA=(供应商默认)');
        return { key: k.name, session_id: k.session_id, user_agent: k.user_agent || '', config: configView(who) };
    }
    if (p === '/api/config/key/protocol') {           // 给客户端 key 绑定协议：/v1/models 据此只回该协议的模型（留空＝自动）
        const k = keyOf(b.name);
        const proto = String(b.protocol || '').toLowerCase();
        if (proto && !['chat', 'responses', 'messages'].includes(proto)) throw new Error('协议只能是 chat / responses / messages，或留空＝自动');
        if (proto) k.protocol = proto; else delete k.protocol;
        saveConfig(cfg); reloadConfig();
        log('CONFIG key protocol', k.name, proto || '(自动)');
        return { key: k.name, protocol: proto };
    }
    if (p === '/api/config/key/interop') {            // 协议互转（实验性，按 key，默认关）：允许该 key 的 responses/messages 客户端跨协议用其它协议的模型
        const k = keyOf(b.name);
        if (b.enabled) k.protocol_interop = true; else delete k.protocol_interop;
        saveConfig(cfg); reloadConfig();
        log('CONFIG key interop', k.name, b.enabled ? 'ON' : 'OFF');
        return { key: k.name, interop: !!k.protocol_interop };
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
    // ---------------- 普通用户自助：带自己的上游进组 / 退出 ----------------
    if (p === '/api/user/provider') {
        if (isAdmin) throw new Error('管理员请用「增加供应商」页与「上游组」页');
        const pool = String(b.pool || '').trim();
        const poolDef = (cfg.pools || {})[pool];
        if (!poolDef) throw new Error('上游组不存在: ' + pool);
        const already = membersOf(poolDef, cfg).some((m) => ((cfg.providers || {})[m.provider] || {}).owner === me.name);
        if (poolDef.user_join !== true && !already) throw new Error('这个组不接受用户加入');
        const label = String(b.name || '').trim();
        if (!/^[A-Za-z0-9][A-Za-z0-9 ._-]{1,40}$/.test(label)) throw new Error('名称只能字母数字空格 . _ -（2–41 字符）');
        const baseUrl = String(b.base_url || '').trim().replace(/\/+$/, '');
        if (!/^https?:\/\//.test(baseUrl)) throw new Error('base_url 必须以 http:// 或 https:// 开头');
        const apiKey = String(b.api_key || '').trim();
        if (!apiKey) throw new Error('请填上游 API key');
        const models = normUpstreamModels(b.models);
        if (!models.length) throw new Error('至少填一个上游模型名（可点「从上游拉取模型」）');
        const slug = label.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-+|-+$/g, '').slice(0, 20) || 'up';
        const pname = (`u-${me.name}-${slug}`).toLowerCase().replace(/[^a-z0-9_-]/g, '-').slice(0, 31);
        const existing = (cfg.providers || {})[pname];
        if (existing && existing.owner !== me.name) throw new Error('供应商名冲突: ' + pname);
        const envName = 'GW_' + pname.toUpperCase().replace(/[^A-Z0-9]/g, '_') + '_KEY';
        cfg.providers = cfg.providers || {};
        // 需要会话头的上游必须带 x-upstream-session，否则回 400 MissingSessionID；UA 不是浏览器式会被前置 WAF 拦。
        // 这里用 __SESSION__ 占位 → 运行时解析成「该客户端 key 的 session_id」，与 key 天然绑定。
        const pExtra = autoUpstreamHeaders(baseUrl, (existing || {}).extra_headers);
        const pUaIn = b.user_agent === undefined ? '' : String(b.user_agent || '').trim();
        const pUa = pUaIn || (existing || {}).user_agent || (isSessionHost(baseUrl) ? DEFAULT_ALT_UA : '');
        cfg.providers[pname] = {
            ...(existing || {}), base_url: baseUrl, api_key_env: envName, owner: me.name,
            ...(pUa ? { user_agent: pUa } : {}),
            ...(Object.keys(pExtra).length ? { extra_headers: pExtra } : {}),
        };
        setEnvVar(envName, apiKey);
        const prefix = pname;
        const re = '^' + prefix.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + '/';
        cfg.routes = cfg.routes || [];
        if (!cfg.routes.some((r) => r.model_regex === re)) {
            cfg.routes.unshift({ model_regex: re, provider: pname, strip: prefix + '/', format: 'auto', responses_regex: '^(gpt-|grok-)' });
        }
        cfg.models = cfg.models || [];
        const fullIds = models.map((m) => prefix + '/' + m);
        if (existing) cfg.models = cfg.models.filter((m) => !String(m).startsWith(prefix + '/') || fullIds.includes(m));   // 重提交时清掉旧模型
        for (const full of fullIds) if (!cfg.models.includes(full)) cfg.models.push(full);
        // 成员：保留组里原有成员，只替换/追加自己这一条
        const clean = membersOf(poolDef, cfg).filter((m) => m.provider !== pname).map((m) => ({
            provider: m.provider, weight: Number(m.weight) > 0 ? Number(m.weight) : 1,
            ...(m.model ? { model: String(m.model) } : {}),
            ...(Array.isArray(m.models) && m.models.length ? { models: m.models.map(String) } : {}),
        }));
        clean.push({ provider: pname, weight: 1, models });
        poolDef.members = clean;
        saveConfig(cfg); reloadConfig();
        log('USER provider', me.name, pname, '->', pool, models.join(','));
        return { provider: pname, prefix, pool, models, full_models: fullIds, config: configView(who) };
    }
    if (p === '/api/user/leave') {
        if (isAdmin) throw new Error('管理员请用「上游组」页编辑成员');
        const pool = String(b.pool || '').trim();
        const pname = String(b.provider || '').trim();
        const poolDef = (cfg.pools || {})[pool];
        if (!poolDef) throw new Error('上游组不存在: ' + pool);
        const prov = (cfg.providers || {})[pname];
        if (!prov || prov.owner !== me.name) throw new Error('只能移除自己的供应商');
        poolDef.members = membersOf(poolDef, cfg).filter((m) => m.provider !== pname).map((m) => ({
            provider: m.provider, weight: Number(m.weight) > 0 ? Number(m.weight) : 1,
            ...(m.model ? { model: String(m.model) } : {}),
            ...(Array.isArray(m.models) && m.models.length ? { models: m.models.map(String) } : {}),
        }));
        const stillUsed = Object.values(cfg.pools || {}).some((pd) => membersOf(pd, cfg).some((m) => m.provider === pname));
        let removed = null;
        if (!stillUsed) removed = dropProviderFromCfg(cfg, pname);
        saveConfig(cfg); reloadConfig();
        log('USER leave', me.name, pname, 'from', pool, stillUsed ? '(组内已摘除)' : '(供应商已删除)');
        return { pool, provider: pname, deleted: !stillUsed, removed, config: configView(who) };
    }
    // ---------------- 管理员：面板用户管理 ----------------
    if (p === '/api/config/user') {
        const name = String(b.name || '').trim();
        if (!USER_RE.test(name)) throw new Error('用户名只能字母数字 . _ -（2–32 字符，首字符字母数字）');
        const role = b.role === 'admin' ? 'admin' : 'user';
        if (name === me.name && role !== 'admin') throw new Error('不能把自己降成普通用户');
        const pw = String(b.password || '');
        if (pw && pw.length < 6) throw new Error('密码至少 6 位');
        cfg.users = cfg.users || {};
        const existed = cfg.users[name] || panelUser(name);
        if (!existed && !pw) throw new Error('新建用户必须设密码');
        const salt = (existed && existed.salt) || crypto.randomBytes(8).toString('hex');
        cfg.users[name] = {
            role, salt,
            password_sha256: pw ? hashPw(salt, pw) : existed.password_sha256,
            created_at: (existed && existed.created_at) || Date.now(),
        };
        saveConfig(cfg); reloadConfig();
        log('CONFIG user', name, role, existed ? 'update' : 'create');
        return { user: name, role, created: !existed };
    }
    if (p === '/api/config/user/delete') {
        const name = String(b.name || '').trim();
        if (name === me.name) throw new Error('不能删除当前登录的账号');
        const u = panelUser(name);
        if (!u) throw new Error('用户不存在: ' + name);
        if (u.virtual && name === (CFG.dashboard_auth || {}).username) throw new Error('Basic 管理员账号不能在这里删除（改 dashboard_auth 配置）');
        if (u.role === 'admin' && panelUserList().filter((x) => x.role === 'admin' && x.name !== name).length === 0) throw new Error('必须保留至少一个管理员');
        const provs = Object.entries(cfg.providers || {}).filter(([, p]) => p && p.owner === name).map(([n]) => n);
        const keys = (cfg.keys || []).filter((k) => k.owner === name).map((k) => k.name);
        if ((provs.length || keys.length) && b.force !== true) {
            throw new Error(`该用户还拥有 ${provs.length} 个供应商、${keys.length} 个 key；先让他退出组/删 key，或勾选强制删除`);
        }
        const removed = { providers: [], keys: keys.length, models: 0 };
        for (const pn of provs) { const r = dropProviderFromCfg(cfg, pn); removed.providers.push(pn); removed.models += r.models; }
        cfg.keys = (cfg.keys || []).filter((k) => k.owner !== name);
        delete cfg.users[name];
        for (const [t, s] of Object.entries(SESSIONS)) if (s.u === name) delete SESSIONS[t];
        saveSessions();
        saveConfig(cfg); reloadConfig();
        log('CONFIG user delete', name, JSON.stringify(removed));
        return { removed_user: name, removed };
    }
    if (p === '/api/config/reload') { reloadConfig(); if (ensureSessionDefaults(CFG)) saveConfig(CFG); return { reloaded: true }; }
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
// 有的上游提供官方余额接口；有的没有额度接口（上游 issue #31084 / #10448 之类都没实现），
// 所以按「官方单价 × 网关记录的 token」估算——只覆盖走本网关的流量。
// 单价表从 config.json 的 prices / peak_prices 读（每 1M tokens，USD；peak_prices 支持峰谷时段）
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
// ---------------- 订阅周期统计 ----------------
// 月付订阅：官方 /usage 的 monthly.resetsAt 就是下次续费时刻，往前推日历月即各周期边界。
// allowance 口径 = 实际成本 × 基数 / 该模型月上限（基数见 config.quota.base_usd），
// 所以「归一化额度 ÷ 基数」可以直接和供应商控制台里的百分比对齐。
const GO_BASE_USD = Number((CFG.quota && CFG.quota.base_usd) || 60) || 60;   // 归一化基数：config.quota.base_usd（默认 60）
const GO_CYCLE_COUNT = 6;                  // 展示最近几个周期
function monthShift(ms, n) {               // 日历月平移，月末溢出回退到目标月最后一天
    const d = new Date(ms), day = d.getUTCDate();
    d.setUTCDate(1);
    d.setUTCMonth(d.getUTCMonth() + n);
    const last = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth() + 1, 0)).getUTCDate();
    d.setUTCDate(Math.min(day, last));
    return d.getTime();
}
function goCycleAnchor(quota) {            // 下次续费时刻（ms）；官方取不到就用上次记下的锚点顺推
    const iso = quota && quota.ok && quota.monthly && quota.monthly.resets_at;
    const t = iso ? Date.parse(iso) : NaN;
    if (Number.isFinite(t)) {
        try { DB.prepare("INSERT OR REPLACE INTO meta(k,v) VALUES('quota_cycle_anchor',?)").run(String(t)); } catch { /* ignore */ }
        return t;
    }
    const row = DB.prepare("SELECT v FROM meta WHERE k='quota_cycle_anchor'").get();
    const prev = row ? Number(row.v) : NaN;
    if (Number.isFinite(prev) && prev > 0) { let a = prev; while (a <= Date.now()) a = monthShift(a, 1); return a; }
    return null;
}
function computeGoCycles(anchorMs, count = GO_CYCLE_COUNT) {
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
        if (cost == null) { e.unpriced++; continue; }        // 价格表里没有的模型只算请求数
        e.priced++; e.cost += cost;
        const allow = cap ? cost * GO_BASE_USD / cap : 0;    // cap=null → 官方 Unlimited 免费档
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
                pct: m.cap ? +(m.cost / m.cap * 100).toFixed(2) : null,      // 占该模型自身月限额
                cap: m.cap || null,
            })),
    }));
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
    // 面板域名单独密码（Basic；只对配置里的域名生效，本机/容器内不受影响）。
    // 有分用户登录后：Basic 仍可当管理员后备；不带 Basic 时不再弹浏览器密码框，改为页面内登录。
    const da = CFG.dashboard_auth || {};
    let dashOk = false;
    // 页面内「登出」后置一个 cookie，让服务端忽略浏览器缓存的 Basic 凭据（Basic 无法用 JS 清除）。
    const panelLoggedOut = /(?:^|;\s*)gw_panel_logout=1(?:;|$)/.test(String(req.headers.cookie || ''));
    if (da.enabled && Array.isArray(da.hosts) && da.hosts.includes(host) && !panelLoggedOut) {
        const hdr = req.headers['authorization'] || '';
        if (hdr.startsWith('Basic ')) {
            try {
                const [u, p] = Buffer.from(hdr.slice(6), 'base64').toString('utf8').split(':');
                const h = crypto.createHash('sha256').update((da.salt || '') + (p || '')).digest('hex');
                dashOk = (u === da.username) && (h === da.password_sha256);
            } catch { }
        }
    }
    const who = url.pathname.startsWith('/api/') ? identityOf(req, dashOk) : null;   // 面板身份（/v1 数据面不算）
    if (url.pathname === '/api/auth/me') {
        if (!who || !who.panel) return sendJson(res, 401, { error: { message: '未登录' } });
        return sendJson(res, 200, {
            ok: true,
            user: { name: who.name, role: who.role, via: who.via },
            pools: who.admin ? Object.keys(CFG.pools || {}) : userPools(who.name),
            joinable: who.admin ? [] : joinablePools(),
        });
    }
    if (url.pathname === '/api/auth/login') {
        if (req.method !== 'POST') return sendJson(res, 405, { error: { message: 'POST only' } });
        let b = {};
        try { b = JSON.parse((await readBody(req)) || '{}'); } catch { return sendJson(res, 400, { error: { message: 'invalid json' } }); }
        const ip = clientIp(req);
        if (loginBlocked(ip)) return sendJson(res, 429, { error: { message: '尝试次数过多，请 10 分钟后再试' } });
        const u = panelUser(b.username);
        if (!u || !pwEq(u.password_sha256, hashPw(u.salt, b.password || ''))) {
            loginFailed(ip);
            log('LOGIN FAIL', String(b.username || ''), ip);
            return sendJson(res, 401, { error: { message: '用户名或密码错误' } });
        }
        LOGIN_FAILS.delete(ip);
        const token = newSession(u.name);
        res.setHeader('Set-Cookie', 'gw_panel_logout=; Path=/; Max-Age=0; SameSite=Lax');   // 重新登录＝恢复 Basic 后备
        log('LOGIN', u.name, ip);
        return sendJson(res, 200, { ok: true, token, user: { name: u.name, role: u.role } });
    }
    if (url.pathname === '/api/auth/logout') {
        const t = String(req.headers['x-gw-token'] || '').trim();
        if (t && SESSIONS[t]) { delete SESSIONS[t]; saveSessions(); }
        res.setHeader('Set-Cookie', 'gw_panel_logout=1; Path=/; Max-Age=31536000; SameSite=Lax');
        log('LOGOUT', (who && who.name) || '', clientIp(req));
        return sendJson(res, 200, { ok: true });
    }
    if (url.pathname === '/api/user/provider/fetch') {     // 用户填上游信息时先拉模型清单（不写配置、不打统计）
        if (!who || !who.panel) return sendJson(res, 401, { error: { message: '未登录' } });
        if (who.admin) return sendJson(res, 400, { error: { message: '管理员请用「增加供应商」页的拉取功能' } });
        if (req.method !== 'POST') return sendJson(res, 405, { error: { message: 'POST only' } });
        let b = {};
        try { b = JSON.parse((await readBody(req)) || '{}'); } catch { return sendJson(res, 400, { error: { message: 'invalid json' } }); }
        const base = String(b.base_url || '').trim().replace(/\/+$/, '');
        if (!/^https?:\/\//.test(base)) return sendJson(res, 400, { error: { message: 'base_url 必须以 http:// 或 https:// 开头' } });
        try {
            const ids = await fetchModelsFrom(base, String(b.api_key || '').trim(), {});
            return sendJson(res, 200, { ok: true, models: ids });
        } catch (e) { return sendJson(res, 400, { error: { message: e.message } }); }
    }
    if (url.pathname === '/healthz') { res.setHeader('Cache-Control', 'no-store'); return sendJson(res, 200, { ok: true, enabled: isEnabled(), capture: captureOn(), total: TOTAL, models: (CFG.models || []).length, api_base: CFG.api_base || '' }); }
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
            res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store' });   // 禁止浏览器缓存单页，改版后刷新即生效
            return res.end(html);
        } catch (e) { return sendJson(res, 500, { error: { message: 'dashboard.html 缺失: ' + e.message } }); }
    }
    if (url.pathname === '/api/stats/requests') {
        if (!who || !who.panel) return sendJson(res, 401, { error: { message: 'Invalid API key', type: 'invalid_request_error', code: 'invalid_api_key' } });
        let q = {};
        if (req.method === 'POST') { try { q = JSON.parse((await readBody(req)) || '{}'); } catch { return sendJson(res, 400, { error: { message: 'invalid json' } }); } }
        else for (const [k, v] of url.searchParams.entries()) q[k] = v;
        try { return sendJson(res, 200, queryRequests(q, who)); } catch (e) { return sendJson(res, 500, { error: { message: e.message } }); }
    }
    if (url.pathname === '/api/upstream/quota') {          // 上游额度：余额（官方接口）+ 用量（本地按价格表估算）
        if (!who || !who.panel) return sendJson(res, 401, { error: { message: 'Invalid API key', type: 'invalid_request_error', code: 'invalid_api_key' } });
        if (!who.admin) return sendJson(res, 403, { error: { message: '只有管理员能看全量上游额度' } });
        const now = Date.now();
        const force = url.searchParams.get('refresh') === '1';
        if (force || !QUOTA_CACHE.data || now - QUOTA_CACHE.t > QUOTA_TTL_MS) {
            const ds = await upstreamBalance();
            const goQuota = await upstreamUsage();
            const go = computeGoUsage();
            const anchor = goCycleAnchor(goQuota);
            const cycles = anchor ? computeGoCycles(anchor) : null;
            QUOTA_CACHE = {
                t: now,
                data: {
                    ts: now,
                    balance: { ...ds, at: now },
                    usage: {
                        quota: goQuota,
                        detail_note: '网关侧明细：按 config.prices 的单价 × 本网关记录的 token 估算，只覆盖走本网关的流量',
                        cycle_stats: cycles ? { anchor, base: GO_BASE_USD, cycles } : null,
                        ...go,
                    },
                },
            };
        }
        return sendJson(res, 200, QUOTA_CACHE.data);
    }
    if (url.pathname === '/api/stats') {
        if (!who || !who.panel) return sendJson(res, 401, { error: { message: 'Invalid API key', type: 'invalid_request_error', code: 'invalid_api_key' } });
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
        const modeParam = () => (url.searchParams.get('mode') === 'direct' ? 'direct' : 'pool');
        return sendJson(res, 200, statsSnapshot({ since: numParam('since'), until: numParam('until'), tz: numParam('tz'), unit: unitParam(), dim: dimParam(), mode: modeParam() }, who));
    }

    // ---------------- 推理档位探测（面板） ----------------
    if (url.pathname === '/api/probe/efforts' || url.pathname === '/api/probe/efforts/bulk') {
        if (!who || !who.panel) return sendJson(res, 401, { error: { message: 'unauthorized', type: 'invalid_request_error' } });
        if (!who.admin) return sendJson(res, 403, { error: { message: '只有管理员能探测推理档位' } });
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
    if (url.pathname === '/api/pool/health') {          // 上游组成员实时健康（内存态：谁在冷却、连续失败几次）
        if (!who || !who.panel) return sendJson(res, 401, { error: { message: 'unauthorized', type: 'invalid_request_error' } });
        if (url.searchParams.get('refresh') === '1') { try { await refreshQuotas(); } catch { } }   // 强制刷一次额度
        // 普通用户只看自己加入/可加入的组，以及组内成员与自己供应商的额度
        const showPools = who.admin ? null : new Set([...userPools(who.name), ...joinablePools()]);
        const showProv = who.admin ? null : new Set(Object.entries(CFG.providers || {}).filter(([, p]) => p && p.owner === who.name).map(([n]) => n));
        if (!who.admin) for (const pn of showPools) for (const m of poolMembers((CFG.pools || {})[pn])) showProv.add(m.provider);
        const out = {};
        for (const [k, v] of POOL_HEALTH) {
            const i = k.indexOf('|');
            const pool = k.slice(0, i), provider = k.slice(i + 1);
            if (showPools && !showPools.has(pool)) continue;
            (out[pool] = out[pool] || {})[provider] = { fails: v.fails || 0, until: v.until || 0 };
        }
        const inflight = {};
        for (const [k, v] of POOL_INFLIGHT) {
            const i = k.indexOf('|');
            const pool = k.slice(0, i);
            if (showPools && !showPools.has(pool)) continue;
            (inflight[pool] = inflight[pool] || {})[k.slice(i + 1)] = v;
        }
        // 成员剩余额度（quota 策略用的即时值；没配额度查询的供应商不会出现在这里）
        const quota = {};
        for (const [n, s] of POOL_QUOTA_CACHE) {
            if (showProv && !showProv.has(n)) continue;
            quota[n] = {
                ok: !!s.ok, source: s.source || 'none', error: s.error || '', at: s.at || 0,
                headroom: Number.isFinite(s.headroom) ? s.headroom : null,
                percent: Number.isFinite(s.percent) ? s.percent : null,
                windows: s.windows || null, local: s.local || null,
            };
        }
        return sendJson(res, 200, { ts: Date.now(), pools: out, inflight, quota });
    }
    if (url.pathname === '/api/user/provider' || url.pathname === '/api/user/leave') {
        if (!who || !who.panel) return sendJson(res, 401, { error: { message: 'unauthorized', type: 'invalid_request_error' } });
        if (req.method !== 'POST') return sendJson(res, 405, { error: { message: 'POST only' } });
        let b = {};
        try { b = JSON.parse((await readBody(req)) || '{}'); } catch { return sendJson(res, 400, { error: { message: 'invalid json' } }); }
        try {
            const out = applyConfigChange(url.pathname, b, who);
            log('USER', url.pathname, 'by', who.name, JSON.stringify(out).slice(0, 160));
            return sendJson(res, 200, { ok: true, ...out, config: configView(who) });
        } catch (e) { return sendJson(res, 400, { error: { message: e.message } }); }
    }
    if (url.pathname.startsWith('/api/config')) {
        if (!who || !who.panel) return sendJson(res, 401, { error: { message: 'unauthorized', type: 'invalid_request_error' } });
        if (req.method === 'GET') return sendJson(res, 200, configView(who));
        let b = {};
        try { b = JSON.parse((await readBody(req)) || '{}'); } catch { return sendJson(res, 400, { error: { message: 'invalid json' } }); }
        try {
            if (url.pathname === '/api/config/key/reveal') {        // 回显 key：管理员看全部，普通用户只能看自己的
                const name = String(b.name || '').trim();
                const k = (CFG.keys || []).find((x) => x.name === name);
                if (!k) return sendJson(res, 404, { error: { message: 'key 不存在: ' + name } });
                if (!who.admin && k.owner !== who.name) return sendJson(res, 403, { error: { message: '只能显示自己的 key' } });
                log('CONFIG reveal key', name, 'by', who.name);
                return sendJson(res, 200, { name, key: k.key });
            }
            if (url.pathname === '/api/config/provider/detail') {   // 编辑用：回显供应商当前属性（含 UA/额外头）
                const name = String(b.name || '').trim();
                const p = (CFG.providers || {})[name];
                if (!p) return sendJson(res, 404, { error: { message: '供应商不存在: ' + name } });
                if (!who.admin && p.owner !== who.name) return sendJson(res, 403, { error: { message: '只能查看自己的供应商' } });
                const rt = (CFG.routes || []).find((r) => r.provider === name && r.strip);
                return sendJson(res, 200, {
                    name,
                    prefix: rt ? String(rt.strip || '').replace(/\/$/, '') : '',
                    base_url: p.base_url || '',
                    api_key_env: p.api_key_env || '',
                    has_key: !!(process.env[p.api_key_env] || '').length,
                    user_agent: p.user_agent || '',
                    reasoning_echo: !!p.reasoning_echo,
                    strip_body_fields: Array.isArray(p.strip_body_fields) ? p.strip_body_fields : [],
                    quota_url: p.quota_url || '',
                    quota_url_auto: quotaUrlOf(p),
                    quota: p.quota || null,
                    efforts: normEfforts((CFG.model_efforts || {})[effortKeyOf(name, '*')]),
                    test: providerIsTest(p),
                    extra_headers: p.extra_headers || {},
                    models: (CFG.models || []).filter((m) => rt && m.startsWith(rt.strip)).length,
                });
            }
            if (url.pathname === '/api/config/provider/models') {   // 拉取上游模型列表（不打网关统计）
                if (!who.admin) {
                    const prefix = String(b.prefix || '').trim().replace(/\/+$/, '');
                    const rt = (CFG.routes || []).find((r) => r.strip === prefix + '/' && r.provider);
                    if (!rt || ((CFG.providers || {})[rt.provider] || {}).owner !== who.name) return sendJson(res, 403, { error: { message: '只能拉取自己的供应商' } });
                }
                const out = await fetchProviderModels(b);
                return sendJson(res, 200, { ok: true, ...out });
            }
            const out = applyConfigChange(url.pathname, b, who);
            log('CONFIG', url.pathname, 'by', who.name, JSON.stringify(out).slice(0, 200));
            return sendJson(res, 200, { ok: true, ...out, config: configView(who) });
        } catch (e) { return sendJson(res, 400, { error: { message: e.message } }); }
    }
    // ---------- 原生 Gemini 直通：客户端自带 Google 协议时原样转发（不做翻译）----------
    // 形状：/v1beta/models/<model>:generateContent 与 :streamGenerateContent[?alt=sse]
    // （同时接受 /v1/models/... 前缀，Google SDK 两种 base_url 都见过）
    // 模型名可能自带前缀斜杠（对外名形如 pfx/model-a），所以这里不能用 [^/:]+ 排除斜杠
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
        let gRt = routeFor(gModel, { stickyKey: gAuth.session_id });
        const gStream = gemHit[2] === 'streamGenerateContent';
        const gBase = { ts: t0, key: gAuth.name, model: gModel, stream: gStream, ip: clientIp(req), passthrough: true, cproto: 'gemini', conv: convFingerprint(gBody), req_bytes: Buffer.byteLength(gRaw || ''), res_bytes: 0, up_res_bytes: 0 };
        gBase.pool = (gRt && gRt.poolPlan && gRt.poolPlan.name) || '';
        countOut(res, gBase);
        if (gRt && gRt.__poolEmpty) {
            record({ ...gBase, status: 400, ms: Date.now() - t0, provider: null });
            return sendJson(res, 400, { error: { code: 400, status: 'INVALID_ARGUMENT', message: `上游组 "${gRt.pool}" 里没有能提供模型 "${gRt.upstream_model}" 的上游` } });
        }
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
        // 上游池：按策略选候选成员，失败就换下一个（gemini 原生直通同样支持）
        const gPlan = gRt.poolPlan || null;
        const gAttempts = gPlan ? gPlan.order : [gRt];
        capture('client_req', { cproto: 'gemini', ip: clientIp(req), key: gAuth.name, model: gModel, stream: gStream, body: gBody });
        try {
            const ac = new AbortController();
            const onClose = () => { if (!res.writableEnded) { try { ac.abort(); } catch { } } };
            req.on('close', onClose);
            let up = null, upTxt = '', upStatus = 0, gLastRt = gRt;
            const gTrail = [];
            for (let i = 0; i < gAttempts.length; i++) {
                const aRt = gAttempts[i];
                gLastRt = aRt;
                const gHeaders = upstreamHeaders(aRt.providerCfg, gAuth.session_id, gStream ? 'text/event-stream' : 'application/json', 'gemini', gAuth.user_agent);
                // 客户端带来的生成参数原样保留，只把 model 从路径里的对外名换成上游名
                const gPayload = { ...gBody };
                stripBodyFields(gPayload, aRt.providerCfg.strip_body_fields, aRt.provider);
                const gUrl = aRt.providerCfg.base_url + '/models/' + encodeURIComponent(aRt.upstream_model) + ':' + gemHit[2] + (gStream ? '?alt=sse' : '');
                capture('upstream_req', { cproto: 'gemini', url: gUrl, provider: aRt.provider, key: gAuth.name, model: gModel, upstream_model: aRt.upstream_model, payload: gPayload });
                try {
                    up = await fetch(gUrl, { method: 'POST', headers: gHeaders, body: jdump(gPayload), signal: ac.signal });
                    upStatus = up.status;
                } catch (fe) {
                    if (fe.name === 'AbortError') throw fe;
                    up = null; upStatus = 0; upTxt = 'fetch failed: ' + fe.message;
                    noteAttempt(gTrail, aRt.provider, 0, fe.message);
                    log('PASSTHROUGH FETCH FAIL', aRt.provider, 'gemini', gModel, fe.message);
                }
                capture('upstream_status', { cproto: 'gemini', provider: aRt.provider, status: up ? up.status : 0, ok: !!up && up.ok });
                if (up && up.ok) { gRt = aRt; if (gPlan) { poolMark(gPlan.name, aRt.provider, true, gPlan.def); log('POOL HIT', gPlan.name, aRt.provider, gModel); } break; }
                if (up) { upTxt = await up.text(); upStatus = up.status; noteAttempt(gTrail, aRt.provider, upStatus, upTxt); }
                if (gPlan) poolMark(gPlan.name, aRt.provider, false, gPlan.def);
                if (gPlan && i < gAttempts.length - 1 && (!up || RETRYABLE_STATUS.has(upStatus))) {
                    log('POOL FAILOVER', gPlan.name, aRt.provider, '->', gAttempts[i + 1].provider, `(${upStatus || 'network'})`);
                    continue;
                }
                break;
            }
            req.off('close', onClose);
            if (!up || !up.ok) {
                const txt = upTxt;
                const gFail = gPlan ? poolFailBody(gPlan, gTrail, txt) : null;
                const outStatus = gPlan ? poolFailStatus(gTrail) : (upStatus || 502);
                let parsed = null; try { parsed = JSON.parse(txt); } catch { }
                log('PASSTHROUGH ERROR', outStatus, gLastRt.provider, 'gemini', gModel, String(gFail ? gFail.hint : txt).slice(0, 250));
                record({ ...gBase, status: outStatus, ms: Date.now() - t0, provider: gLastRt.provider, upstream_model: gLastRt.upstream_model, format: 'gemini', error: String(gFail ? gFail.message : txt).slice(0, 500) });
                return sendJson(res, outStatus, gFail
                    ? { error: { code: outStatus, status: 'UNAVAILABLE', message: gFail.message, hint: gFail.hint, attempts: gFail.attempts } }
                    : parsed?.error ? parsed : { error: { code: outStatus, message: txt.slice(0, 800) || 'upstream error' } });
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
        let pRt = routeFor(pBody.model, { stickyKey: pAuth.session_id });
        const pBase = { ts: t0, key: pAuth.name, model: pBody.model, stream: !!pBody.stream, ip: clientIp(req), passthrough: true, cproto: kind, conv: convFingerprint(pBody), req_bytes: Buffer.byteLength(pRaw || ''), res_bytes: 0, up_res_bytes: 0 };
        pBase.pool = (pRt && pRt.poolPlan && pRt.poolPlan.name) || '';
        countOut(res, pBase);
        if (pRt && pRt.__poolEmpty) {
            record({ ...pBase, status: 400, ms: Date.now() - t0, provider: null, error: 'pool_no_capable_member' });
            return sendJson(res, 400, { error: { message: `上游组 "${pRt.pool}" 里没有能提供模型 "${pRt.upstream_model}" 的上游`, type: 'invalid_request_error', code: 'model_not_in_pool' } });
        }
        if (!pRt) { record({ ...pBase, status: 400, ms: Date.now() - t0, provider: null }); return sendJson(res, 400, { error: { message: `model "${pBody.model}" not routed`, type: 'invalid_request_error' } }); }
        // 协议互转（按 key 开关 keys[].protocol_interop，默认关；看板「客户端 key」页可勾）：开启时
        // 「上游协议 != 客户端协议」走 chat 中枢互转；关闭时一律走下面的原生直通，由上游报协议不支持。
        const upFmt = resolveFormat(pRt);
        if (upFmt !== kind && pAuth.interop) {
            log('CONVERT', kind, '->', upFmt, pBody.model);
            return serveChatPipeline(req, res, { auth: pAuth, rt: pRt, body: pBody, rawBody: pRaw, t0, clientProto: kind });
        }
        // 上游池：按策略选出候选成员链，前面的人挂了就换下一个（只在写出任何字节之前换，流一旦开始就不折腾）
        const pPlan = pRt.poolPlan || null;
        const pAttempts = pPlan ? pPlan.order : [pRt];
        capture('client_req', { kind, ip: clientIp(req), key: pAuth.name, model: pBody.model, stream: !!pBody.stream, body: pBody });
        try {
            const ac = new AbortController();
            const onClose = () => { if (!res.writableEnded) { try { ac.abort(); } catch { } } };
            req.on('close', onClose);
            let up = null, upTxt = '', upStatus = 0, pLastRt = pRt;
            const pTrail = [];
            for (let i = 0; i < pAttempts.length; i++) {
                const aRt = pAttempts[i];
                pLastRt = aRt;
                const pHeaders = upstreamHeaders(aRt.providerCfg, pAuth.session_id, pBody.stream ? 'text/event-stream' : 'application/json', kind, pAuth.user_agent);
                for (const hn of Object.keys(req.headers)) {                 // 把客户端的协议头原样带上去（anthropic-beta / openai-beta / x-stainless-*）
                    if (/^(anthropic-|openai-beta|x-stainless)/i.test(hn)) pHeaders[hn] = String(req.headers[hn]);
                }
                const pPayload = { ...clampBodyEffort(pBody, aRt), model: aRt.upstream_model };     // 只换模型名 + 档位就近降级，输入原样（含客户端自己回传的 reasoning item）
                stripBodyFields(pPayload, aRt.providerCfg.strip_body_fields, aRt.provider);
                if (aRt.providerCfg.reasoning_echo && kind === 'responses' && Array.isArray(pBody.input)) {
                    const inj = injectReasonsIntoInput(pBody.input, aRt.provider);   // 客户端没回传的那几条，由网关补上（只补缺的、不重复、且只在同一供应商内）
                    if (inj.injected) { pPayload.input = inj.input; pBase.reason_inj = inj.injected; log('[reason] 直通补回', inj.injected, '条 ->', pBody.model); }
                }
                const pUrl = aRt.providerCfg.base_url + (kind === 'responses' ? '/responses' : '/messages');
                capture('upstream_req', { kind, url: pUrl, provider: aRt.provider, key: pAuth.name, model: pBody.model, upstream_model: aRt.upstream_model, payload: pPayload });
                try {
                    up = await fetch(pUrl, { method: 'POST', headers: pHeaders, body: jdump(pPayload), signal: ac.signal });
                    upStatus = up.status;
                } catch (fe) {
                    if (fe.name === 'AbortError') throw fe;                       // 客户端断开：不重试
                    up = null; upStatus = 0; upTxt = 'fetch failed: ' + fe.message;
                    noteAttempt(pTrail, aRt.provider, 0, fe.message);
                    log('PASSTHROUGH FETCH FAIL', aRt.provider, kind, pBody.model, fe.message);
                }
                capture('upstream_status', { kind, provider: aRt.provider, status: up ? up.status : 0, ok: !!up && up.ok });
                if (up && up.ok) { pRt = aRt; if (pPlan) { poolMark(pPlan.name, aRt.provider, true, pPlan.def); log('POOL HIT', pPlan.name, aRt.provider, pBody.model); } break; }
                if (up) { upTxt = await up.text(); upStatus = up.status; noteAttempt(pTrail, aRt.provider, upStatus, upTxt); }
                if (pPlan) poolMark(pPlan.name, aRt.provider, false, pPlan.def);
                if (pPlan && i < pAttempts.length - 1 && (!up || RETRYABLE_STATUS.has(upStatus))) {
                    log('POOL FAILOVER', pPlan.name, aRt.provider, '->', pAttempts[i + 1].provider, `(${upStatus || 'network'})`);
                    continue;
                }
                break;
            }
            req.off('close', onClose);
            if (!up || !up.ok) {
                const txt = upTxt;
                const pFail = pPlan ? poolFailBody(pPlan, pTrail, txt) : null;
                const outStatus = pPlan ? poolFailStatus(pTrail) : (upStatus || 502);
                let parsed = null; try { parsed = JSON.parse(txt); } catch { }
                log('PASSTHROUGH ERROR', outStatus, pLastRt.provider, kind, pBody.model, String(pFail ? pFail.hint : txt).slice(0, 250));
                record({ ...pBase, status: outStatus, ms: Date.now() - t0, provider: pLastRt.provider, upstream_model: pLastRt.upstream_model, format: kind, error: String(pFail ? pFail.message : txt).slice(0, 500) });
                const pErrBody = pFail
                    ? { error: { message: pFail.message, type: 'upstream_pool_exhausted', code: 'upstream_unavailable', hint: pFail.hint, attempts: pFail.attempts } }
                    : parsed?.error ? { error: parsed.error } : { error: { message: txt.slice(0, 800) || 'upstream error', type: 'upstream_error' } };
                return sendJson(res, outStatus, pErrBody);
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
    const rt = routeFor(body.model, { stickyKey: auth.session_id });
    if (rt && rt.__poolEmpty) {                    // 组里没有能提供这个模型的上游（不是路由不存在）
        const msg = `上游组 "${rt.pool}" 里没有能提供模型 "${rt.upstream_model}" 的上游：成员的可用模型里都没有它（或成员还没声明模型清单）`;
        record({ ts: t0, key: auth.name, model: body.model, stream: !!body.stream, ip: clientIp(req), cproto: 'chat', req_bytes: Buffer.byteLength(rawBody || ''), status: 400, ms: Date.now() - t0, provider: null, error: 'pool_no_capable_member' });
        log('POOL NO MODEL', rt.pool, rt.upstream_model);
        return sendJson(res, 400, { error: { message: msg, type: 'invalid_request_error', code: 'model_not_in_pool' } });
    }
    if (!rt) {
        record({ ts: t0, key: auth.name, model: body.model, stream: !!body.stream, ip: clientIp(req), cproto: 'chat', req_bytes: Buffer.byteLength(rawBody || ''), status: 400, ms: Date.now() - t0, provider: null });
        return sendJson(res, 400, { error: { message: `model "${body.model}" not routed`, type: 'invalid_request_error' } });
    }
    return serveChatPipeline(req, res, { auth, rt, body, rawBody, t0, clientProto: 'chat' });
});

// 启动时补齐会话默认值（老配置缺 session_id、或 session_hosts 里的上游缺 x-upstream-session / UA 时自动修好并落盘）
try { if (ensureSessionDefaults(CFG)) { saveConfig(CFG); reloadConfig(); log('SESSION defaults applied'); } } catch (e) { log('SESSION defaults FAILED', e.message); }

const { host = '0.0.0.0', port = 8010 } = CFG.listen || {};
server.listen(port, host, () => log(`gateway listening on ${host}:${port} — keys=${KEYS.size} models=${(CFG.models || []).length} env=${ENV_LOADED} providers=${Object.keys(CFG.providers || {}).length} hold=${HOLD_MS > 0 ? HOLD_MS / 1000 + 's/' + KA_MS / 1000 + 's' : 'off'}`));

// 成员额度定时刷新：只有 quota 策略的池成员才会真去打接口，没配就是空转
refreshQuotas().catch(() => { });
setInterval(() => refreshQuotas().catch(() => { }), POOL_QUOTA_TTL_MS).unref?.();
