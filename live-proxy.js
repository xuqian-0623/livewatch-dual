/**
 * LiveWatch 本地数据代理  ——  零依赖（仅用 Node 内置模块）
 * ------------------------------------------------------------------
 * 解决两个浏览器无法绕过的问题：
 *   1. B 站接口不返回 CORS 头，网页无法直接请求
 *   2. 弹幕/礼物走私有二进制 WebSocket 协议（brotli/zlib + WBI 鉴权）
 *
 * 启动:  node live-proxy.js
 * 看板:  http://localhost:8787
 *
 * 对外接口:
 *   GET /api/health              健康检查
 *   GET /api/room?url=<直播间链接>  房间信息（真实）
 *   GET /api/stream?room=<房间号>   SSE 实时流：弹幕/礼物/人气/点赞/上舰/SC
 */
"use strict";
const http = require("http");
const https = require("https");
const crypto = require("crypto");
const zlib = require("zlib");
const fs = require("fs");
const path = require("path");
const { URL } = require("url");

/* ===== WebSocket Server (用于讯飞转写中继) ===== */
const WebSocket = require("ws");
let XunfeiRTASR = null;
try { XunfeiRTASR = require("./xunfei-asr").XunfeiRTASR; } catch (e) { console.log("[xunfei] 模块加载失败:", e.message); }

/* ===== 抖音弹幕客户端（独立模块） ===== */
let DouyinDanmakuClient = null;
try { DouyinDanmakuClient = require("./douyin-danmaku").DouyinDanmakuClient; } catch (e) {
  console.log("[douyin] 弹幕模块加载失败（可选功能）:", e.message);
}
const DY_DANMAKU = new Map(); // roomId -> { client, subs:Set(res), sockets:Set(ws) }
const ACTIVE_DUAL_SESSIONS = new Map(); // roomId -> { sessionId, activatedAt }
const DOUYIN_RESOLVE_CACHE = new Map(); // roomUrl -> { result, expiresAt }
const DOUYIN_RESOLVE_PENDING = new Map(); // roomUrl -> Promise
const PUBLIC_MODE = process.env.PUBLIC_MODE === "true" || process.env.NODE_ENV === "production";
const PUBLIC_ROOM_MAP = new Map([
  ["dual-room-1", "https://live.douyin.com/214698741282"],
  ["dual-room-2", "https://live.douyin.com/978001089489"]
]);
const SERVER_XUNFEI_CONFIG = {
  appId: String(process.env.XUNFEI_APP_ID || "").trim(),
  apiKey: String(process.env.XUNFEI_API_KEY || "").trim(),
  apiSecret: String(process.env.XUNFEI_API_SECRET || "").trim()
};

function canonicalDouyinRoomUrl(rawUrl) {
  const match = String(rawUrl || "").trim().match(/live\.douyin\.com\/(\d+)/i);
  return match ? `https://live.douyin.com/${match[1]}` : "";
}

function validatePublicRoom(roomId, rawUrl) {
  if (!PUBLIC_MODE) return { ok: true, url: String(rawUrl || "").trim() };
  const expected = PUBLIC_ROOM_MAP.get(String(roomId || ""));
  const actual = canonicalDouyinRoomUrl(rawUrl);
  if (!expected || actual !== expected) {
    return { ok: false, reason: "公网模式仅允许监控预设的两个公司直播间" };
  }
  return { ok: true, url: expected };
}

function validatePublicResolve(rawUrl) {
  if (!PUBLIC_MODE) return { ok: true, url: String(rawUrl || "").trim() };
  const actual = canonicalDouyinRoomUrl(rawUrl);
  if (!actual || !Array.from(PUBLIC_ROOM_MAP.values()).includes(actual)) {
    return { ok: false, reason: "公网模式禁止解析非预设直播间" };
  }
  return { ok: true, url: actual };
}

function claimDualSession(roomId, sessionId, activatedAt) {
  if (PUBLIC_MODE || !roomId || !sessionId) return { ok: true };
  const generation = Number(activatedAt) || 0;
  const current = ACTIVE_DUAL_SESSIONS.get(roomId);
  if (current && current.sessionId !== sessionId && generation < current.activatedAt) {
    return { ok: false, reason: "该请求来自已失效的旧看板页面，已阻止其覆盖当前监控会话" };
  }
  if (!current || current.sessionId !== sessionId || generation > current.activatedAt) {
    ACTIVE_DUAL_SESSIONS.set(roomId, { sessionId, activatedAt: generation });
  }
  return { ok: true };
}

function startDyDanmaku(roomId, roomUrl, res) {
  if (!DouyinDanmakuClient) return;
  let entry = DY_DANMAKU.get(roomId);
  if (!entry) {
    const client = new DouyinDanmakuClient(roomUrl, ev => {
      const line = "data: " + JSON.stringify(ev) + "\n\n";
      for (const s of entry.subs) { try { s.write(line); } catch (e) {} }
    });
    entry = { client, subs: new Set(), sockets: new Set() };
    DY_DANMAKU.set(roomId, entry);
    client.start();
    console.log(`[douyin] 弹幕客户端已启动: ${roomUrl}`);
  }
  entry.subs.add(res);
  if (entry._gcTimer) { clearTimeout(entry._gcTimer); entry._gcTimer = null; }
  res.on("close", () => {
    entry.subs.delete(res);
    if (entry.subs.size === 0) {
      entry._gcTimer = setTimeout(() => {
        entry.client.stop();
        DY_DANMAKU.delete(roomId);
        console.log(`[douyin] 弹幕客户端已清理: ${roomId}`);
      }, 30000);
    }
  });
}
const { spawn, execSync } = require("child_process");

const PORT = Number(process.env.PORT) || 8787;
const UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36";
const MIX = [46,47,18,2,53,8,23,32,15,50,10,31,58,3,45,35,27,43,5,49,33,9,42,19,29,28,14,39,12,38,41,13,37,48,7,16,24,55,40,61,26,17,0,1,60,51,30,4,22,25,54,21,56,59,6,63,57,62,11,36,20,34,44,52];
const md5 = s => crypto.createHash("md5").update(s).digest("hex");
const log = (...a) => console.log("[" + new Date().toLocaleTimeString("zh-CN") + "]", ...a);

/* ============ 基础 HTTP ============ */
function get(url, headers) {
  return new Promise((resolve, reject) => {
    const req = https.get(url, { headers: Object.assign({ "User-Agent": UA, "Accept": "application/json, text/plain, */*" }, headers || {}) }, res => {
      const chunks = [];
      res.on("data", c => chunks.push(c));
      res.on("end", () => resolve({ status: res.statusCode, headers: res.headers, body: Buffer.concat(chunks).toString("utf8") }));
    });
    req.on("error", reject);
    req.setTimeout(10000, () => { req.destroy(new Error("timeout")); });
  });
}
async function getJSON(url, headers) {
  const r = await get(url, headers);
  try { return JSON.parse(r.body); } catch (e) { throw new Error("非 JSON 响应: " + r.body.slice(0, 120)); }
}

/* ============ 鉴权：buvid3 + WBI 签名 ============ */
const AUTH = { buvid: null, buvidAt: 0, mixinKey: null, mixinAt: 0 };
async function getBuvid() {
  if (AUTH.buvid && Date.now() - AUTH.buvidAt < 3600e3) return AUTH.buvid;
  const j = await getJSON("https://api.bilibili.com/x/frontend/finger/spi");
  AUTH.buvid = { b3: j.data.b_3, b4: j.data.b_4 };
  AUTH.buvidAt = Date.now();
  return AUTH.buvid;
}
async function getMixinKey() {
  if (AUTH.mixinKey && Date.now() - AUTH.mixinAt < 600e3) return AUTH.mixinKey;
  const bv = await getBuvid();
  const j = await getJSON("https://api.bilibili.com/x/web-interface/nav", {
    Referer: "https://www.bilibili.com/", Cookie: `buvid3=${bv.b3}; buvid4=${bv.b4}`
  });
  const wi = j.data.wbi_img;
  if (!wi || !wi.img_url || !wi.sub_url) throw new Error("WBI 鉴权数据异常，img_url/sub_url 为空");
  const raw = wi.img_url.split("/").pop().split(".")[0] + wi.sub_url.split("/").pop().split(".")[0];
  AUTH.mixinKey = MIX.map(i => raw[i]).join("").slice(0, 32);
  AUTH.mixinAt = Date.now();
  return AUTH.mixinKey;
}
async function wbi(params) {
  const mk = await getMixinKey();
  const p = Object.assign({}, params, { wts: Math.round(Date.now() / 1000) });
  const q = Object.keys(p).sort()
    .map(k => `${encodeURIComponent(k)}=${encodeURIComponent(String(p[k]).replace(/[!'()*]/g, ""))}`).join("&");
  return `${q}&w_rid=${md5(q + mk)}`;
}

/* ============ 链接解析 ============ */
function parseUrl(raw) {
  let u = String(raw || "").trim();
  if (!u) return { platform: "unknown", roomId: "" };
  if (/^\d{1,12}$/.test(u)) return { platform: "bilibili", roomId: u };
  if (!/^https?:\/\//i.test(u)) u = "https://" + u;
  let url; try { url = new URL(u); } catch (e) { return { platform: "unknown", roomId: "" }; }
  const h = url.hostname.replace(/^www\./, "").toLowerCase();
  const seg = url.pathname.split("/").filter(Boolean);
  const last = seg[seg.length - 1] || "";
  if (h.includes("bilibili.com")) return { platform: "bilibili", roomId: (seg[0] || url.searchParams.get("roomid") || "").replace(/\D/g, "") };
  if (h.includes("douyin.com")) return { platform: "douyin", roomId: last };
  if (h.includes("kuaishou.com")) return { platform: "kuaishou", roomId: last };
  if (h.includes("huya.com")) return { platform: "huya", roomId: last };
  if (h.includes("douyu.com")) return { platform: "douyu", roomId: last };
  if (h.includes("youtube.com") || h.includes("youtu.be")) return { platform: "youtube", roomId: url.searchParams.get("v") || last };
  if (h.includes("twitch.tv")) return { platform: "twitch", roomId: last };
  return { platform: "unknown", roomId: last || h };
}

/* ============ 房间信息（真实） ============ */
async function fetchRoom(roomId) {
  const bv = await getBuvid();
  const H = { Referer: "https://live.bilibili.com/", Cookie: `buvid3=${bv.b3}; buvid4=${bv.b4}` };
  const base = await getJSON(`https://api.live.bilibili.com/xlive/web-room/v1/index/getRoomBaseInfo?room_ids=${roomId}&req_biz=web`, H);
  if (base.code !== 0) throw new Error("getRoomBaseInfo: " + base.message);
  const map = base.data && base.data.by_room_ids || {};
  const info = map[roomId] || Object.values(map)[0];
  if (!info) throw new Error("房间不存在或已封禁");

  const out = {
    roomId: String(info.room_id), uid: info.uid,
    title: info.title, cover: info.cover || info.keyframe || "",
    area: [info.parent_area_name, info.area_name].filter(Boolean).join(" · "),
    live: info.live_status === 1,
    liveStatus: info.live_status,               // 0未开播 1直播中 2轮播
    startTime: info.live_time && info.live_time !== "0000-00-00 00:00:00"
      ? new Date(String(info.live_time).replace(/-/g, "/")).getTime() : 0,
    online: typeof info.online === "number" ? info.online : 0,
    anchor: info.uname || "", avatar: "", fans: null, liveUrl: info.live_url || ""
  };
  try {
    const m = await getJSON(`https://api.live.bilibili.com/live_user/v1/Master/info?uid=${info.uid}`, H);
    if (m.code === 0 && m.data) {
      out.avatar = (m.data.info && m.data.info.face) || "";
      out.anchor = (m.data.info && m.data.info.uname) || out.anchor;
      out.fans = typeof m.data.follower_num === "number" ? m.data.follower_num : null;
      out.medal = m.data.medal_name || "";
    }
  } catch (e) { /* 非关键字段，忽略 */ }
  return out;
}

/* ============ 弹幕 WebSocket 客户端（手写，零依赖） ============ */
function packet(op, body) {
  const b = Buffer.from(body || "", "utf8");
  const h = Buffer.alloc(16);
  h.writeUInt32BE(16 + b.length, 0);
  h.writeUInt16BE(16, 4);
  h.writeUInt16BE(1, 6);
  h.writeUInt32BE(op, 8);
  h.writeUInt32BE(1, 12);
  return Buffer.concat([h, b]);
}

class DanmakuClient {
  constructor(roomId, onEvent) {
    this.roomId = String(roomId);
    this.onEvent = onEvent;
    this.ws = null; this.hb = null; this.closed = false; this.retry = 0;
    this.stat = { danmaku: 0, gift: 0, giftValue: 0 };
  }
  async connect() {
    if (this.closed) return;
    try {
      const bv = await getBuvid();
      const q = await wbi({ id: this.roomId, type: 0 });
      const j = await getJSON(`https://api.live.bilibili.com/xlive/web-room/v1/index/getDanmuInfo?${q}`, {
        Referer: "https://live.bilibili.com/", Cookie: `buvid3=${bv.b3}; buvid4=${bv.b4}`
      });
      if (j.code !== 0) throw new Error("getDanmuInfo: " + j.message);
      const host = j.data?.host_list?.[0];
      if (!host) throw new Error("host_list 为空，无法连接弹幕服务器");
      const ws = new WebSocket(`wss://${host.host}:${host.wss_port}/sub`);
      this.ws = ws; ws.binaryType = "arraybuffer";
      ws.onopen = () => {
        this.retry = 0;
        ws.send(packet(7, JSON.stringify({
          uid: 0, roomid: Number(this.roomId), protover: 3,
          buvid: bv.b3, platform: "web", type: 2, key: j.data.token
        })));
        log(`房间 ${this.roomId} 弹幕流已连接`);
        // 心跳必须在认证成功(op=8)之后才发，见 decode() 中的 startHeartbeat()
      };
      ws.onmessage = ev => { try { this.decode(Buffer.from(ev.data)); } catch (e) { log(`房间 ${this.roomId} 弹幕解码错误: ${e.message}`); } };
      ws.onclose = () => { clearInterval(this.hb); this.hb = null; this.reconnect(); };
      ws.onerror = e => log(`房间 ${this.roomId} WebSocket 错误: ${e.message || e}`);
    } catch (e) {
      log(`房间 ${this.roomId} 弹幕连接失败: ${e.message}`);
      this.onEvent({ type: "error", message: e.message });
      this.reconnect();
    }
  }
  // 心跳回包(op=3)携带实时人气值，是在线人数趋势的数据来源。
  // 15 秒一轮：既满足 B 站 30 秒内必须心跳的要求，又保证趋势图数据点密度。
  startHeartbeat() {
    if (this.hb) return;
    const send = () => {
      if (!this.ws || this.ws.readyState !== 1) return;
      try { this.ws.send(packet(2, "")); }
      catch (e) { log(`房间 ${this.roomId} 心跳发送失败: ${e.message}`); }
    };
    send();
    this.hb = setInterval(send, 15000);
  }
  reconnect() {
    if (this.closed) return;
    this.retry++;
    if (this.retry > 8) {
      this.onEvent({ type: "error", message: "弹幕流重连超过上限，已断开" });
      this.close(); // 彻底关闭并从 ROOMS 中移除
      return;
    }
    const d = Math.min(20000, 1500 * this.retry);
    setTimeout(() => this.connect(), d);
  }
  close() {
    this.closed = true; clearInterval(this.hb); this.hb = null;
    if (this.ws) { try { this.ws.close(); } catch (e) {} }
    // 通知所有订阅者关闭并清理 ROOMS 连接池
    const r = ROOMS.get(this.roomId);
    if (r && r.client === this) {
      for (const s of r.subs) { try { s.end(); } catch (e) {} }
      ROOMS.delete(this.roomId);
    }
  }
  decode(buf) {
    let off = 0;
    while (off + 16 <= buf.length) {
      const len = buf.readUInt32BE(off);
      const hl = buf.readUInt16BE(off + 4);
      const ver = buf.readUInt16BE(off + 6);
      const op = buf.readUInt32BE(off + 8);
      if (len <= 0 || off + len > buf.length) break;
      const body = buf.subarray(off + hl, off + len);
      if (ver === 2) this.decode(zlib.inflateSync(body));
      else if (ver === 3) this.decode(zlib.brotliDecompressSync(body));
      else if (op === 3) this.onEvent({ type: "popularity", value: body.readUInt32BE(0) });
      else if (op === 8) { this.onEvent({ type: "connected", room: this.roomId }); this.startHeartbeat(); }
      else if (op === 5) { try { this.handle(JSON.parse(body.toString("utf8"))); } catch (e) {} }
      off += len;
    }
  }
  handle(m) {
    const c = m.cmd || "";
    if (c === "DANMU_MSG") {
      // info[2] = [uid, uname, isAdmin, ...]
      // info[3] = [勋章等级, 勋章名, 主播名, 房间号, 颜色, ..., guard_level(10)]
      const i = m.info;
      const md = i[3] || [];
      const medalLevel = md[0] || 0, medalName = md[1] || "";
      const guardLevel = md[10] || 0;                       // 1总督 2提督 3舰长
      const admin = !!(i[2] && i[2][2] === 1);
      this.stat.danmaku++;
      this.onEvent({
        type: "danmaku", user: i[2][1], text: i[1],
        level: admin ? "mgr" : (guardLevel > 0 ? "gd" : (medalName ? "fan" : null)),
        medal: medalName ? `${medalName}${medalLevel}` : "",
        guard: guardLevel,
        ts: (i[9] && i[9].ts * 1000) || Date.now()
      });
    } else if (c === "SEND_GIFT") {
      const d = m.data;
      const yuan = d.coin_type === "gold" ? (d.total_coin || 0) / 1000 : 0;
      this.stat.gift++; this.stat.giftValue += yuan;
      this.onEvent({ type: "gift", user: d.uname, gift: d.giftName, num: d.num, value: yuan, free: d.coin_type !== "gold" });
    } else if (c === "GUARD_BUY") {
      const d = m.data;
      const yuan = (d.price || 0) / 1000 * (d.num || 1);
      this.stat.gift++; this.stat.giftValue += yuan;
      this.onEvent({ type: "gift", user: d.username, gift: d.gift_name || "大航海", num: d.num || 1, value: yuan, guard: true });
    } else if (c === "SUPER_CHAT_MESSAGE") {
      const d = m.data;
      this.stat.gift++; this.stat.giftValue += (d.price || 0);
      this.onEvent({ type: "gift", user: d.user_info && d.user_info.uname, gift: "醒目留言 SC", num: 1, value: d.price || 0, sc: true });
      this.onEvent({ type: "danmaku", user: (d.user_info && d.user_info.uname) || "", text: "【SC ¥" + d.price + "】" + d.message, level: "gd" });
    } else if (c === "WATCHED_CHANGE") {
      this.onEvent({ type: "watched", value: m.data.num });
    } else if (c === "LIKE_INFO_V3_UPDATE") {
      this.onEvent({ type: "likes", value: m.data.click_count });
    } else if (c === "ONLINE_RANK_COUNT") {
      this.onEvent({ type: "rankCount", value: m.data.count || m.data.online_count });
    } else if (c === "INTERACT_WORD") {
      const t = m.data.msg_type;
      if (t === 2) this.onEvent({ type: "follow", user: m.data.uname });
      else if (t === 1) this.onEvent({ type: "enter", user: m.data.uname });
    } else if (c === "PREPARING") {
      this.onEvent({ type: "liveEnd" });
    } else if (c === "LIVE") {
      this.onEvent({ type: "liveStart" });
    } else if (c === "ROOM_CHANGE") {
      this.onEvent({ type: "roomChange", title: m.data.title, area: [m.data.parent_area_name, m.data.area_name].filter(Boolean).join(" · ") });
    }
  }
}

/* ============ 房间连接池（多客户端共享一条上游连接） ============ */
const ROOMS = new Map();   // roomId -> { client, subs:Set(res), timer }
function subscribe(roomId, res) {
  let r = ROOMS.get(roomId);
  if (!r) {
    // 原子操作：先占位，防并发创建双客户端
    r = { client: null, subs: new Set(), _promise: null };
    ROOMS.set(roomId, r);
    r.client = new DanmakuClient(roomId, ev => {
      const line = "data: " + JSON.stringify(ev) + "\n\n";
      for (const s of r.subs) { try { s.write(line); } catch (e) {} }
    });
    r.client.connect();
  }
  r.subs.add(res);
  if (r.gcTimer) { clearTimeout(r.gcTimer); r.gcTimer = null; }
  return r;
}
function unsubscribe(roomId, res) {
  const r = ROOMS.get(roomId);
  if (!r) return;
  r.subs.delete(res);
  if (r.subs.size === 0) {
    r.gcTimer = setTimeout(() => {
      if (r.subs.size === 0) { r.client.close(); ROOMS.delete(roomId); log(`房间 ${roomId} 无订阅者，已断开`); }
    }, 15000);
  }
}

/* ============ 直播流转码（FFmpeg → HLS） ============ */
const STREAMS_DIR = path.join(__dirname, "streams");
try { fs.mkdirSync(STREAMS_DIR, { recursive: true }); } catch (e) {}

const FFMPEG_CANDIDATES = [
  path.join(__dirname, "tools", "ffmpeg-master-latest-win64-gpl", "bin", "ffmpeg.exe"),
  path.join(__dirname, "tools", "ffmpeg.exe"),
  "ffmpeg",
  "ffmpeg.exe",
];

let FFMPEG_PATH = null;
function findFFmpeg() {
  if (FFMPEG_PATH) return FFMPEG_PATH;
  // 1. 先在 tools/ 目录下递归搜索
  try {
    const toolsDir = path.join(__dirname, "tools");
    function scanDir(dir, depth) {
      if (depth > 4) return;
      try {
        const entries = fs.readdirSync(dir, { withFileTypes: true });
        for (const e of entries) {
          const full = path.join(dir, e.name);
          if (e.name === "ffmpeg.exe" && e.isFile()) {
            try {
              const r = execSync(`"${full}" -version`, { timeout: 5000, stdio: "pipe" });
              if (r.toString().includes("ffmpeg version")) { FFMPEG_PATH = full; return; }
            } catch (ex) {}
          }
          if (e.isDirectory() && !e.name.startsWith(".")) scanDir(full, depth + 1);
        }
      } catch (ex) {}
    }
    scanDir(toolsDir, 0);
    if (FFMPEG_PATH) { log(`FFmpeg 就绪 → ${FFMPEG_PATH}`); return FFMPEG_PATH; }
  } catch (e) {}
  // 2. 回退固定候选列表和 PATH
  for (const c of FFMPEG_CANDIDATES) {
    try {
      const r = execSync(`"${c}" -version`, { timeout: 5000, stdio: "pipe" });
      if (r.toString().includes("ffmpeg version")) { FFMPEG_PATH = c; log(`FFmpeg 就绪 → ${c}`); return c; }
    } catch (e) {}
  }
  return null;
}

// 房间转码状态: roomId → { proc, url, status, startTime, errors, output }
const TRANSCODES = new Map();

function startTranscode(roomId, url, extraHeaders, sourceRoomUrl) {
  const existing = TRANSCODES.get(roomId);
  if (PUBLIC_MODE && existing && existing.sourceRoomUrl === sourceRoomUrl && ["starting", "transcoding"].includes(existing.status)) {
    return { ok: true, roomId, hlsUrl: `/streams/${roomId}/index.m3u8`, shared: true };
  }
  stopTranscode(roomId);
  if (!FFMPEG_PATH) return { ok: false, reason: "FFmpeg 未安装" };

  const roomDir = path.join(STREAMS_DIR, roomId);
  try { fs.mkdirSync(roomDir, { recursive: true }); } catch (e) {}
  try { fs.readdirSync(roomDir).forEach(f => { try { fs.unlinkSync(path.join(roomDir, f)); } catch (e) {} }); } catch (e) {}

  const hlsPath = path.join(roomDir, "index.m3u8");

  // 抖音 CDN 通常需要完整浏览器 headers (Referer/Origin/Cookie/Accept 等)
  const baseHeaders = [
    "Referer: https://live.douyin.com/",
    "Origin: https://live.douyin.com",
    "Accept: */*",
    "Accept-Language: zh-CN,zh;q=0.9,en;q=0.8",
    "Accept-Encoding: gzip, deflate, br",
    "Sec-Fetch-Dest: empty",
    "Sec-Fetch-Mode: cors",
    "Sec-Fetch-Site: same-site"
  ];
  // 额外 headers（yt-dlp 解出的 Cookie 等）
  const allHeaders = extraHeaders ? [...baseHeaders, ...extraHeaders] : baseHeaders;
  // 安全：过滤换行符防止 header 注入
  const headerStr = allHeaders.map(h => String(h).replace(/[\r\n]/g, "")).join("\r\n");

  const args = [
    "-re", "-user_agent", UA,
    "-headers", headerStr,
    "-multiple_requests", "1",
    "-reconnect", "1",
    "-reconnect_streamed", "1",
    "-reconnect_delay_max", "5",
    "-i", url,
    // 重编码为 H264（源流可能为 HEVC），用 veryfast 避免 ultrafast 导致的解码崩溃
    "-c:v", "libx264", "-preset", "veryfast", "-tune", "zerolatency",
    "-crf", "26", "-g", "60", "-keyint_min", "60",
    "-sc_threshold", "0",
    "-maxrate", "3000k", "-bufsize", "6000k",
    "-vf", "scale=ceil(iw/2)*2:ceil(ih/2)*2,fps=25",
    "-c:a", "aac", "-b:a", "96k", "-ar", "44100", "-ac", "2",
    "-f", "hls", "-hls_time", "4", "-hls_list_size", "10",
    "-hls_flags", "omit_endlist+independent_segments+program_date_time",
    "-hls_segment_type", "mpegts",
    hlsPath
  ];

  // 安全：URL 中过滤换行符防止参数注入
  if (/[\r\n]/.test(url)) {
    return { ok: false, reason: "URL 包含非法字符" };
  }
  log(`[${roomId}] FFmpeg URL: ${url.slice(0, 100)}...`);

  log(`[${roomId}] 开始转码 → ${url.slice(0, 60)}...`);

  try {
    const proc = spawn(FFMPEG_PATH, args, { stdio: ["ignore", "pipe", "pipe"], windowsHide: true });
    const state = { proc, url, sourceUrl: url, sourceRoomUrl: sourceRoomUrl || "", extraHeaders, status: "starting", startTime: Date.now(), errors: [], output: "" };
    TRANSCODES.set(roomId, state);

    proc.stderr.on("data", chunk => {
      const t = chunk.toString();
      state.output = (state.output + t).slice(-3000);
      // 检测关键错误
      if (t.includes("403 Forbidden") || t.includes("404 Not Found") || t.includes("Immediate exit") || t.includes("Server returned 4") || t.includes("Server returned 5")) {
        if (state.errors.length < 3) state.errors.push(t.slice(0, 300));
        if (state.status === "starting" || state.status === "transcoding") state.status = "error";
        log(`[${roomId}] FFmpeg 错误: ${t.slice(0, 200).replace(/\n/g, ' | ')}`);
      }
    });

    // 周期性检查 m3u8 文件是否存在 + 有内容，确保前端不会太早开始播放
    const readyCheck = setInterval(() => {
      if (!TRANSCODES.has(roomId)) { clearInterval(readyCheck); return; }
      try {
        const m3u8Path = path.join(roomDir, "index.m3u8");
        if (fs.existsSync(m3u8Path)) {
          const stat = fs.statSync(m3u8Path);
          if (stat.size > 0) {
            // 检查 m3u8 内容是否至少有一个分片
            const content = fs.readFileSync(m3u8Path, "utf8");
            if (content.includes("#EXTINF") && state.status === "starting") {
              state.status = "transcoding";
              log(`[${roomId}] 转码就绪 (m3u8 ${stat.size}B, 首片已生成)`);
            }
          }
        }
      } catch (e) {}
    }, 1000);
    state.readyCheck = readyCheck;

    proc.on("close", code => {
      clearInterval(readyCheck);
      if (state.status === "stopped") return;
      const wasTranscoding = state.status === "transcoding";
      state.status = code === 0 || state.status === "transcoding" ? "finished" : "error";
      state.proc = null;
      log(`[${roomId}] FFmpeg 退出 (code=${code}, status=${state.status})`);
      // 自动清理
      setTimeout(() => {
        const cur = TRANSCODES.get(roomId);
        if (cur === state && !cur.proc) {
          TRANSCODES.delete(roomId);
          try { fs.rmSync(roomDir, { recursive: true, force: true }); } catch (e) {}
        }
      }, 120000);

      // 自动重启：曾正常转码过但意外非手动停止 → 3秒后自动重启
      if (wasTranscoding && state.status !== "stopped" && state.sourceUrl && !state._autoRestart) {
        state._autoRestart = true;
        log(`[${roomId}] 自动重启转码 → ${state.sourceUrl.slice(0, 80)}...`);
        setTimeout(() => {
          const cur = TRANSCODES.get(roomId);
          if (cur && !cur.proc && cur.sourceUrl) {
            try {
              log(`[${roomId}] 触发重启...`);
              startTranscode(roomId, cur.sourceUrl, cur.extraHeaders || [], cur.sourceRoomUrl || "");
            } catch (e) {
              log(`[${roomId}] 重启失败: ${e.message}`);
            }
          }
        }, 3000);
      }
    });

    proc.on("error", err => {
      state.status = "error"; state.errors.push("无法启动: " + err.message);
      log(`[${roomId}] FFmpeg 进程错误: ${err.message}`);
    });

    return { ok: true, roomId, hlsUrl: `/streams/${roomId}/index.m3u8` };
  } catch (e) {
    return { ok: false, reason: `进程创建失败: ${e.message}` };
  }
}

function stopTranscode(roomId) {
  const state = TRANSCODES.get(roomId);
  if (!state) return { ok: true, message: "无活跃转码" };
  state.status = "stopped";
  // 清理待处理定时器
  if (state.readyCheck) clearInterval(state.readyCheck);
  if (state.killTimer) clearTimeout(state.killTimer);
  if (state.cleanupTimer) clearTimeout(state.cleanupTimer);
  // 先杀进程，不立即删 TRANSCODES（避免并发创建第二个进程）
  if (state.proc) {
    try { state.proc.kill("SIGTERM"); } catch (e) {}
    state.killTimer = setTimeout(() => { try { if (state.proc && !state.proc.killed) state.proc.kill("SIGKILL"); } catch (e) {} }, 3000);
    // 等进程退出后再删除
    state.proc.on("close", () => {
      if (TRANSCODES.get(roomId) !== state) return;
      TRANSCODES.delete(roomId);
      const roomDir = path.join(STREAMS_DIR, roomId);
      setTimeout(() => {
        if (!TRANSCODES.has(roomId)) {
          try { fs.rmSync(roomDir, { recursive: true, force: true }); } catch (e) {}
        }
      }, 1000);
    });
  } else {
    TRANSCODES.delete(roomId);
  }
  log(`[${roomId}] 转码已停止`);
  return { ok: true };
}

function getTranscodeStatus(roomId) {
  const s = TRANSCODES.get(roomId);
  if (!s) return { ok: true, active: false, hasFFmpeg: !!FFMPEG_PATH };
  return {
    ok: true, active: true, status: s.status, url: s.url,
    startTime: s.startTime, hlsUrl: `/streams/${roomId}/index.m3u8`,
    errors: s.errors.slice(-3), hasFFmpeg: true
  };
}

// 启动时检测 FFmpeg
findFFmpeg();
if (!FFMPEG_PATH) log("⚠️ FFmpeg 未检测到 —— 服务端转码不可用。请将 ffmpeg.exe 放到 tools/ 目录，或安装 FFmpeg 后重启服务。");

/* ============ yt-dlp 自动检测（抖音专用解析器） ============ */
let YTDLP_PATH = null;
function findYtdlp() {
  if (YTDLP_PATH) return YTDLP_PATH;
  try {
    const toolsDir = path.join(__dirname, "tools");
    function scanDir(dir, depth) {
      if (depth > 3) return;
      try {
        const entries = fs.readdirSync(dir, { withFileTypes: true });
        for (const e of entries) {
          const full = path.join(dir, e.name);
          if (/^yt-dlp(\.exe)?$/i.test(e.name) && e.isFile()) {
            try {
              const r = execSync(`"${full}" --version`, { timeout: 5000, stdio: "pipe" });
              const v = r.toString().trim();
              if (v.match(/\d+\.\d+/)) { YTDLP_PATH = full; log(`yt-dlp 就绪 → ${full} (v${v})`); return; }
            } catch (ex) {}
          }
          if (e.isDirectory() && !e.name.startsWith(".")) scanDir(full, depth + 1);
        }
      } catch (ex) {}
    }
    scanDir(toolsDir, 0);
  } catch (e) {}
  if (!YTDLP_PATH) {
    try {
      const r = execSync(`yt-dlp --version`, { timeout: 5000, stdio: "pipe" });
      if (r.toString().match(/\d+\.\d+/)) { YTDLP_PATH = "yt-dlp"; log(`yt-dlp 就绪 (PATH)`); }
    } catch (e) {}
  }
  return YTDLP_PATH;
}
findYtdlp();
if (!YTDLP_PATH) log("ℹ️ yt-dlp 未检测到 —— 抖音房间链接将无法自动解析为流地址。可从 https://github.com/yt-dlp/yt-dlp/releases 下载 yt-dlp.exe 到 tools/ 目录。");

/* 用 yt-dlp 解析抖音/B站等直播源 */
async function resolveStream(input) {
  const isDouyinRoom = /live\.douyin\.com\/[0-9]+/i.test(input);
  if (isDouyinRoom) {
    const cached = DOUYIN_RESOLVE_CACHE.get(input);
    if (cached && cached.expiresAt > Date.now()) {
      log(`[resolve] 使用抖音解析缓存: ${input}`);
      return { ...cached.result, via: `${cached.result.via}-cache` };
    }
    const pending = DOUYIN_RESOLVE_PENDING.get(input);
    if (pending) {
      log(`[resolve] 复用正在进行的抖音解析: ${input}`);
      return pending;
    }
    const job = resolveDouyinWebcast(input).then(result => {
      if (result && result.ok) {
        DOUYIN_RESOLVE_CACHE.set(input, { result, expiresAt: Date.now() + 45000 });
        return result;
      }
      return { ok: false, reason: "抖音直播页面暂未返回可用视频流。直播页面可以打开，但当前请求可能被风控为页面空壳；代理已使用会话 Cookie 重试。" };
    }).finally(() => {
      if (DOUYIN_RESOLVE_PENDING.get(input) === job) DOUYIN_RESOLVE_PENDING.delete(input);
    });
    DOUYIN_RESOLVE_PENDING.set(input, job);
    return job;
  }

  if (!YTDLP_PATH) return { ok: false, reason: "yt-dlp 未安装，无法解析房间链接" };

  // 已是流链接（FLV/HLS/MP4/CDN）→ 直接使用，跳过 yt-dlp 避免尝试下载
  const isStreamUrl = /\.(flv|m3u8|hls|mp4|ts)(\?|$)/i.test(input) ||
                      /douyincdn|ksyungslb|custom\.kuaishou|aliyuncs|cdn[.-]/i.test(input);
  if (isStreamUrl) {
    log(`[resolve] 检测到流链接，跳过 yt-dlp: ${input.slice(0, 80)}...`);
    return {
      ok: true,
      url: input,
      title: "直连流",
      uploader: "",
      liveStatus: "is_live",
      extraHeaders: [],
      originalUrl: input,
      via: "direct"
    };
  }

  return new Promise((resolve) => {
    try {
      const args = ["--no-warnings", "--no-progress", "--dump-json", "--no-download", "--user-agent", UA, input];
      log(`[resolve] yt-dlp ${input.slice(0, 60)}...`);

      const proc = spawn(YTDLP_PATH, args, { stdio: ["pipe", "pipe", "pipe"], windowsHide: true });
      let stdout = "", stderr = "";
      proc.stdout.on("data", c => stdout += c);
      proc.stderr.on("data", c => stderr += c);

      proc.on("close", code => {
        if (code === 0 && stdout.trim()) {
          try {
            const data = JSON.parse(stdout);
            const formats = data.formats || [];
            let best = formats.find(f => f.url && /\.m3u8$/i.test(f.url) && f.vcodec !== "none");
            if (!best) best = formats.find(f => f.url && /\.flv$/i.test(f.url) && f.vcodec !== "none");
            if (!best) best = formats.find(f => f.url && /\/play\?/i.test(f.url));
            if (!best) best = formats.find(f => f.url && f.vcodec !== "none" && f.acodec !== "none");

            if (!best || !best.url) {
              return resolve({ ok: false, reason: "yt-dlp 解析成功但未找到可用流（可能房间未开播）" });            }

            let cookieStr = "";
            if (best.http_headers && best.http_headers.Cookie) cookieStr = best.http_headers.Cookie;
            else if (data.http_headers && data.http_headers.Cookie) cookieStr = data.http_headers.Cookie;

            return resolve({
              ok: true,
              url: best.url,
              formatId: best.format_id,
              ext: best.ext,
              title: data.title || data.description || input,
              uploader: data.uploader || data.channel || "",
              liveStatus: data.live_status || "is_live",
              extraHeaders: cookieStr ? [`Cookie: ${cookieStr}`] : [],
              originalUrl: best.url,
              via: "yt-dlp"
            });
          } catch (e) {
            return resolve({ ok: false, reason: `yt-dlp JSON 解析失败: ${e.message}` });
          }
        }

        // 失败：把 stderr 翻译成中文
        const msg = stderr || "";
        let reason = `yt-dlp 解析失败: ${msg.split("\n")[0] || `exit code ${code}`}`;
        if (msg.includes("Unsupported URL")) {
          reason = "抖音返回 404 / 房间不存在。\n\n可能原因：\n① 房间已下播或被封\n② 房间 ID 错误（19 位数字 ID）\n③ 链接不是有效的直播间\n\n请确认链接正确后再试";
        } else if (msg.includes("HTTP Error 404") || msg.includes("Unable to download webpage")) {
          reason = "抖音页面返回 404。\n\n请检查链接：\n① 直播间是否正在开播\n② ID 是否完整正确（19 位数字）\n③ 是否能从浏览器正常打开此链接";
        } else if (msg.includes("Unable to extract")) {
          reason = "yt-dlp 无法提取流信息。\n\n抖音可能更新了页面结构，需要更新 yt-dlp 版本。\n可手动从 F12→Network 复制 .flv URL";
        } else if (msg.toLowerCase().includes("ssl")) {
          reason = "SSL/TLS 连接错误。可能是抖音 CDN 临时故障，请稍后重试";
        } else if (msg.toLowerCase().includes("timeout")) {
          reason = "连接超时。可能是网络问题或抖音 CDN 不可达";
        } else if (msg.includes("Unable to handle request")) {
          reason = "yt-dlp 不识别此 URL 格式。\n\n请确认是抖音直播间链接：\nhttps://live.douyin.com/19位数字";
        }
        log(`[resolve] 失败: ${msg.split("\n")[0]}`);

        // 失败兜底：如果是抖音 URL，尝试自定义解析器
        if (/live\.douyin\.com\/[0-9]+/i.test(input)) {
          log(`[resolve] 尝试自定义抖音解析器...`);
          resolveDouyinWebcast(input).then(fallback => {
            if (fallback) resolve(fallback);
            else resolve({ ok: false, reason, detail: msg.slice(-500) });
          }).catch(err => {
            resolve({ ok: false, reason, detail: `自定义解析失败: ${err.message}` });
          });
          return;
        }

        resolve({ ok: false, reason, detail: msg.slice(-500) });
      });

      proc.on("error", err => {
        resolve({ ok: false, reason: `yt-dlp 进程启动失败: ${err.message}` });
      });
    } catch (e) {
      resolve({ ok: false, reason: `yt-dlp 调用失败: ${e.message}` });
    }
  });
}

/* 自定义抖音解析器（不依赖 yt-dlp，直接抓页面提取 FLV） */
async function resolveDouyinWebcast(url) {
  try {
    const m = url.match(/live\.douyin\.com\/([0-9]+)/i);
    if (!m) return null;
    const roomId = m[1];
    log(`[douyin-custom] 抓取房间 ${roomId} 页面...`);

    const cookieJar = new Map();
    const mergeCookies = setCookies => {
      for (const rawCookie of setCookies || []) {
        const pair = rawCookie.split(";", 1)[0];
        const separator = pair.indexOf("=");
        if (separator > 0) cookieJar.set(pair.slice(0, separator), pair.slice(separator + 1));
      }
    };
    const cookieHeader = () => Array.from(cookieJar, ([name, value]) => `${name}=${value}`).join("; ");
    const requestPage = (targetUrl, redirectsLeft = 5) => new Promise((resolve, reject) => {
      const req = https.get(targetUrl, {
        headers: {
          "User-Agent": UA,
          "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
          "Accept-Language": "zh-CN,zh;q=0.9",
          "Cache-Control": "no-cache",
          "Pragma": "no-cache",
          "Referer": "https://live.douyin.com/",
          "Cookie": cookieHeader()
        },
        timeout: 15000
      }, res => {
        mergeCookies(res.headers["set-cookie"]);
        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          res.resume();
          if (redirectsLeft <= 0) return reject(new Error("重定向次数过多"));
          const redirectUrl = new URL(res.headers.location, targetUrl).toString();
          return requestPage(redirectUrl, redirectsLeft - 1).then(resolve, reject);
        }
        const chunks = [];
        res.on("data", chunk => chunks.push(chunk));
        res.on("end", () => {
          if (res.statusCode !== 200) return reject(new Error(`HTTP ${res.statusCode}`));
          resolve(Buffer.concat(chunks).toString("utf8"));
        });
      });
      req.on("error", reject);
      req.setTimeout(15000, () => req.destroy(new Error("timeout")));
    });

    await requestPage("https://live.douyin.com/");
    if (!cookieJar.has("ttwid")) log("[douyin-custom] 首页未下发 ttwid，继续使用风控兼容 Cookie");
    cookieJar.set("msToken", crypto.randomBytes(80).toString("base64url").slice(0, 107));
    cookieJar.set("__ac_nonce", crypto.randomBytes(7).toString("hex").slice(0, 13));

    try {
      const enterParams = new URLSearchParams({
        aid: "6383",
        app_name: "douyin_web",
        live_id: "1",
        device_platform: "web",
        language: "zh-CN",
        enter_from: "page_refresh",
        cookie_enabled: "true",
        screen_width: "1536",
        screen_height: "864",
        browser_language: "zh-CN",
        browser_platform: "Win32",
        browser_name: "Chrome",
        browser_version: "126.0.0.0",
        web_rid: roomId,
        room_id_str: "",
        enter_source: "",
        is_need_double_stream: "false"
      });
      const enterText = await requestPage(`https://live.douyin.com/webcast/room/web/enter/?${enterParams}`);
      const enterData = JSON.parse(enterText);
      const room = enterData?.data?.data?.[0] || enterData?.data?.room || enterData?.data?.[0];
      const stream = room?.stream_url || enterData?.data?.web_stream_url;
      const flvMap = stream?.flv_pull_url || {};
      const streamCandidates = [
        flvMap.HD1,
        flvMap.SD2,
        flvMap.SD1,
        stream?.rtmp_pull_url,
        flvMap.FULL_HD1,
        flvMap.ORIGION,
        ...Object.values(flvMap)
      ].filter(Boolean);
      const preferredFlv = streamCandidates.find(candidate => /_(?:hd|sd|md|ld)\.flv(?:\?|$)/i.test(candidate)) || streamCandidates[0];
      if (Number(room?.status) === 2 && preferredFlv) {
        const uploader = room?.owner?.nickname || "";
        const title = room?.title || (uploader ? `${uploader} 的直播间` : `抖音直播 ${roomId.slice(-6)}`);
        log(`[douyin-custom] web/enter 命中: roomId=${room?.id_str || room?.id || ""} uploader="${uploader}"`);
        return {
          ok: true,
          url: preferredFlv,
          formatId: "douyin-h264-hd",
          ext: "flv",
          title,
          uploader,
          fans: 0,
          roomTag: "",
          liveStatus: "is_live",
          realRoomId: String(room?.id_str || room?.id || ""),
          extraHeaders: cookieHeader() ? [`Cookie: ${cookieHeader()}`] : [],
          originalUrl: preferredFlv,
          via: "douyin-web-enter"
        };
      }
      log(`[douyin-custom] web/enter 未返回在播流: status=${room?.status ?? enterData?.data?.room_status ?? "unknown"}`);
    } catch (enterError) {
      log(`[douyin-custom] web/enter 失败，回退页面解析: ${enterError.message}`);
    }

    let html = "";
    let cleanHtml = "";
    let matches = [];
    for (let attempt = 1; attempt <= 3; attempt += 1) {
      html = await requestPage(url);
      cleanHtml = html.replace(/\\u0026/g, "&").replace(/\\\//g, "/");
      const flvRegex = /https?:\/\/[^\s"'<>]*?douyincdn\.com\/[^\s"'<>]*?\.flv[^\s"'<>]*/gi;
      matches = cleanHtml.match(flvRegex) || [];
      if (matches.length) break;
      log(`[douyin-custom] 第 ${attempt}/3 次仅返回页面空壳 (${html.length}B)，携带会话 Cookie 重试`);
    }
    const cookies = cookieHeader();

    // 提取所有 FLV URL
    if (!matches.length) {
      log(`[douyin-custom] 页面未找到 FLV URL，下播或私密场`);
      return null;
    }

    // 优先级：H264 优先（浏览器可解码），画质次之
    // 抖音 URL 命名规律：
    //   _md.flv   → H264（中等）
    //   _hd.flv   → H264（高清，老格式）
    //   _hd5.flv  → HEVC（高清，新格式） → 浏览器不能直播
    //   _sd5.flv  → HEVC
    //   _ld5.flv  → HEVC
    //   _or4.flv  → HEVC（原画）
    //   .flv 无后缀 → HEVC（原画）
    const isH264Candidate = (u) => {
      // 必须有 _X.flv 后缀，且不是 _X5/_or4
      const m = u.match(/[_\/]([a-z0-9]+)\.flv/i);
      if (!m) return false;
      const quality = m[1];
      // _md 或 _hd 或 _sd 或 _ld（无 5）= H264
      return /^(md|hd|sd|ld)$/.test(quality);
    };
    const qualityRank = (u) => {
      const m = u.match(/[_\/]([a-z0-9]+)\.flv/i);
      const q = m ? m[1] : "";
      const ranks = { hd: 5, sd: 4, md: 3, ld: 2, or4: 6, hd5: 100, sd5: 100, ld5: 100 };
      return ranks[q] || (q === "" ? 100 : 0);  // 无后缀 or _5 = HEVC
    };
    matches.sort((a, b) => {
      const aH264 = isH264Candidate(a) ? 1 : 0;
      const bH264 = isH264Candidate(b) ? 1 : 0;
      if (aH264 !== bH264) return bH264 - aH264;
      return qualityRank(b) - qualityRank(a);
    });
    log(`[douyin-custom] 候选 ${matches.length} 个: ${matches.slice(0,5).map(u => u.match(/[\/_]([^_/.]*?)\.flv/)?.[1] || 'base').join(',')}`);
    const bestFlv = matches[0];
    const cleanUrl = bestFlv.replace(/\\+$/, "");
    log(`[douyin-custom] 完整 URL: ${cleanUrl}`);
    log(`[douyin-custom] 候选: ${matches.length} 个 | 首选: ${isH264Candidate(bestFlv) ? 'H264(_md)' : 'HEVC'}`);

    // 提取房间元数据
    let title = "", uploader = "", roomTag = "", fans = 0;

    // 1. 主播昵称：抖音 HTML 有两种格式（裸 JSON 和 HTML 转义），都要匹配
    const nickRegexes = [
      /(?:^|[^\\])"(?:nickname|anchor_nickname|user_name)"\s*:\s*"((?:[^"\\]|\\.){2,40})"/g,
      /&quot;(?:nickname|anchor_nickname)&quot;\s*:\s*&quot;((?:[^&]|&(?!quot;)){2,40})&quot;/g
    ];
    for (const re of nickRegexes) {
      let nm;
      while ((nm = re.exec(html)) !== null) {
        let candidate = nm[1]
          .replace(/\\"/g, '"').replace(/&quot;/g, '"').replace(/\\\\/g, "\\");
        // 跳过占位和无意义值
        if (/^[$]?\w*undef|^null$|^undefined$|^\s*$/.test(candidate)) continue;
        // 跳过广告/页脚关键词
        if (/广告|协议|政策|帮助|反馈|关于|下载|联系|营业|执照|站点|友情|找回|服务|违规/.test(candidate)) continue;
        uploader = candidate;
        break;
      }
      if (uploader) break;
    }
    // 2. 粉丝数
    const fansMatch = html.match(/"(?:followerCount|follower_count|fansCount|fans_count)"\s*:\s*"?(\d+)"?/);
    if (fansMatch) fans = parseInt(fansMatch[1]) || 0;

    // 3. 标题兜底：抖音静态 HTML 没有真实房间标题（JS 渲染）
    if (uploader) {
      title = `${uploader} 的直播间`;
    } else {
      const idMatch = url.match(/live\.douyin\.com\/(\d+)/);
      title = idMatch ? `抖音直播 ${idMatch[1].slice(-6)}` : "抖音直播";
    }
    log(`[douyin-custom] 抓取到: uploader="${uploader}" | title="${title.slice(0, 30)}" | fans=${fans}`);

    log(`[douyin-custom] ✓ 找到 FLV: ${cleanUrl.slice(0, 80)}...`);

    return {
      ok: true,
      url: cleanUrl,
      title: title || "抖音直播",
      uploader,
      fans,
      roomTag,
      liveStatus: "is_live",
      extraHeaders: cookies ? [`Cookie: ${cookies}`] : [],
      originalUrl: cleanUrl,
      via: "douyin-custom"
    };
  } catch (e) {
    log(`[douyin-custom] 失败: ${e.message}`);
    return null;
  }
}

/* ============ HTTP 服务 ============ */
const MIME = { ".html": "text/html; charset=utf-8", ".js": "text/javascript; charset=utf-8", ".css": "text/css; charset=utf-8", ".json": "application/json; charset=utf-8", ".ico": "image/x-icon", ".png": "image/png", ".svg": "image/svg+xml", ".m3u8": "application/vnd.apple.mpegurl", ".ts": "video/mp2t" };
function cors(res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Headers", "*");
}
function json(res, code, obj) {
  cors(res);
  res.writeHead(code, { "Content-Type": "application/json; charset=utf-8", "Cache-Control": "no-store" });
  res.end(JSON.stringify(obj));
}

const server = http.createServer(async (req, res) => {
  const u = new URL(req.url, "http://localhost");
  const p = u.pathname;
  if (req.method === "OPTIONS") { cors(res); res.writeHead(204); return res.end(); }

  /* --- 通用的安全请求体读取 --- */
  function readSafeBody(maxBytes = 524288) {
    return new Promise((resolve) => {
      let body = "";
      let len = 0;
      req.on("data", c => { len += c.length; if (len <= maxBytes) body += c; else req.destroy(); });
      req.on("end", () => resolve(len > maxBytes ? null : body));
    });
  }

  /* --- 健康检查 --- */
  if (p === "/api/health") return json(res, 200, {
    ok: true,
    service: "livewatch-proxy",
    version: "3.1",
    publicMode: PUBLIC_MODE,
    rooms: ROOMS.size,
    transcodes: TRANSCODES.size,
    hasFFmpeg: !!FFMPEG_PATH,
    hasYtdlp: !!YTDLP_PATH,
    xunfeiConfigured: Boolean(SERVER_XUNFEI_CONFIG.appId && SERVER_XUNFEI_CONFIG.apiKey && SERVER_XUNFEI_CONFIG.apiSecret)
  });

  /* --- 当前在播的推荐房间（给「快速试用」用） --- */
  if (p === "/api/hot") {
    const CAND = [7734200, 6136246, 545068, 8792912, 1017, 5441, 21452505, 22637261, 23058, 47867,
                  1029, 21013446, 22389319, 80397, 3819533, 510, 3, 6, 21144080, 25788785, 27183290,
                  1900141, 22384516, 24393316, 5050, 336037];
    try {
      const bv = await getBuvid();
      const q = CAND.map(i => "room_ids=" + i).join("&");
      const j = await getJSON(`https://api.live.bilibili.com/xlive/web-room/v1/index/getRoomBaseInfo?${q}&req_biz=web`,
        { Referer: "https://live.bilibili.com/", Cookie: `buvid3=${bv.b3}` });
      const all = Object.values((j.data && j.data.by_room_ids) || {});
      const list = all.filter(v => v.live_status === 1)
        .sort((a, b) => (b.online || 0) - (a.online || 0)).slice(0, 4)
        .map(v => ({ roomId: v.room_id, uname: v.uname, title: v.title, online: v.online, area: v.area_name }));
      return json(res, 200, { ok: true, list });
    } catch (e) { return json(res, 200, { ok: false, list: [], reason: e.message }); }
  }

  /* --- 房间信息 --- */
  /* --- 抖音弹幕 SSE --- */
  if (p === "/api/dy-danmaku") {
    const rurl = u.searchParams.get("url") || "";
    const rid = u.searchParams.get("roomId") || "dy_" + Date.now();
    if (!rurl) return json(res, 400, { ok: false, reason: "缺少 url 参数" });
    cors(res);
    res.setHeader("Content-Type", "text/event-stream; charset=utf-8");
    res.setHeader("Cache-Control", "no-cache, no-transform");
    res.setHeader("Connection", "keep-alive");
    res.setHeader("X-Accel-Buffering", "no");
    res.flushHeaders();
    // 立即写多行确保浏览器识别为流
    res.write(": connected at " + new Date().toISOString() + "\n");
    res.write(": heartbeat=" + Date.now() + "\n");
    res.write("data: " + JSON.stringify({type:"connected",t:Date.now()}) + "\n\n");
    if (res.socket) res.socket.setNoDelay(true);
    if (res.socket) res.socket.setKeepAlive(true, 30000);
    // 心跳（每 30 秒发一条，避免代理/浏览器断开）
    const ka = setInterval(() => {
      try { res.write(": ka " + Date.now() + "\n\n"); } catch (e) {}
    }, 30000);
    res.on("close", () => { clearInterval(ka); });
    startDyDanmaku(rid, rurl, res);
    return;
  }

  if (p === "/api/room") {
    const t = parseUrl(u.searchParams.get("url") || u.searchParams.get("room") || "");
    if (t.platform !== "bilibili" || !t.roomId) {
      return json(res, 200, { ok: false, platform: t.platform, roomId: t.roomId, reason: "该平台需要额外的签名/风控处理，代理当前仅实现 B 站真实数据" });
    }
    try {
      const data = await fetchRoom(t.roomId);
      return json(res, 200, { ok: true, platform: "bilibili", data });
    } catch (e) {
      return json(res, 200, { ok: false, platform: "bilibili", roomId: t.roomId, reason: e.message });
    }
  }

  /* --- SSE 实时流 --- */
  if (p === "/api/stream") {
    const t = parseUrl(u.searchParams.get("room") || u.searchParams.get("url") || "");
    if (t.platform !== "bilibili" || !t.roomId) return json(res, 400, { ok: false, reason: "仅支持 B 站房间" });
    cors(res);
    res.writeHead(200, { "Content-Type": "text/event-stream; charset=utf-8", "Cache-Control": "no-cache", "Connection": "keep-alive", "X-Accel-Buffering": "no" });
    res.write("retry: 3000\n\n");
    res.write("data: " + JSON.stringify({ type: "hello", room: t.roomId }) + "\n\n");
    subscribe(t.roomId, res);
    const ka = setInterval(() => { try { res.write(": ka\n\n"); } catch (e) {} }, 20000);
    req.on("close", () => { clearInterval(ka); unsubscribe(t.roomId, res); });
    return;
  }

  /* --- 服务端转码 API --- */
  if (p === "/api/transcode/start" && req.method === "POST") {
    let body = ""; req.on("data", c => body += c);
    return req.on("end", async () => {
      try {
        const { url, roomId, sessionId, activatedAt } = JSON.parse(body);
        if (!url || !roomId) return json(res, 400, { ok: false, reason: "缺少 url 或 roomId" });
        const publicRoom = validatePublicRoom(roomId, url);
        if (!publicRoom.ok) return json(res, 403, publicRoom);
        const safeRoomUrl = publicRoom.url;
        const sessionClaim = claimDualSession(roomId, sessionId, activatedAt);
        if (!sessionClaim.ok) return json(res, 409, sessionClaim);
        if (!FFMPEG_PATH) return json(res, 503, { ok: false, reason: "FFmpeg 未安装。请安装 FFmpeg 或将 ffmpeg.exe 放到 tools/ 目录后重启代理。", hasFFmpeg: false });

        // 检测是否为房间链接（需要解析为实际媒体流）
        const isRoomUrl = /live\.douyin\.com\/|live\.bilibili\.com\/|live\.kuaishou\.com\//.test(safeRoomUrl)
          && !/\.(flv|m3u8|ts)(\?|$)/i.test(safeRoomUrl);

        let streamUrl = safeRoomUrl;
        let extraHeaders = [];
        let resolved = null;
        if (isRoomUrl && YTDLP_PATH) {
          log(`[${roomId}] 房间链接 → yt-dlp 解析`);
          resolved = await resolveStream(safeRoomUrl);
          if (!resolved.ok) return json(res, 502, { ok: false, reason: resolved.reason, detail: resolved.detail });
          streamUrl = resolved.url;
          extraHeaders = resolved.extraHeaders || [];
          log(`[${roomId}] yt-dlp 解析成功: ${resolved.formatId} (${resolved.ext})`);
        } else if (isRoomUrl && !YTDLP_PATH) {
          return json(res, 503, { ok: false, reason: "检测到房间链接但 yt-dlp 未安装。请提供直 FLV/HLS 地址，或下载 yt-dlp.exe 到 tools/ 目录后重启。" });
        }

        // 抖音 CDN 通常不提供浏览器跨域 FLV 读取头，统一经本地 FFmpeg 转成同源 HLS。
        // 这样页面能稳定播放，也能避免播放器只显示“已解析”但实际拿不到首帧。
        if (/\.flv(\?|$)/i.test(streamUrl) || streamUrl.includes("douyincdn")) {
          log(`[${roomId}] FLV 源流 → 强制本地转码，避免 CDN CORS 拦截`);
        }

        const r = startTranscode(roomId, streamUrl, extraHeaders, safeRoomUrl);
        if (r.ok && resolved) {
          r.resolvedFrom = safeRoomUrl;
          r.title = resolved.title;
          r.uploader = resolved.uploader;
        }
        return json(res, r.ok ? 200 : 500, r);
      } catch (e) { return json(res, 400, { ok: false, reason: e.message }); }
    });
  }

  /* --- 独立 yt-dlp 解析 API --- */
  if (p === "/api/resolve" && req.method === "POST") {
    let body = ""; req.on("data", c => body += c);
    return req.on("end", async () => {
      try {
        const { url } = JSON.parse(body);
        if (!url) return json(res, 400, { ok: false, reason: "缺少 url" });
        const publicResolve = validatePublicResolve(url);
        if (!publicResolve.ok) return json(res, 403, publicResolve);
        const r = await resolveStream(publicResolve.url);
        return json(res, r.ok ? 200 : 502, r);
      } catch (e) { return json(res, 400, { ok: false, reason: e.message }); }
    });
  }

  if (p === "/api/transcode/stop" && req.method === "POST") {
    let body = ""; req.on("data", c => body += c);
    return req.on("end", () => {
      try {
        const { roomId } = JSON.parse(body);
        if (PUBLIC_MODE) return json(res, 200, { ok: true, shared: true, message: "公网共享转码保持运行" });
        return json(res, 200, stopTranscode(roomId));
      } catch (e) { return json(res, 400, { ok: false, reason: e.message }); }
    });
  }

  if (p === "/api/transcode/status") {
    const roomId = u.searchParams.get("roomId") || "";
    if (!roomId) return json(res, 400, { ok: false, reason: "缺少 roomId" });
    return json(res, 200, getTranscodeStatus(roomId));
  }

  if (p === "/api/transcode/list") {
    const list = [];
    TRANSCODES.forEach((v, k) => list.push({ roomId: k, status: v.status, url: v.url, startTime: v.startTime }));
    return json(res, 200, { ok: true, list, hasFFmpeg: !!FFMPEG_PATH, hasYtdlp: !!YTDLP_PATH });
  }

  /* --- 静态文件 --- */
  // 提前阻止路径遍历：禁止 ../ 和 ~
  if (p.includes("..") || p.includes("~")) { res.writeHead(403); return res.end("forbidden"); }
  let file = p === "/" ? "/live-monitor-v3.html" : p;
  file = path.join(__dirname, path.normalize(file).replace(/^([/\\])+/, ""));
  if (!file.startsWith(__dirname)) { res.writeHead(403); return res.end("forbidden"); }
  fs.readFile(file, (err, buf) => {
    if (err) { res.writeHead(404, { "Content-Type": "text/plain; charset=utf-8" }); return res.end("404 Not Found"); }
    cors(res);
    const ext = path.extname(file).toLowerCase();
    res.setHeader("Cache-Control", "no-cache, no-store, must-revalidate");
    res.setHeader("Pragma", "no-cache");
    res.setHeader("Expires", "0");
    res.writeHead(200, { "Content-Type": MIME[ext] || "application/octet-stream" });
    res.end(buf);
  });
});

/* ===== WebSocket 中继：抖音弹幕 ===== */
const dyWss = new WebSocket.Server({ noServer: true });
server.on("upgrade", (req, socket, head) => {
  const u = new URL(req.url, "http://localhost");
  if (u.pathname === "/ws/douyin") {
    dyWss.handleUpgrade(req, socket, head, ws => dyWss.emit("connection", ws, req));
  } else if (u.pathname === "/ws/xunfei") {
    wss.handleUpgrade(req, socket, head, ws => wss.emit("connection", ws, req));
  } else {
    socket.destroy();
  }
});
dyWss.on("connection", (ws, req) => {
  const u = new URL(req.url, "http://localhost");
  const requestedUrl = u.searchParams.get("url") || "";
  const rid = u.searchParams.get("roomId") || "dy_" + Date.now();
  const sessionId = u.searchParams.get("sessionId") || "";
  const activatedAt = u.searchParams.get("activatedAt") || "0";
  if (!requestedUrl) { ws.send(JSON.stringify({type:"error",message:"缺少 url 参数"})); ws.close(); return; }
  const publicRoom = validatePublicRoom(rid, requestedUrl);
  if (!publicRoom.ok) {
    ws.send(JSON.stringify({ type: "error", message: publicRoom.reason }));
    ws.close(4003, "room forbidden");
    return;
  }
  const rurl = publicRoom.url;
  const sessionClaim = claimDualSession(rid, sessionId, activatedAt);
  if (!sessionClaim.ok) {
    ws.send(JSON.stringify({ type: "error", message: sessionClaim.reason }));
    ws.close(4002, "stale dashboard session");
    return;
  }
  console.log("[douyin-ws] 前端连接:", rid);
  let roomId = rid;
  let entry = DY_DANMAKU.get(roomId);
  if (entry && entry.roomUrl !== rurl) {
    console.log(`[douyin-ws] 房间地址已更换，停止旧客户端: ${entry.roomUrl} → ${rurl}`);
    if (entry._gcTimer) clearTimeout(entry._gcTimer);
    for (const oldSocket of entry.sockets || []) {
      try { oldSocket.close(4001, "room url changed"); } catch (e) {}
    }
    try { entry.client.stop(); } catch (e) {}
    if (DY_DANMAKU.get(roomId) === entry) DY_DANMAKU.delete(roomId);
    entry = null;
  }
  if (entry) {
    if (!entry.sockets) entry.sockets = new Set();
    if (entry._gcTimer) { clearTimeout(entry._gcTimer); entry._gcTimer = null; }
  }
  if (!entry) {
    const newEntry = { client: null, roomUrl: rurl, subs: new Set(), sockets: new Set() };
    const client = new DouyinDanmakuClient(rurl, ev => {
      const line = JSON.stringify(ev);
      for (const s of newEntry.sockets) { try { if (s.readyState === WebSocket.OPEN) s.send(line); } catch (e) {} }
    });
    newEntry.client = client;
    entry = newEntry;
    DY_DANMAKU.set(roomId, entry);
    client.start();
  }
  const socketEntry = entry;
  socketEntry.sockets.add(ws);
  ws.send(JSON.stringify({type:"proxy_connected",t:Date.now(),roomId,roomUrl:rurl}));
  ws.on("close", () => {
    socketEntry.sockets.delete(ws);
    if (socketEntry.sockets.size === 0 && socketEntry.subs.size === 0) {
      socketEntry._gcTimer = setTimeout(() => {
        if (DY_DANMAKU.get(roomId) !== socketEntry) return;
        try { socketEntry.client.stop(); } catch(e){}
        DY_DANMAKU.delete(roomId);
        console.log(`[douyin] 弹幕客户端已清理: ${roomId} (${socketEntry.roomUrl})`);
      }, 30000);
    }
  });
  ws.on("error", err => console.error("[douyin-ws] 错误:", err.message));
});

/* ===== WebSocket 中继：讯飞 ASR ===== */
const wss = new WebSocket.Server({ noServer: true });
wss.on("connection", (clientWS, req) => {
  console.log("[xunfei-ws] 前端已连接");
  let xf = null, configReceived = false;

  clientWS.on("error", (err) => {
    console.error("[xunfei-ws] 客户端 WS 错误:", err.message);
    if (xf) { try { xf.end(); xf.close(); } catch (e) {} xf = null; }
    try { clientWS.close(); } catch (e) {}
  });

  clientWS.on("message", (data) => {
    try {
      // 第一条消息是 JSON 配置
      if (!configReceived) {
        let cfg = {};
        try { cfg = JSON.parse(data.toString()); } catch (error) { cfg = {}; }
        const serverConfigured = SERVER_XUNFEI_CONFIG.appId && SERVER_XUNFEI_CONFIG.apiKey && SERVER_XUNFEI_CONFIG.apiSecret;
        if (serverConfigured) cfg = SERVER_XUNFEI_CONFIG;
        if (!cfg.appId || !cfg.apiKey || !cfg.apiSecret) {
          clientWS.send(JSON.stringify({ type: "error", message: "讯飞 ASR 未配置。服务器需设置 XUNFEI_APP_ID、XUNFEI_API_KEY、XUNFEI_API_SECRET" }));
          clientWS.close();
          return;
        }
        configReceived = true;
        console.log(`[xunfei-ws] 使用${serverConfigured ? "服务器" : "浏览器"}配置连接讯飞...`);
        if (!XunfeiRTASR) {
          clientWS.send(JSON.stringify({ type: "error", message: "讯飞模块未加载" }));
          clientWS.close();
          return;
        }
        try {
          xf = new XunfeiRTASR(cfg.appId, cfg.apiKey, cfg.apiSecret);
        } catch (err) {
          clientWS.send(JSON.stringify({ type: "error", message: "讯飞初始化失败: " + err.message }));
          clientWS.close();
          return;
        }
        xf.onReady = () => {
          if (clientWS.readyState === WebSocket.OPEN) clientWS.send(JSON.stringify({ type: "ready" }));
        };
        xf.onResult = (text, isFinal, meta = {}) => {
          if (clientWS.readyState === WebSocket.OPEN) clientWS.send(JSON.stringify({
            type: "result", text, isFinal,
            segmentId: meta.segmentId ?? null,
            resultType: meta.resultType ?? 1
          }));
        };
        xf.onError = (err) => {
          if (clientWS.readyState === WebSocket.OPEN) clientWS.send(JSON.stringify({ type: "error", message: err.message }));
        };
        xf.onClose = () => {
          if (clientWS.readyState === WebSocket.OPEN) clientWS.send(JSON.stringify({ type: "closed" }));
        };
        xf.connect();
        return;
      }
      // 后续消息是 PCM 音频数据（纯二进制，无帧头，直接转发）
      // 堆积到 1280 字节（40ms @ 16kHz）再发，匹配讯飞推荐间隔
      if (xf && (Buffer.isBuffer(data) || data instanceof ArrayBuffer || data instanceof Uint8Array)) {
        const buf = Buffer.isBuffer(data) ? data : Buffer.from(data);
        if (!xf._buf) xf._buf = Buffer.alloc(0);
        xf._buf = Buffer.concat([xf._buf, Buffer.from(buf.buffer, buf.byteOffset, buf.byteLength)]);
        // 每攒够 6400 字节（200ms）发一次，平衡延迟和效率
        if (xf._buf.length >= 6400) {
          xf.sendAudio(xf._buf.buffer.slice(xf._buf.byteOffset, xf._buf.byteOffset + xf._buf.length));
          if (xf._dbgCount === undefined) xf._dbgCount = 0;
          xf._dbgCount++;
          if (xf._dbgCount % 20 === 0) console.log(`[xunfei-ws] 已转发 ${xf._dbgCount} 批音频，累积 ${Math.round(xf._buf.length*20/1024)} KB`);
          xf._buf = Buffer.alloc(0);
        }
      }
    } catch (e) {
      console.error("[xunfei-ws] 消息处理异常:", e.message);
    }
  });

  clientWS.on("close", () => {
    console.log("[xunfei-ws] 前端断开");
    if (xf) { xf.end(); setTimeout(() => xf.close(), 500); }
  });
});

server.listen(PORT, () => {
  console.log("");
  console.log("  ┌────────────────────────────────────────────────┐");
  console.log("  │   LiveWatch 直播监控看板 · 本地数据代理已启动   │");
  console.log("  └────────────────────────────────────────────────┘");
  console.log("");
  console.log("   看板地址   →  http://localhost:" + PORT);
  console.log("   数据能力   →  B站真实弹幕 / 礼物 / 人气 / 点赞 / 上舰 / SC");
  console.log("   停止服务   →  Ctrl + C");
  console.log("");
});
process.on("uncaughtException", e => {
  log("致命异常:", e.message);
  console.error(e.stack);
  server.close(() => process.exit(1));
  setTimeout(() => process.exit(1), 10000);
});
process.on("unhandledRejection", (reason) => {
  log("未处理的 Promise Rejection:", reason?.message || reason);
  console.error(reason?.stack);
});

/* ===== 优雅关闭 ===== */
function shutdown(signal) {
  log(`收到 ${signal}，正在清理...`);
  for (const [roomId, state] of TRANSCODES) {
    try { if (state.proc) { state.proc.kill("SIGTERM"); setTimeout(() => { try { state.proc.kill("SIGKILL"); } catch(e){} }, 2000); } } catch (e) {}
  }
  for (const [roomId, entry] of ROOMS) {
    try { entry.client.close(); } catch (e) {}
  }
  for (const [roomId, entry] of DY_DANMAKU) {
    try { entry.client.stop(); } catch (e) {}
  }
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 5000);
}
process.on("SIGINT", () => shutdown("SIGINT"));
process.on("SIGTERM", () => shutdown("SIGTERM"));
