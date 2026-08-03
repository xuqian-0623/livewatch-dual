// DouyinDanmakuClient - 抖音直播弹幕 WebSocket 客户端
// 基于 DouyinLiveWebFetcher Python 方案，用 Node.js 重写
//
// 用法:
//   const client = new DouyinDanmakuClient("214698741282", callback);
//   client.start();
//
// 依赖: npm install ws protobufjs zlib (zlib 是 Node 内置)

const https = require("https");
const WebSocket = require("ws");
const crypto = require("crypto");
const zlib = require("zlib");


// 启动时延迟加载 proto（从 douyin.proto 文件）
const path = require("path");
let _protoTextCache = null;
function getProtoText() {
  if (_protoTextCache !== null) return _protoTextCache;
  try {
    _protoTextCache = require("fs").readFileSync(path.join(__dirname, "douyin.proto"), "utf8");
  } catch (e) {
    console.error("[dy-danmaku] 无法读取 douyin.proto:", e.message);
    _protoTextCache = "";
  }
  return _protoTextCache;
}

const UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36";

function randomStr(length = 107) {
  const chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
  let result = "";
  for (let i = 0; i < length; i++) result += chars[Math.floor(Math.random() * chars.length)];
  return result;
}

function parseUrlParams(url) {
  const q = url.includes("?") ? url.split("?")[1] : url;
  const params = {};
  q.split("&").forEach(p => { const [k, v] = p.split("="); if (k) params[k] = decodeURIComponent(v || ""); });
  return params;
}

class DouyinDanmakuClient {
  constructor(roomUrl, onEvent) {
    this.roomUrl = roomUrl;           // 如 "https://live.douyin.com/214698741282"
    this.onEvent = onEvent;           // (event) => {}
    this.ws = null;
    this.closed = false;
    this._ttwid = null;
    this._roomId = null;
    this._proto = null;
    this._hbTimer = null;
    this._reconnectTimer = null;
    this._inactivityTimer = null;
    this.status = "idle";
    this.connectedAt = 0;
    this.lastMessageAt = 0;
    this.lastEventAt = 0;
    this.lastError = "";
    this.reconnectCount = 0;
  }

  async start() {
    this.closed = false;
    this.status = "starting";
    try {
      // 1. 获取 ttwid + roomId
      await this._fetchRoomInfo();
      // 2. 加载 protobuf
      await this._loadProto();
      // 3. 连接 WebSocket
      this._connectWS();
    } catch (e) {
      this.status = "error";
      this.lastError = e.message;
      this.onEvent({ type: "error", message: "启动失败: " + e.message });
      this._scheduleReconnect(true);
    }
  }

  stop() {
    this.closed = true;
    this.status = "stopped";
    if (this._hbTimer) { clearInterval(this._hbTimer); this._hbTimer = null; }
    if (this._reconnectTimer) { clearTimeout(this._reconnectTimer); this._reconnectTimer = null; }
    if (this._inactivityTimer) { clearInterval(this._inactivityTimer); this._inactivityTimer = null; }
    if (this.ws) { try { this.ws.close(); } catch (e) {} this.ws = null; }
  }

  _scheduleReconnect(refreshRoomInfo = false) {
    if (this.closed || this._reconnectTimer) return;
    this.reconnectCount += 1;
    this.status = "reconnecting";
    const delay = Math.min(30000, 2000 * Math.pow(2, Math.min(this.reconnectCount - 1, 4)));
    this._reconnectTimer = setTimeout(async () => {
      this._reconnectTimer = null;
      if (this.closed) return;
      if (refreshRoomInfo) {
        this._ttwid = null;
        this._roomId = null;
      }
      await this.start();
    }, delay);
  }

  /* ====== 内部方法 ====== */

  async _fetchRoomInfo() {
    // Step 1: 获取 ttwid
    if (!this._ttwid) {
      const ttwid = await this._httpGet("https://live.douyin.com/", {
        "User-Agent": UA
      }, true);
      this._ttwid = ttwid;
      console.log("[dy-danmaku] ttwid =", this._ttwid?.slice(0, 20) + "...");
    }

    // Step 2: 通过 web/enter 获取内部 roomId。直接抓直播页会间歇返回风控空壳。
    if (!this._roomId) {
      const webRidMatch = this.roomUrl.match(/live\.douyin\.com\/(\d+)/i);
      if (!webRidMatch) throw new Error("直播间地址中未找到 web_rid");
      const webRid = webRidMatch[1];
      const msToken = randomStr(107);
      const cookie = `ttwid=${this._ttwid}; msToken=${msToken}; __ac_nonce=01234567abcde`;
      const params = new URLSearchParams({
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
        web_rid: webRid,
        room_id_str: "",
        enter_source: "",
        is_need_double_stream: "false"
      });
      const text = await this._httpGet(`https://live.douyin.com/webcast/room/web/enter/?${params}`, {
        "User-Agent": UA,
        "Referer": this.roomUrl,
        "Accept": "application/json, text/plain, */*",
        "Cookie": cookie
      }, false);
      let room;
      try {
        const data = JSON.parse(text);
        room = data?.data?.data?.[0] || data?.data?.room || data?.data?.[0];
      } catch (error) {
        throw new Error("web/enter 返回非 JSON 数据");
      }
      this._roomId = String(room?.id_str || room?.id || "");
      if (!this._roomId) throw new Error("web/enter 未返回内部 roomId");
      console.log("[dy-danmaku] roomId =", this._roomId);
    }
  }

  _httpGet(url, headers = {}, cookieOnly = false) {
    return new Promise((resolve, reject) => {
      const req = https.get(url, { headers, timeout: 15000 }, res => {
        if (res.statusCode >= 300 && res.statusCode < 400) {
          return this._httpGet(res.headers.location, headers, cookieOnly).then(resolve, reject);
        }
        if (cookieOnly) {
          const setCookie = res.headers["set-cookie"] || [];
          const ttwid = setCookie.find(c => c.startsWith("ttwid="));
          return resolve(ttwid ? ttwid.split(";")[0].replace("ttwid=", "") : null);
        }
        let body = "";
        res.on("data", c => body += c);
        res.on("end", () => resolve(body));
      });
      req.on("error", reject);
    });
  }

  async _loadProto() {
    const protobuf = require("protobufjs");
    const protoText = getProtoText();
    if (!protoText) throw new Error("proto 文件未找到");
    const root = await protobuf.parse(protoText, { keepCase: true });
    this._proto = root.root;
    this._protobuf = protobuf;
  }

  _generateSignature(wssUrl) {
    // 用 Node VM 加载 sign.js 并调用 get_sign
    const params = [
      "live_id", "aid", "version_code", "webcast_sdk_version",
      "room_id", "sub_room_id", "sub_channel_id", "did_rule",
      "user_unique_id", "device_platform", "device_type", "ac", "identity"
    ];
    const wssParams = parseUrlParams(wssUrl);
    const tpl = params.map(p => `${p}=${wssParams[p] || ""}`).join(",");
    const md5 = crypto.createHash("md5").update(tpl).digest("hex");

    // sign.js 需要 document/window/navigator 全局对象
    const fs = require("fs");
    const signJs = fs.readFileSync(__dirname + "/douyin-sign.js", "utf8");
    const vm = require("vm");
    const sandbox = {
      document: { cookie: "" },
      window: {},
      navigator: { userAgent: UA },
      console: { log: () => {} },
      get_sign: null
    };
    const ctx = vm.createContext(sandbox);
    vm.runInContext(signJs, ctx, { timeout: 30000 });
    const getSign = sandbox.get_sign;
    if (!getSign) throw new Error("sign.js 未导出 get_sign 函数");
    return getSign(md5);
  }

  _connectWS() {
    if (this.closed || (this.ws && [WebSocket.CONNECTING, WebSocket.OPEN].includes(this.ws.readyState))) return;
    this.closed = false;
    this.status = "connecting";
    const did_rule = "3";
    const user_unique_id = String(Math.floor(Math.random() * 1e16));
    const cursor = `d-1_u-1_fh-${Date.now()}000_t-${Date.now()}000_r-1`;
    const internalExt = `internal_src:dim|wss_push_room_id:${this._roomId}|wss_push_did:${user_unique_id}|first_req_ms:${Date.now()}000|fetch_time:${Date.now()}000|seq:1|wss_info:0-${Date.now()}000-0-0|wrds_v:${Date.now()}000`;

    let wssUrl = "wss://webcast100-ws-web-lq.douyin.com/webcast/im/push/v2/"
      + "?app_name=douyin_web&version_code=180800&webcast_sdk_version=1.0.14-beta.0"
      + "&update_version_code=1.0.14-beta.0&compress=gzip&device_platform=web"
      + "&cookie_enabled=true&screen_width=1536&screen_height=864"
      + "&browser_language=zh-CN&browser_platform=Win32&browser_name=Mozilla"
      + "&browser_version=5.0%20(Windows%20NT%2010.0;%20Win64;%20x64)%20AppleWebKit/537.36"
      + "&browser_online=true&tz_name=Asia/Shanghai&cursor=" + encodeURIComponent(cursor)
      + "&internal_ext=" + encodeURIComponent(internalExt)
      + "&host=https://live.douyin.com&aid=6383&live_id=1&did_rule=" + did_rule
      + "&endpoint=live_pc&support_wrds=1&user_unique_id=" + user_unique_id
      + "&im_path=/webcast/im/fetch/&identity=audience&need_persist_msg_count=15"
      + "&insert_task_id=&live_reason=&room_id=" + this._roomId + "&heartbeatDuration=0";

    // 生成签名
    let signature;
    try {
      signature = this._generateSignature(wssUrl);
    } catch (e) {
      console.error("[dy-danmaku] 签名生成失败:", e.message);
      // 用空签名试一下（有些房间不需要）
      signature = "00000000";
    }
    wssUrl += "&signature=" + signature;

    console.log("[dy-danmaku] 连接 WSS:", wssUrl.slice(0, 100) + "...");

    const headers = {
      "Cookie": `ttwid=${this._ttwid}`,
      "User-Agent": UA
    };

    const socket = new WebSocket(wssUrl, { headers });
    this.ws = socket;

    socket.on("open", () => {
      if (this.ws !== socket || this.closed) return;
      console.log("[dy-danmaku] WebSocket 连接成功");
      this.status = "connected";
      this.connectedAt = Date.now();
      this.lastMessageAt = Date.now();
      this.lastError = "";
      this.reconnectCount = 0;
      this.onEvent({ type: "connected", roomId: this._roomId, roomUrl: this.roomUrl });
      // 心跳
      this._hbTimer = setInterval(() => {
        if (!this.ws || this.ws.readyState !== WebSocket.OPEN) return;
        try {
          const PushFrame = this._proto.lookupType("douyin.PushFrame");
          const hb = PushFrame.encode({ payloadType: "hb" }).finish();
          this.ws.ping(hb);
        } catch (e) {}
      }, 5000);
      if (this._inactivityTimer) clearInterval(this._inactivityTimer);
      this._inactivityTimer = setInterval(() => {
        if (this.closed || !this.ws || this.ws.readyState !== WebSocket.OPEN) return;
        if (Date.now() - this.lastMessageAt > 45000) {
          this.lastError = "45秒未收到抖音上游消息";
          try { this.ws.terminate(); } catch (e) {}
        }
      }, 10000);
    });

    socket.on("message", (data) => {
      if (this.ws !== socket || this.closed) return;
      try {
        this.lastMessageAt = Date.now();
        if (global.__dyMsgCount === undefined) global.__dyMsgCount = 0;
        global.__dyMsgCount++;
        if (global.__dyMsgCount % 50 === 0 || global.__dyMsgCount <= 3) {
          console.log(`[dy-danmaku] 收到消息 #${global.__dyMsgCount}, 长度=${data.length || (data.byteLength)}`);
        }
        this._handleMessage(data);
      } catch (e) {
        console.log("[dy-danmaku] 消息解析失败:", e.message);
      }
    });

    socket.on("close", (code, reason) => {
      if (this.ws !== socket) return;
      console.log("[dy-danmaku] WebSocket 关闭:", code, reason?.toString());
      clearInterval(this._hbTimer);
      clearInterval(this._inactivityTimer);
      this._hbTimer = null;
      this._inactivityTimer = null;
      this.ws = null;
      if (!this.closed) this.status = "disconnected";
      this.onEvent({ type: "disconnected", code, reason: reason?.toString() });
      this._scheduleReconnect(false);
    });

    socket.on("error", (err) => {
      if (this.ws !== socket) return;
      this.lastError = err.message;
      console.error("[dy-danmaku] WebSocket 错误:", err.message);
    });
  }

  _handleMessage(data) {
    const PushFrame = this._proto.lookupType("douyin.PushFrame");
    const Response = this._proto.lookupType("douyin.Response");

    let frame;
    try {
      // 抖音 WebSocket 发的就是 raw protobuf 字节流（没 length-prefix）
      // protobufjs 的 decode 默认期望 length-delimited，需用 Reader 强制 raw 解析
      const reader = this._protobuf.Reader.create(new Uint8Array(data));
      frame = PushFrame.decode(reader);
    } catch (e) {
      console.log("[dy-danmaku] PushFrame 解码失败:", e.message);
      return; // 不是 protobuf 消息
    }

    // 发送 ack
    let payload;
    try {
      // 抖音 payload 总是 gzip 压缩的（payloadEncoding 字段指示压缩方式）
      const raw = zlib.gunzipSync(frame.payload);
      const reader = this._protobuf.Reader.create(raw);
      payload = Response.decode(reader);
    } catch (e) {
      console.log("[dy-danmaku] Response 解码失败:", e.message);
      return; // 解析失败，跳过
    }
    console.log(`[dy-danmaku] 解码成功: messagesList.length=${payload.messagesList?.length || 0}, needAck=${payload.needAck}`);

    if (payload.needAck) {
      try {
        const ackFrame = PushFrame.encode({
          logId: frame.logId,
          payloadType: "ack",
          payload: Buffer.from(payload.internalExt || "")
        }).finish();
        if (this.ws && this.ws.readyState === WebSocket.OPEN) {
          this.ws.send(ackFrame);
        }
      } catch (e) {}
    }

    // 解析消息
    if (!payload.messagesList) return;
    if (global.__dyEventCount === undefined) global.__dyEventCount = 0;
    for (const msg of payload.messagesList) {
      const event = this._parseMessage(msg.method, msg.payload);
      // 调试：每 5 条记录 method 分布
      if (global.__dyMethodLog === undefined) global.__dyMethodLog = 0;
      global.__dyMethodLog++;
      if (global.__dyMethodLog % 5 === 0) {
        console.log(`[dy-danmaku] 最近 5 条 method: ${msg.method} → event=${event ? event.type : "null"}`);
      }
      if (event) {
        this.lastEventAt = Date.now();
        global.__dyEventCount++;
        if (global.__dyEventCount <= 3 || global.__dyEventCount % 10 === 0) {
          console.log(`[dy-danmaku] 事件 #${global.__dyEventCount}: ${event.type} ${event.userName || ""} ${event.content || event.giftName || ""}`.slice(0, 120));
        }
        this.onEvent(event);
      } else if (msg.method) {
        // 不支持的消息类型，统计
        if (!global.__dyUnhandled) global.__dyUnhandled = {};
        global.__dyUnhandled[msg.method] = (global.__dyUnhandled[msg.method] || 0) + 1;
      }
    }
    if (global.__dyEventCount > 0 && global.__dyEventCount % 30 === 0) {
      console.log("[dy-danmaku] 未处理类型:", JSON.stringify(global.__dyUnhandled));
    }
  }

  _parseMessage(method, payload) {
    const handlers = {
      "WebcastChatMessage": () => {
        try {
          const ChatMessage = this._proto.lookupType("douyin.ChatMessage");
          const m = ChatMessage.decode(payload);
          return {
            type: "chat",
            userId: String(m.user?.id || ""),
            userName: m.user?.nickName || "",
            content: m.content || "",
            gender: m.user?.gender === 1 ? "女" : "男"
          };
        } catch (e) { console.error("[dy-danmaku] ChatMessage decode failed:", e.message); return null; }
      },
      "WebcastGiftMessage": () => {
        try {
          const GiftMessage = this._proto.lookupType("douyin.GiftMessage");
          const m = GiftMessage.decode(payload);
          return {
            type: "gift",
            userName: m.user?.nickName || "",
            giftName: m.gift?.name || "",
            comboCount: m.comboCount || 1,
            diamondCount: m.gift?.diamondCount || 0
          };
        } catch (e) { return null; }
      },
      "WebcastLikeMessage": () => {
        try {
          const LikeMessage = this._proto.lookupType("douyin.LikeMessage");
          const m = LikeMessage.decode(payload);
          return { type: "like", userName: m.user?.nickName || "", count: Number(m.count || 0) };
        } catch (e) { return null; }
      },
      "WebcastMemberMessage": () => {
        try {
          const MemberMessage = this._proto.lookupType("douyin.MemberMessage");
          const m = MemberMessage.decode(payload);
          return { type: "member", userId: String(m.user?.id || ""), userName: m.user?.nickName || "", gender: m.user?.gender === 1 ? "女" : "男" };
        } catch (e) { return null; }
      },
      "WebcastSocialMessage": () => {
        try {
          const SocialMessage = this._proto.lookupType("douyin.SocialMessage");
          const m = SocialMessage.decode(payload);
          return { type: "social", userId: String(m.user?.id || ""), userName: m.user?.nickName || "" };
        } catch (e) { return null; }
      },
      "WebcastRoomUserSeqMessage": () => {
        try {
          const RoomUserSeqMessage = this._proto.lookupType("douyin.RoomUserSeqMessage");
          const m = RoomUserSeqMessage.decode(payload);
          return { type: "stats", online: Number(m.total || 0), totalVisit: Number(m.totalPvForAnchor || 0) };
        } catch (e) { return null; }
      },
      "WebcastFansclubMessage": () => {
        try {
          const FansclubMessage = this._proto.lookupType("douyin.FansclubMessage");
          const m = FansclubMessage.decode(payload);
          return { type: "fansclub", content: m.content || "" };
        } catch (e) { return null; }
      },
      "WebcastControlMessage": () => {
        // status=3 表示下播
        return { type: "control" };
      }
    };

    const handler = handlers[method];
    if (handler) return handler();
    return null;
  }
}

module.exports = { DouyinDanmakuClient };
