// 讯飞实时语音转写大模型 (RTASR LLM) — 服务端中继
// 文档: https://www.xfyun.cn/doc/spark/asr_llm/rtasr_llm.html
//
// 关键差异（对比旧 API）:
//   1. URL: wss://office-api-ast-dx.iflyaisol.com/ast/communicate/v1
//   2. 鉴权: accessKeyId + HmacSHA1 签名 → URL 参数
//   3. 音频: 纯 PCM 二进制流，无帧头！每 40ms 发 1280 字节
//   4. 结束: JSON {"end":true, "sessionId":"xxx"}
//   5. 响应: msg_type:result, data.cn.st.rt.ws[].cw[].w

const WebSocket = require("ws");
const crypto = require("crypto");

const HOST = "office-api-ast-dx.iflyaisol.com";
const PATH = "/ast/communicate/v1";

class XunfeiRTASR {
  /**
   * @param {string} appId      - 讯飞控制台 AppID
   * @param {string} apiKey     - 讯飞控制台 APIKey（文档中叫 accessKeyId）
   * @param {string} apiSecret  - 讯飞控制台 APISecret（文档中叫 accessKeySecret）
   */
  constructor(appId, apiKey, apiSecret) {
    this.appId = appId;
    this.accessKeyId = apiKey;
    this.accessKeySecret = apiSecret;
    this.ws = null;
    this._connected = false;
    this._closed = false;
    this._sessionId = crypto.randomUUID();
    this._partial = "";  // 中间结果累积
    this._lastFinal = "";
    this.onReady = null;    // () => {}，讯飞上游握手完成
    this.onResult = null;   // (text, isFinal) => {}
    this.onError = null;    // (err) => {}
    this.onClose = null;    // (code, reason, hint) => {}
  }

  connect() {
    // 1. 生成固定 UTC+8 时间。云服务器通常运行在 UTC，不能用本机 getHours() 后直接标记 +0800，
    // 否则签名时间会相差 8 小时并被讯飞立即断开（35014）。
    const chinaTime = new Date(Date.now() + 8 * 60 * 60 * 1000);
    const pad = (n) => String(n).padStart(2, "0");
    const utc =
      `${chinaTime.getUTCFullYear()}-${pad(chinaTime.getUTCMonth() + 1)}-${pad(chinaTime.getUTCDate())}` +
      `T${pad(chinaTime.getUTCHours())}:${pad(chinaTime.getUTCMinutes())}:${pad(chinaTime.getUTCSeconds())}+0800`;

    // 2. 构造请求参数（不含 signature）
    const params = {
      accessKeyId: this.accessKeyId,
      appId: this.appId,
      uuid: this._sessionId,
      utc: utc,
      audio_encode: "pcm_s16le",
      lang: "autodialect",
      samplerate: "16000"
    };

    // 3. 按 key 字母序排列 → URL 编码 → 拼接 → HMAC-SHA1 → Base64
    const sortedKeys = Object.keys(params).sort();
    const baseString = sortedKeys
      .map(k => encodeURIComponent(k) + "=" + encodeURIComponent(params[k]))
      .join("&");

    const signature = crypto.createHmac("sha1", this.accessKeySecret)
      .update(baseString).digest("base64");

    // 4. 拼接完整 URL
    const queryStr = baseString + "&signature=" + encodeURIComponent(signature);
    const url = `wss://${HOST}${PATH}?${queryStr}`;

    console.log("[xunfei] 签名:", baseString.slice(0, 80) + "...");
    console.log("[xunfei] 连接:", url.slice(0, 100) + "...");
    this.ws = new WebSocket(url);

    this.ws.on("open", () => {
      console.log("[xunfei] 握手成功，开始传输音频");
      this._connected = true;
      if (this.onReady) this.onReady();
    });

    this.ws.on("message", (data) => {
      try {
        const msg = JSON.parse(data.toString());
        if (msg.msg_type === "result" && msg.data) {
          const d = msg.data;
          // 拼接所有识别词
          const words = [];
          const st = d.cn?.st;
          if (st?.rt) {
            for (const rt of st.rt) {
              if (rt.ws) {
                for (const ws of rt.ws) {
                  if (ws.cw) {
                    for (const cw of ws.cw) {
                      if (cw.w) words.push(cw.w);
                    }
                  }
                }
              }
            }
          }
          const text = words.join("");
          const isFinal = d.ls === true;
          const segmentId = d.seg_id ?? null;
          const resultType = Number(st?.type ?? 1);
          console.log(`[xunfei] ${isFinal?"✅":"🔹"} ${text ? `「${text.slice(0,40)}」` : "(空)"} ls=${d.ls} type=${st?.type} seg=${segmentId}`);

          if (text || d.ls) {
            if (isFinal) {
              this._lastFinal = text;
              this._partial = "";
              if (this.onResult) this.onResult(text, true, { segmentId, resultType });
            } else if (text !== this._partial) {
              this._partial = text;
              if (this.onResult) this.onResult(text, false, { segmentId, resultType });
            }
          }
        } else if (msg.res_type === "frc") {
          console.error(`❌ [xunfei] 识别失败: code=${msg.code || "?"} desc=${msg.data?.desc || ""}`, JSON.stringify(msg.data?.detail || "").slice(0, 100));
          if (this.onError) this.onError(new Error(msg.data?.desc || "识别失败: " + (msg.code || "未知")));
        } else if (msg.msg_type === "action") {
          console.log("[xunfei] 动作:", msg.data?.action || "?", "sessionId:", msg.data?.sessionId?.slice(0, 8));
        } else {
          console.log("[xunfei] 其他消息:", JSON.stringify(msg).slice(0, 200));
        }
      } catch (e) {
        console.log("[xunfei] 原始消息:", data.toString().slice(0, 200));
      }
    });

    this.ws.on("close", (code, reason) => {
      const reasonStr = reason?.toString() || "";
      // 讯飞自定义 HTTP 状态码说明
      const codeMap = { 35001: "鉴权失败", 35002: "用量不足", 35004: "appId 不存在", 35005: "appId 已禁用",
        35006: "并发满", 35014: "时间戳偏差过大", 35022: "用量超限", 35030: "签名重复/过期" };
      const hint = codeMap[code] || reasonStr || "上游连接已关闭";
      console.warn(`⚠️  讯飞 WS 关闭: 代码=${code} ${hint} ${reasonStr}`);
      this._connected = false;
      if (this.onClose) this.onClose(code, reasonStr, hint);
    });

    this.ws.on("error", (e) => {
      // 'ws' 库 'error' 事件无 status code，只能给方向性提示
      const msg = String(e.message || e);
      let hint = "检查网络";
      if (/Invalid response status/i.test(msg)) hint = "鉴权失败：检查 AppID/APIKey/APISecret 是否正确，时间是否准确";
      else if (/401/i.test(msg)) hint = "鉴权失败 (401)";
      else if (/timeout/i.test(msg)) hint = "连接超时";
      else if (/ENOTFOUND|ECONNREFUSED/i.test(msg)) hint = "网络不通";
      console.error("[xunfei] 连接错误:", msg, "→", hint);
      if (this.onError) this.onError(new Error(`${msg}（${hint}）`));
    });
  }

  /**
   * 发送 PCM 音频数据（纯二进制，无帧头！）
   * 建议每 40ms 发送 1280 字节（16kHz 16bit mono = 32000 bytes/s，32000*0.04=1280）
   */
  sendAudio(pcmBuffer) {
    if (!this._connected || this._closed) return;
    try {
      this.ws.send(Buffer.from(pcmBuffer));
    } catch (e) {
      console.error("[xunfei] 发送音频失败:", e.message);
    }
  }

  /** 发送结束标识 */
  end() {
    if (!this._connected || this._closed) return;
    this._closed = true;
    try {
      this.ws.send(JSON.stringify({ end: true, sessionId: this._sessionId }));
    } catch (e) {
      console.error("[xunfei] 发送结束失败:", e.message);
    }
  }

  close() {
    this._closed = true;
    this._connected = false;
    if (this.ws) { try { this.ws.close(); } catch (e) {} this.ws = null; }
  }
}

module.exports = { XunfeiRTASR };
