# LiveWatch Dual

双路抖音直播实时监控看板。页面同时展示两个固定公司直播间的视频、真实评论、真实进房事件、官方在线人数、点赞、讯飞语音转写和本地风险关键词告警。

## 固定监控房间

- https://live.douyin.com/214698741282
- https://live.douyin.com/978001089489

生产环境启用 `PUBLIC_MODE=true` 后，服务端只接受以上两个直播间，避免公开接口被滥用为任意视频转码服务。

## 运行能力

- Node.js HTTP 与 WebSocket 服务
- 抖音真实 Webcast 弹幕中继
- FFmpeg 将直播流转换为同源 HLS
- 讯飞实时语音转写 WebSocket 中继
- 多个网页访客共享两路 FFmpeg 与弹幕上游

## 本地运行

要求：Node.js 20+、FFmpeg。

```bash
npm install
npm start
```

访问 `http://localhost:8787/`。

本地模式默认继续支持浏览器旧配置 `localStorage.lwp_cfg`。如果设置了服务器讯飞环境变量，服务端配置优先。

## Docker

```bash
docker build -t livewatch-dual .
docker run --rm -p 8787:8787 \
  -e XUNFEI_APP_ID=your_app_id \
  -e XUNFEI_API_KEY=your_api_key \
  -e XUNFEI_API_SECRET=your_api_secret \
  livewatch-dual
```

## Render 部署

[![Deploy to Render](https://render.com/images/deploy-to-render-button.svg)](https://render.com/deploy?repo=https://github.com/xuqian-0623/livewatch-dual)

仓库内包含 `render.yaml` 和 `Dockerfile`。点击上方按钮创建 Blueprint，或在 Render 创建 Docker Web Service，并在控制台填写：

- `XUNFEI_APP_ID`
- `XUNFEI_API_KEY`
- `XUNFEI_API_SECRET`

不要把真实凭证写进 Git 仓库。`render.yaml` 默认使用 Free 规格，部署时不要求绑定付费实例；但免费实例会休眠，而且双路实时转码可能因 CPU/内存不足而卡顿或被重启。需要稳定运行时再手动升级规格。

## 安全说明

- `.gitignore` 已排除本地 FFmpeg 工具、HLS 分片、测试密钥文件、环境变量和项目记忆。
- 公网模式限制固定房间及固定 `dual-room-1/2` 槽位。
- 公网访客停止页面时不会关闭共享转码。
- 讯飞凭证仅从服务器环境变量读取时，不会发送到浏览器。

## 健康检查

访问 `/api/health` 可查看 FFmpeg、公网模式和讯飞配置状态，但不会返回任何密钥内容。
