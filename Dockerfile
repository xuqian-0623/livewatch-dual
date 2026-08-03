FROM node:22-bookworm-slim

RUN apt-get update \
  && apt-get install -y --no-install-recommends ffmpeg ca-certificates \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY package*.json ./
RUN npm ci --omit=dev

COPY live-proxy.js douyin-danmaku.js douyin-sign.js xunfei-asr.js douyin.proto ./
COPY live-monitor-v3.html mpegts.min.js hls.min.js flv.min.js ./

RUN mkdir -p /app/streams && chown -R node:node /app

USER node
ENV NODE_ENV=production
ENV PUBLIC_MODE=true
ENV PORT=8787
EXPOSE 8787

CMD ["node", "live-proxy.js"]
