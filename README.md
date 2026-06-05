# mimi3 (mimo2api)

小米 AI Studio 自动化控制网关，将 MIMO 模型进行转发并兼容。

## 功能

- OpenAI 兼容 API 中转（支持 `/v1/chat/completions`, `/v1/responses`, `/anthropic/v1/messages`）
- Web 控制面板（实时监控、日志查看）
- 多账号轮询负载均衡
- 流式响应支持

## 快速开始

```bash
# 安装依赖
pip install -r requirements.txt

# 复制并配置环境变量
cp env.example .env

# 启动服务
python main.py
```

## Docker 启动

```bash
cp env.example .env
docker compose up -d --build
```

默认服务端口为 `8000`，可在 `.env` 中通过 `SERVER_PORT` 调整。

Docker Compose 会挂载以下本地目录：

- `./users` -> `/app/users`
- `./logs` -> `/app/logs`
- `./data` -> `/app/data`

容器内默认将指标数据库、指标快照、进程锁和模型映射文件放在 `/app/data`，对应宿主机 `./data` 目录：

```bash
MIMO_METRICS_DB_PATH=/app/data/gateway_metrics.db
MIMO_METRICS_SNAPSHOT_PATH=/app/data/gateway_snapshot.json
MIMO_PROCESS_LOCK_PATH=/app/data/mimo2api.lock
```

## 前置条件
一台拥有公网 ip 的机器，或者本机进行内网穿透。此为必备配置选项
```bash
WS_TUNNEL_URL=ws://your-domain.com:8000/ws
```

公网部署建议为节点接入开启 WebSocket 鉴权：

```bash
MIMO_WS_TUNNEL_KEY=ws-your-random-secret-here
```

设置后，`/ws` 只接受携带相同密钥的桥接节点连接；不设置时保持兼容模式。生产环境建议通过 `Authorization: Bearer` 或 `x-ws-token` 请求头传递密钥，避免把 token 放入 URL 查询参数。

## 免责声明

1. **本项目仅供学习交流使用，禁止一切商业/滥用行为。**
2. 本项目为个人独立开发的开源项目，与小米公司及其关联方**无任何隶属、授权或合作关系**。
3. MIMO、Xiaomi AI Studio 等名称及商标归小米公司所有，本项目不主张任何权利。
4. 本项目不提供任何小米账号、密钥或付费服务的破解，仅作为技术研究用途。
5. 使用者应遵守所在地法律法规及小米服务条款，因使用本项目产生的一切后果由使用者自行承担。
6. 本项目代码随缘更新，作者不提供任何保证或技术支持。
7. **建议优先使用小米官方 API**，本项目仅为技术研究备选方案。
8. 如有任何权益问题，请联系删除。

## 致谢
[linux.do](https://linux.do)
