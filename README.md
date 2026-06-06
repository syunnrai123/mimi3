# mimi3 (mimo2api)

小米 AI Studio 自动化控制网关，将 MIMO 模型进行转发并兼容。

## 功能

- OpenAI 兼容 API 中转（支持 `/v1/chat/completions`, `/v1/responses`, `/anthropic/v1/messages`）
- 语音合成（TTS）：兼容 OpenAI `/v1/audio/speech`，适配 MiMo-V2.5-TTS 系列
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

## 语音合成（TTS）调用示例

网关已适配 MiMo-V2.5-TTS 系列模型，提供两种调用方式。

> **鉴权**：若在 `.env` 中设置了 `MIMO_RELAY_OPENAI_KEY`，则 `/v1/*`、`/anthropic/v1/*` 的所有请求都需携带该密钥（请求头 `Authorization: Bearer <key>`，或 `x-api-key` / `api-key`）；未设置时为兼容模式，可省略鉴权头。

### 方式一：`/v1/audio/speech`（OpenAI 兼容，推荐）

直接传入目标文本即可，网关会自动满足 MiMo「目标文本须置于 assistant 角色」的要求。

```bash
curl -X POST http://localhost:8000/v1/audio/speech \
  -H "Authorization: Bearer $MIMO_RELAY_OPENAI_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "mimo-v2.5-tts",
    "input": "你好，这是一段语音合成测试。",
    "voice": "mimo_default",
    "response_format": "wav",
    "instructions": "用温柔、平静的语气朗读"
  }' --output speech.wav
```

```python
import os
from openai import OpenAI

client = OpenAI(
    base_url="http://localhost:8000/v1",
    api_key=os.getenv("MIMO_RELAY_OPENAI_KEY", "sk-no-auth"),  # 未启用鉴权时填任意非空字符串
)

with client.audio.speech.with_streaming_response.create(
    model="mimo-v2.5-tts",
    input="你好，这是一段语音合成测试。",
    voice="mimo_default",
    response_format="wav",
    instructions="用温柔、平静的语气朗读",  # 可选：控制语气 / 情绪 / 语速
) as response:
    response.stream_to_file("speech.wav")
```

参数说明：

| 字段 | 说明 | 默认值 |
| --- | --- | --- |
| `input` | 要合成为语音的目标文本（必填） | — |
| `voice` | 音色；OpenAI 音色名（alloy 等）会映射为 `mimo_default`，也可直接填 MiMo 精品音色名 | `mimo_default` |
| `response_format` | 音频格式，支持 `wav` / `mp3` / `flac` / `aac` / `opus` / `pcm` | `wav` |
| `instructions` | 可选，自然语言风格指令（语气 / 语速 / 情绪），内容不会被读出来 | — |

### 方式二：`/v1/chat/completions`（原生，功能完整）

MiMo TTS 复用 chat 接口，但规则与普通对话**相反**：**要合成的文本必须放在 `assistant` 角色**；`user` 角色用于传可选的风格指令（不会被读出来）。

```bash
curl -X POST http://localhost:8000/v1/chat/completions \
  -H "Authorization: Bearer $MIMO_RELAY_OPENAI_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "mimo-v2.5-tts",
    "messages": [
      { "role": "user",      "content": "用兴奋、轻快的语气" },
      { "role": "assistant", "content": "（开心）老板，我考过了，还是优秀！" }
    ],
    "audio": { "format": "wav", "voice": "mimo_default" }
  }'
```

- 返回为标准 Chat Completion 结构，音频数据（base64 编码）位于 `choices[0].message.audio.data`。
- `assistant` 文本中可用 `（风格）` 与 `[音频标签]` 做精细控制，例如 `（唱歌）`、`（四川话）`、`[大笑]` 等。
- 同系列的音色设计、音色复刻模型也走该端点，但参数不同，详见下文。

### 音色设计：`mimo-v2.5-tts-voicedesign`

无需音频样本，在 `user` 角色里用自然语言**描述想要的音色**（必填），`assistant` 角色放目标文本。该模型**不支持 `voice` 字段**。

```bash
curl -X POST http://localhost:8000/v1/chat/completions \
  -H "Authorization: Bearer $MIMO_RELAY_OPENAI_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "mimo-v2.5-tts-voicedesign",
    "messages": [
      { "role": "user",      "content": "一个温柔的年轻女性音色，语速偏慢" },
      { "role": "assistant", "content": "欢迎回来，今天也辛苦啦。" }
    ],
    "audio": { "format": "wav", "optimize_text_preview": true }
  }'
```

> `optimize_text_preview` 设为 `true` 时会对播报文本智能润色，此时可省略 `assistant` 消息。

### 音色复刻：`mimo-v2.5-tts-voiceclone`

传入一段参考音频即可复刻其音色。参考音频放在 **`audio.voice`** 字段，格式为 `data:{MIME_TYPE};base64,{BASE64}`：

- 仅支持 `mp3`、`wav`，MIME 取值为 `audio/mpeg`（或 `audio/mp3`）、`audio/wav`
- Base64 字符串大小 ≤ 10 MB（建议用清晰、单人、少背景噪声的 10~30 秒人声片段）
- `user` 角色可选（附加风格指令），`assistant` 角色放目标文本

```python
import base64
import os
from openai import OpenAI

client = OpenAI(
    base_url="http://localhost:8000/v1",
    api_key=os.getenv("MIMO_RELAY_OPENAI_KEY", "sk-no-auth"),
)

with open("voice_sample.mp3", "rb") as f:
    voice_b64 = base64.b64encode(f.read()).decode()

completion = client.chat.completions.create(
    model="mimo-v2.5-tts-voiceclone",
    messages=[
        {"role": "user", "content": ""},  # 可选：风格指令
        {"role": "assistant", "content": "这是用复刻音色合成的一句话。"},
    ],
    audio={"format": "wav", "voice": f"data:audio/mpeg;base64,{voice_b64}"},
)

audio_bytes = base64.b64decode(completion.choices[0].message.audio.data)
with open("cloned.wav", "wb") as f:
    f.write(audio_bytes)
```

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
