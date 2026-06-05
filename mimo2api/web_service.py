import asyncio
import base64
import binascii
import json
import logging
import time
import uuid
from dataclasses import dataclass
from typing import Any, TextIO
from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, Response, status
from fastapi.responses import StreamingResponse, JSONResponse
import uvicorn
import os
from pathlib import Path

try:
    import fcntl
except ImportError:
    fcntl = None

try:
    import msvcrt
except ImportError:
    msvcrt = None

MODEL_MAPPING_FILE = Path(__file__).parent.parent / "model_mapping.json"

# 引入 Manager 长驻协程任务
from .manager import start_manager_tasks, trigger_rebuild

# Responses API 转换器
from .responses_converter import convert_request as responses_convert_request
from .responses_converter import convert_response as responses_convert_response
from .responses_converter import ResponsesStreamConverter
from .audio_helpers import (
    AudioSpeechRequest,
    audio_media_type,
    extract_audio_payload,
    map_openai_tts_model,
    map_openai_tts_voice,
)
from .auth import (
    get_webui_username,
    is_ai_auth_enabled,
    is_ws_tunnel_auth_enabled,
    is_web_auth_enabled,
    require_ai_request,
    require_webui_request,
    verify_ws_tunnel_request,
)
from .metrics_store import (
    METRICS_BUCKET_SECONDS,
    METRICS_RETENTION_DAYS,
    build_gateway_stats,
    extract_usage_from_sse_chunk,
    init_metrics_db,
    load_status_history,
    metrics_history_worker,
    node_label,
    reclassify_history,
    record_attempt_finished,
    record_attempt_started,
    record_request_finished,
    record_request_started,
)

# 配置基础日志
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

manager_bg_task = None
metrics_persist_task = None
sweeper_bg_task = None
single_process_lock_file = None
STALE_QUEUE_TTL = 300
SHUTDOWN_TASK_TIMEOUT = float(os.getenv("MIMO_SHUTDOWN_TASK_TIMEOUT", "5"))

def sweep_stale_queues_once(now: float | None = None) -> int:
    now = time.time() if now is None else now
    stale_count = 0
    for req_id, last_activity_at in list(state.req_id_timestamps.items()):
        if now - last_activity_at > STALE_QUEUE_TTL:
            logger.error(f"💀 发现长时间无活动的悬挂队列，强制回收: [{req_id[:8]}]")
            cleanup_pending_request(req_id)
            stale_count += 1
    if stale_count > 0:
        logger.info(f"🧹 垃圾回收周期结束，共清理了 {stale_count} 个泄露队列。当前活跃队列数: {len(state.pending_queues)}")
    return stale_count

async def sweep_stale_queues():
    """后台巡检任务，清理长时间无活动的悬挂请求队列。"""
    while True:
        try:
            await asyncio.sleep(60)
            sweep_stale_queues_once()
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"清理死锁队列任务发生异常: {e}")


async def close_active_clients() -> None:
    clients = list(state.active_clients)
    if not clients:
        return

    logger.info(f"🛑 正在关闭 {len(clients)} 个内网节点连接...")
    for client in clients:
        try:
            await client.close()
        except Exception as exc:
            logger.debug(f"关闭内网节点连接失败: {exc}")


async def cancel_and_wait_tasks(tasks: list[asyncio.Task | None], *, label: str) -> None:
    pending = [task for task in tasks if task is not None and not task.done()]
    if not pending:
        return

    for task in pending:
        task.cancel()

    try:
        await asyncio.wait_for(asyncio.gather(*pending, return_exceptions=True), timeout=SHUTDOWN_TASK_TIMEOUT)
    except asyncio.TimeoutError:
        still_running = [task for task in pending if not task.done()]
        logger.warning(
            f"⚠️ 关闭 {label} 超时，{len(still_running)} 个任务在 {SHUTDOWN_TASK_TIMEOUT}s 内未退出"
        )

@asynccontextmanager
async def lifespan(app: FastAPI):
    global manager_bg_task, metrics_persist_task, sweeper_bg_task
    logger.info("🚀 正在拉起挂后台的 Claw 账号守护线程...")
    acquire_single_process_lock()

    await asyncio.to_thread(init_metrics_db)
    fixed = await asyncio.to_thread(reclassify_history)
    if fixed:
        logger.info(f"🔧 重新分类了 {fixed} 条历史状态记录")
        
    manager_bg_task = asyncio.create_task(start_manager_tasks(), name="mimo-manager")
    metrics_persist_task = asyncio.create_task(metrics_history_worker(), name="mimo-metrics")
    sweeper_bg_task = asyncio.create_task(sweep_stale_queues(), name="mimo-sweeper") # 启动巡检死神
    
    yield

    try:
        await close_active_clients()
        await cancel_and_wait_tasks(
            [manager_bg_task, metrics_persist_task, sweeper_bg_task],
            label="核心后台任务",
        )
        await cancel_and_wait_tasks(list(_background_tasks), label="转发清理任务")
    finally:
        manager_bg_task = None
        metrics_persist_task = None
        sweeper_bg_task = None
        release_single_process_lock()

app = FastAPI(lifespan=lifespan)

# 全局状态从 gateway_state 引入
from .gateway_state import state

# 注入前面拆分出的 WebUI 独立路由
from .ui_router import router as ui_router
app.include_router(ui_router)

RETRYABLE_STATUS_CODES = {401, 403, 429}
NODE_RESPONSE_TIMEOUT = 30
MAX_RETRIES = 3
MAX_PENDING_QUEUES = 2000
AI_ROUTE_PREFIXES = ("/v1/", "/anthropic/v1/")
WEBUI_PUBLIC_PATHS = {"/", "/api/auth/session", "/api/auth/login", "/api/auth/logout", "/api/stats", "/api/status/history", "/webui"}
NODE_REPLACED_CLOSE_CODE = 4001
DUPLICATE_NODE_CLOSE_CODE = 4002

if is_ai_auth_enabled():
    logger.info("🔐 AI API 鉴权已启用")
if is_ws_tunnel_auth_enabled():
    logger.info("🔐 WebSocket 节点接入鉴权已启用")
if is_web_auth_enabled():
    logger.info(f"🔐 WebUI 鉴权已启用，登录用户: {get_webui_username()}")


def is_ai_route(path: str) -> bool:
    return path.startswith(AI_ROUTE_PREFIXES)


def is_webui_route(path: str) -> bool:
    return path.startswith("/api/") and path not in WEBUI_PUBLIC_PATHS


@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    path = request.url.path

    if is_ai_route(path):
        auth_error = require_ai_request(request)
        if auth_error is not None:
            return auth_error

    if is_webui_route(path):
        auth_error = require_webui_request(request)
        if auth_error is not None:
            return auth_error

    return await call_next(request)


def diagnose_request(body_text: str) -> str:
    """从请求体中提取关键诊断信息，用于 400 错误追踪"""
    try:
        req = json.loads(body_text)
    except Exception:
        return "body=非法JSON"
    msgs = req.get("messages", [])
    model = req.get("model", "未指定")
    stream = req.get("stream", False)
    total_chars = sum(len(str(m.get("content", ""))) for m in msgs)
    est_tokens = total_chars // 3
    tools = req.get("tools", [])
    return (
        f"model={model}, stream={stream}, msgs={len(msgs)}, "
        f"est_tokens≈{est_tokens}, chars={total_chars}, tools={len(tools)}"
    )


def record_error(route: str, status_code: int, reason: str, model: str = "", detail: str = "", request_body: str = ""):
    """记录错误到环形缓冲区，可通过 /api/errors 查询"""
    state.recent_errors.append({
        "ts": int(time.time()),
        "route": route,
        "status": status_code,
        "reason": reason[:200],
        "model": model,
        "detail": detail[:500],
        "request": request_body[:2000] if request_body else "",
    })

STREAM_CHUNK_TIMEOUT = 60
STREAM_KEEPALIVE_INTERVAL = 25  # 秒，需小于 Cloudflare 超时 (~100s)
QUEUE_DRAIN_TIMEOUT = 5
DEFAULT_GATEWAY_ERROR = "Gateway Error: 所有节点请求失败"
NODE_401_COOLDOWN_SECONDS = int(os.getenv("MIMO_NODE_401_COOLDOWN_SECONDS", "900"))
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROCESS_LOCK_PATH = os.getenv("MIMO_PROCESS_LOCK_PATH", os.path.join(ROOT_DIR, "mimo2api.lock"))
try:
    UNAUTHORIZED_WS_LOG_INTERVAL = max(1, int(os.getenv("MIMO_UNAUTHORIZED_WS_LOG_INTERVAL_SECONDS", "10")))
except ValueError:
    UNAUTHORIZED_WS_LOG_INTERVAL = 10
try:
    UNAUTHORIZED_WS_LOG_STATE_MAX_SIZE = max(
        100,
        int(os.getenv("MIMO_UNAUTHORIZED_WS_LOG_STATE_MAX_SIZE", "4096")),
    )
except ValueError:
    UNAUTHORIZED_WS_LOG_STATE_MAX_SIZE = 4096

# 后台 fire-and-forget 任务集合
_background_tasks: set[asyncio.Task] = set()
PROCESS_LOCK_SIZE = 1
_unauthorized_ws_log_state: dict[str, tuple[float, int]] = {}
_unauthorized_ws_next_cleanup_at = 0.0


def prune_unauthorized_ws_log_state(now: float) -> None:
    """定期清理未授权 WS 日志限流状态，并对高基数来源做容量兜底。"""
    global _unauthorized_ws_next_cleanup_at
    if now < _unauthorized_ws_next_cleanup_at and len(_unauthorized_ws_log_state) <= UNAUTHORIZED_WS_LOG_STATE_MAX_SIZE:
        return

    cutoff = now - UNAUTHORIZED_WS_LOG_INTERVAL * 3
    for host, (last_logged_at, _) in list(_unauthorized_ws_log_state.items()):
        if last_logged_at < cutoff:
            _unauthorized_ws_log_state.pop(host, None)

    overflow = len(_unauthorized_ws_log_state) - UNAUTHORIZED_WS_LOG_STATE_MAX_SIZE
    if overflow > 0:
        stale_hosts = sorted(
            _unauthorized_ws_log_state,
            key=lambda host: _unauthorized_ws_log_state[host][0],
        )[:overflow]
        for host in stale_hosts:
            _unauthorized_ws_log_state.pop(host, None)

    _unauthorized_ws_next_cleanup_at = now + UNAUTHORIZED_WS_LOG_INTERVAL


def should_log_unauthorized_ws_rejection(client_host: str, now: float | None = None) -> tuple[bool, int]:
    """按来源 IP 限流未授权 WS 日志，返回是否记录以及被抑制的次数。"""
    now = time.time() if now is None else now
    client_key = client_host or "Unknown"
    prune_unauthorized_ws_log_state(now)

    log_record = _unauthorized_ws_log_state.get(client_key)
    if log_record is None:
        _unauthorized_ws_log_state[client_key] = (now, 0)
        if len(_unauthorized_ws_log_state) > UNAUTHORIZED_WS_LOG_STATE_MAX_SIZE:
            prune_unauthorized_ws_log_state(now)
        return True, 0

    last_logged_at, suppressed_count = log_record
    if now - last_logged_at >= UNAUTHORIZED_WS_LOG_INTERVAL:
        _unauthorized_ws_log_state[client_key] = (now, 0)
        if len(_unauthorized_ws_log_state) > UNAUTHORIZED_WS_LOG_STATE_MAX_SIZE:
            prune_unauthorized_ws_log_state(now)
        return True, suppressed_count

    _unauthorized_ws_log_state[client_key] = (last_logged_at, suppressed_count + 1)
    return False, suppressed_count + 1


def _track_task(task: asyncio.Task) -> None:
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)

def _lock_file_nonblocking(lock_file: TextIO) -> None:
    if os.name == "nt":
        if msvcrt is None:
            raise OSError("当前平台缺少 msvcrt，无法加锁。")
        lock_file.seek(0)
        msvcrt.locking(lock_file.fileno(), msvcrt.LK_NBLCK, PROCESS_LOCK_SIZE)
        return

    if fcntl is None:
        raise OSError("当前平台缺少 fcntl，无法加锁。")
    fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)

def _unlock_file(lock_file: TextIO) -> None:
    if os.name == "nt":
        if msvcrt is None:
            raise OSError("当前平台缺少 msvcrt，无法解锁。")
        lock_file.seek(0)
        msvcrt.locking(lock_file.fileno(), msvcrt.LK_UNLCK, PROCESS_LOCK_SIZE)
        return

    if fcntl is None:
        raise OSError("当前平台缺少 fcntl，无法解锁。")
    fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)

def acquire_single_process_lock() -> None:
    global single_process_lock_file
    if single_process_lock_file is not None:
        return

    try:
        lock_path = Path(PROCESS_LOCK_PATH)
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        lock_path.touch(exist_ok=True)
        lock_file = lock_path.open("r+", encoding="utf-8")
        if lock_path.stat().st_size < PROCESS_LOCK_SIZE:
            lock_file.write("\n")
            lock_file.flush()
        _lock_file_nonblocking(lock_file)
    except (BlockingIOError, OSError) as exc:
        if 'lock_file' in locals():
            lock_file.close()
        raise RuntimeError("当前进程锁被占用。") from exc

    lock_file.seek(0)
    lock_file.truncate()
    lock_file.write(str(os.getpid()))
    lock_file.flush()
    single_process_lock_file = lock_file

def release_single_process_lock() -> None:
    global single_process_lock_file
    if single_process_lock_file is None:
        return
    try:
        _unlock_file(single_process_lock_file)
    finally:
        single_process_lock_file.close()
        single_process_lock_file = None

@dataclass(slots=True)
class RetryState:
    status_code: int = 502
    response_text: str = DEFAULT_GATEWAY_ERROR

@dataclass(slots=True)
class ForwardAttempt:
    req_id: str
    queue: asyncio.Queue
    target_ws: WebSocket
    first_msg: dict[str, Any]
    attempt_number: int

@app.post("/api/rebuild")
async def api_rebuild():
    trigger_rebuild()
    return JSONResponse(content={"ok": True, "message": "重建信号已发送，所有节点将在当前循环结束后立即重建"})

@app.get("/api/stats")
async def api_stats():
    return JSONResponse(content=build_gateway_stats(len(_background_tasks)))

@app.get("/api/status/history")
async def api_status_history(hours: int = 24):
    hours = max(1, min(hours, 24 * METRICS_RETENTION_DAYS))
    return JSONResponse(content=await asyncio.to_thread(load_status_history, hours))

@app.get("/api/errors")
async def api_errors(request: Request, limit: int = 50):
    if not is_web_auth_enabled():
        return JSONResponse(
            {"detail": "查看错误记录需要先配置 MIMO_WEBUI_PASSWORD 以启用 WebUI 鉴权"},
            status_code=403,
        )
    auth_error = require_webui_request(request)
    if auth_error is not None:
        return auth_error

    limit = max(1, min(limit, 200))
    errors = list(state.recent_errors)[-limit:]
    errors.reverse()  # 最新的在前
    return JSONResponse(content={"count": len(errors), "errors": errors})

def load_model_mapping() -> dict[str, str]:
    if not MODEL_MAPPING_FILE.exists():
        return {}
    try:
        return json.loads(MODEL_MAPPING_FILE.read_text("utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}

def save_model_mapping(mapping: dict[str, str]) -> None:
    tmp = MODEL_MAPPING_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(mapping, ensure_ascii=False, indent=2), "utf-8")
    tmp.rename(MODEL_MAPPING_FILE)

def apply_model_mapping(body_text: str) -> str:
    mapping = load_model_mapping()
    if not mapping:
        return body_text
    try:
        data = json.loads(body_text)
    except (json.JSONDecodeError, AttributeError):
        return body_text
    original_model = data.get("model")
    if original_model and original_model in mapping:
        data["model"] = mapping[original_model]
        logger.info(f"🔀 模型映射: {original_model} → {data['model']}")
        return json.dumps(data, ensure_ascii=False)
    return body_text


def get_ws_node_id(ws: WebSocket) -> str | None:
    node_id = ws.headers.get("x-node-id", "").strip()
    if not node_id:
        node_id = ws.query_params.get("node_id", "").strip()
    return node_id or None


def get_ws_node_started_at(ws: WebSocket) -> float | None:
    raw_value = ws.headers.get("x-node-started-at", "").strip()
    if not raw_value:
        raw_value = ws.query_params.get("node_started_at", "").strip()
    try:
        value = float(raw_value)
    except (TypeError, ValueError):
        return None
    return value if value > 0 else None


def get_ws_node_instance_id(ws: WebSocket) -> str | None:
    instance_id = ws.headers.get("x-node-instance-id", "").strip()
    if not instance_id:
        instance_id = ws.query_params.get("node_instance_id", "").strip()
    return instance_id or None


def get_trusted_ws_node_id(ws: WebSocket) -> str | None:
    node_id = get_ws_node_id(ws)
    if node_id and not is_ws_tunnel_auth_enabled():
        logger.warning("⚠️ WS 鉴权未启用，已忽略客户端上报的 node_id，避免未授权节点替换")
        return None
    return node_id


def cleanup_client_state(ws: WebSocket, disconnect_body: str = "节点断开连接") -> int:
    ws_id = id(ws)
    state.active_clients[:] = [client for client in state.active_clients if client is not ws]
    state.client_cooldowns.pop(ws_id, None)

    node_id = state.ws_id_to_node_id.pop(ws_id, None)
    if node_id and state.node_id_to_ws_id.get(node_id) == ws_id:
        state.node_id_to_ws_id.pop(node_id, None)
    state.ws_id_to_node_started_at.pop(ws_id, None)
    state.ws_id_to_node_instance_id.pop(ws_id, None)

    orphan_ids = state.ws_to_req_ids.pop(ws_id, set())
    for orphan_id in orphan_ids:
        q = state.pending_queues.pop(orphan_id, None)
        state.req_id_to_ws_id.pop(orphan_id, None)
        state.req_id_timestamps.pop(orphan_id, None)
        if q is not None:
            try:
                q.put_nowait({"type": "error", "body": disconnect_body})
            except asyncio.QueueFull:
                pass

    if state.current_client_index >= len(state.active_clients):
        state.current_client_index = 0
    return len(orphan_ids)


async def replace_existing_node_connection(node_id: str, new_ws: WebSocket) -> bool:
    old_ws_id = state.node_id_to_ws_id.get(node_id)
    if old_ws_id is None:
        return True

    old_ws = next((client for client in state.active_clients if id(client) == old_ws_id), None)
    if old_ws is None or old_ws is new_ws:
        state.node_id_to_ws_id.pop(node_id, None)
        return True

    old_started_at = state.ws_id_to_node_started_at.get(old_ws_id)
    new_started_at = get_ws_node_started_at(new_ws)
    old_instance_id = state.ws_id_to_node_instance_id.get(old_ws_id, "")
    new_instance_id = get_ws_node_instance_id(new_ws) or ""
    if old_started_at is not None:
        should_reject_new = new_started_at is None or new_started_at <= old_started_at
    else:
        should_reject_new = new_started_at is None
    if should_reject_new:
        logger.debug(
            f"忽略节点 {node_id} 的旧实例重复连接: old_started_at={old_started_at}, "
            f"new_started_at={new_started_at}, old_instance={old_instance_id}, new_instance={new_instance_id}"
        )
        try:
            await new_ws.close(code=DUPLICATE_NODE_CLOSE_CODE)
        except Exception as exc:
            logger.debug(f"关闭重复节点连接失败: {exc}")
        return False

    logger.warning(f"♻️ 节点 {node_id} 建立新连接，正在清理旧连接 [{old_ws_id}]")
    orphan_count = cleanup_client_state(old_ws, "节点连接已被新实例替换")
    try:
        await old_ws.close(code=NODE_REPLACED_CLOSE_CODE)
    except Exception as exc:
        logger.debug(f"关闭旧节点连接失败: {exc}")
    if orphan_count:
        logger.warning(f"🧹 节点 {node_id} 替换旧连接时清理 {orphan_count} 个孤儿请求队列")
    return True

@app.get("/api/model_mapping")
async def api_get_model_mapping():
    return JSONResponse(content=load_model_mapping())

@app.put("/api/model_mapping")
async def api_put_model_mapping(request: Request):
    body = await request.body()
    try:
        new_mapping = json.loads(body.decode("utf-8", "ignore").lstrip("\ufeff"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        return JSONResponse({"error": "请求体不是合法 JSON"}, status_code=400)
    if not isinstance(new_mapping, dict):
        return JSONResponse({"error": "映射必须是 JSON 对象"}, status_code=400)
    save_model_mapping(new_mapping)
    return JSONResponse(content=new_mapping)

@app.delete("/api/model_mapping/{model_name:path}")
async def api_delete_model_mapping(model_name: str):
    mapping = load_model_mapping()
    if model_name in mapping:
        del mapping[model_name]
        save_model_mapping(mapping)
        return JSONResponse({"ok": True, "deleted": model_name})
    return JSONResponse({"error": f"模型 {model_name} 不在映射中"}, status_code=404)

@app.websocket("/ws")
async def ws_tunnel(ws: WebSocket):
    client_host = ws.client.host if ws.client else "Unknown"
    client_addr = f"{client_host}:{ws.client.port}" if ws.client else "Unknown"
    if not verify_ws_tunnel_request(ws):
        should_log, suppressed_count = should_log_unauthorized_ws_rejection(client_host)
        if should_log:
            if suppressed_count:
                logger.warning(
                    f"🚫 拒绝未授权内网节点接入: {client_addr}，此前同源已抑制 {suppressed_count} 次"
                )
            else:
                logger.warning(f"🚫 拒绝未授权内网节点接入: {client_addr}")
        await ws.close(code=status.WS_1008_POLICY_VIOLATION)
        return

    await ws.accept()
    node_id = get_trusted_ws_node_id(ws)
    if node_id:
        if not await replace_existing_node_connection(node_id, ws):
            return
        state.ws_id_to_node_id[id(ws)] = node_id
        state.node_id_to_ws_id[node_id] = id(ws)
        node_started_at = get_ws_node_started_at(ws)
        if node_started_at is not None:
            state.ws_id_to_node_started_at[id(ws)] = node_started_at
        node_instance_id = get_ws_node_instance_id(ws)
        if node_instance_id:
            state.ws_id_to_node_instance_id[id(ws)] = node_instance_id
    state.active_clients.append(ws)
    state.client_cooldowns.pop(id(ws), None)
    logger.info(f"✅ 内网节点已接入: {node_id or client_addr}。当前在线节点数: {len(state.active_clients)}")
    
    try:
        while True:
            msg = await ws.receive_text()
            data = json.loads(msg)
            req_id = data.get("req_id")
            if req_id and req_id in state.pending_queues:
                touch_pending_request(req_id)
                state.pending_queues[req_id].put_nowait(data)
    except WebSocketDisconnect:
        logger.warning(f"❌ 内网节点主动断开: {client_addr}")
    except Exception as e:
        logger.error(f"❌ 内网节点异常断开: {client_addr}, 错误: {e}")
    finally:
        orphan_count = cleanup_client_state(ws)
        if orphan_count:
            logger.warning(f"🧹 节点断开，已清理 {orphan_count} 个孤儿请求队列")
        logger.info(f"当前在线节点数: {len(state.active_clients)}")


def get_next_client() -> WebSocket | None:
    if not state.active_clients:
        return None
    now = time.time()
    available_clients: list[WebSocket] = []
    for client in state.active_clients:
        if state.client_cooldowns.get(id(client), 0) <= now:
            available_clients.append(client)
    if not available_clients:
        return None
    if state.current_client_index >= len(available_clients):
        state.current_client_index = 0
    client = available_clients[state.current_client_index]
    state.current_client_index = (state.current_client_index + 1) % len(available_clients)
    return client


def get_available_client_count() -> int:
    now = time.time()
    return sum(1 for c in state.active_clients if state.client_cooldowns.get(id(c), 0) <= now)


def touch_pending_request(req_id: str) -> None:
    if req_id in state.pending_queues:
        state.req_id_timestamps[req_id] = time.time()


def create_pending_request() -> tuple[str, asyncio.Queue]:
    if len(state.pending_queues) >= MAX_PENDING_QUEUES:
        raise RuntimeError("pending queue 已满")
    req_id = str(uuid.uuid4())
    queue: asyncio.Queue = asyncio.Queue()
    state.pending_queues[req_id] = queue
    state.req_id_timestamps[req_id] = time.time()
    return req_id, queue


def cleanup_pending_request(req_id: str) -> None:
    state.pending_queues.pop(req_id, None)
    state.req_id_timestamps.pop(req_id, None)
    ws_id = state.req_id_to_ws_id.pop(req_id, None)
    if ws_id is not None:
        req_ids = state.ws_to_req_ids.get(ws_id)
        if req_ids is not None:
            req_ids.discard(req_id)
            if not req_ids:
                state.ws_to_req_ids.pop(ws_id, None)


def cooldown_client(ws: WebSocket, seconds: int, reason: str) -> None:
    cooldown_until = time.time() + max(seconds, 0)
    state.client_cooldowns[id(ws)] = cooldown_until
    logger.warning(
        f"⛔ 节点 {node_label(ws)} 因 {reason} 进入冷却 {seconds}s，"
        f"冷却结束时间戳: {int(cooldown_until)}"
    )

async def drain_and_close(req_id: str, queue: asyncio.Queue) -> None:
    try:
        while True:
            msg = await asyncio.wait_for(queue.get(), timeout=QUEUE_DRAIN_TIMEOUT)
            if msg.get("type") in ["finish", "error"]:
                break
    except Exception:
        pass
    finally:
        cleanup_pending_request(req_id)

def should_retry_status(status_code: int) -> bool:
    return status_code in RETRYABLE_STATUS_CODES or status_code >= 500

def build_ws_payload(req_id: str, method: str, path: str, body: str) -> str:
    return json.dumps({"req_id": req_id, "method": method, "path": path, "body": body})

async def dispatch_to_node(*, method: str, path: str, body: str, log_label: str, attempt_number: int) -> ForwardAttempt | None:
    try:
        req_id, queue = create_pending_request()
    except RuntimeError:
        logger.warning("⚠️ pending queue 已满，拒绝新请求")
        return None
        
    target_ws = get_next_client()
    if not target_ws:
        cleanup_pending_request(req_id)
        return None

    # 🌟 修复内存泄漏的双向绑定：既知道 WS 管哪些 req_id，也知道 req_id 归属于哪个 WS
    state.req_id_to_ws_id[req_id] = id(target_ws)
    state.ws_to_req_ids.setdefault(id(target_ws), set()).add(req_id)

    ws_payload = build_ws_payload(req_id, method, path, body)
    attempt_started_at = time.monotonic()
    target_node_key = node_label(target_ws)
    record_attempt_started(target_ws, node_key=target_node_key)

    try:
        await target_ws.send_text(ws_payload)
        logger.debug(f"👉 {log_label} [{req_id[:8]}] ({method} {path}) -> 节点: {node_label(target_ws)} (尝试 {attempt_number})")
    except RuntimeError:
        record_attempt_finished(target_ws=target_ws, node_key=target_node_key, status_code=0, first_byte_latency_ms=(time.monotonic() - attempt_started_at) * 1000, success=False)
        logger.warning(f"⚠️ {log_label} 转发失败，节点状态异常，尝试切换...")
        cleanup_pending_request(req_id) # 内部会自动解绑 target_ws
        cleanup_client_state(target_ws)
        return None

    try:
        first_msg = await asyncio.wait_for(queue.get(), timeout=NODE_RESPONSE_TIMEOUT)
    except asyncio.TimeoutError:
        record_attempt_finished(target_ws=target_ws, node_key=target_node_key, status_code=504, first_byte_latency_ms=(time.monotonic() - attempt_started_at) * 1000, success=False)
        raise

    record_attempt_finished(
        target_ws=target_ws,
        node_key=target_node_key,
        status_code=int(first_msg.get("status", 200)),
        first_byte_latency_ms=(time.monotonic() - attempt_started_at) * 1000,
        success=first_msg.get("type") != "error" and not should_retry_status(int(first_msg.get("status", 200))),
    )
    return ForwardAttempt(req_id=req_id, queue=queue, target_ws=target_ws, first_msg=first_msg, attempt_number=attempt_number)


async def prepare_forward_attempt(*, method: str, path: str, body: str, log_label: str, retry_state: RetryState, attempt_number: int) -> ForwardAttempt | None:
    attempt = await dispatch_to_node(method=method, path=path, body=body, log_label=log_label, attempt_number=attempt_number)
    if attempt is None:
        return None

    first_msg = attempt.first_msg
    if first_msg.get("type") == "error":
        error_text = first_msg.get("body") or "节点返回错误"
        logger.warning(f"⚠️ {log_label} 节点返回内部错误: {error_text}，尝试切换...")
        retry_state.response_text = f"Gateway Error: {error_text}"
        cleanup_pending_request(attempt.req_id)
        return None

    status_code = first_msg.get("status", 200)
    if status_code == 401:
        cooldown_client(attempt.target_ws, NODE_401_COOLDOWN_SECONDS, "401 Unauthorized")
        retry_state.status_code = 401
        retry_state.response_text = "Gateway Error: 节点鉴权失败 (401)，已临时跳过该节点"

    if should_retry_status(status_code):
        logger.warning(f"⚠️ {log_label} 节点返回状态码 {status_code}，触发自动重试 (当前 attempt={attempt_number})...")
        retry_state.status_code = status_code
        _track_task(asyncio.create_task(drain_and_close(attempt.req_id, attempt.queue)))
        return None

    return attempt


def normalize_response_headers(headers: dict | None) -> tuple[str, dict]:
    response_headers = dict(headers or {})
    content_type = response_headers.pop("content-type", "application/json")
    for key in ["content-length", "transfer-encoding", "content-encoding", "connection"]:
        response_headers.pop(key, None)
    return content_type, response_headers


async def collect_response_body(current_req_id: str, current_queue: asyncio.Queue, timeout: int = 120) -> str:
    chunks: list[str] = []
    try:
        while True:
            msg = await asyncio.wait_for(current_queue.get(), timeout=timeout)
            if msg.get("type") == "finish":
                break
            if msg.get("type") == "error":
                raise RuntimeError(msg.get("body") or "节点返回错误")
            if msg.get("type") == "chunk":
                chunks.append(msg.get("body", ""))
    finally:
        cleanup_pending_request(current_req_id)
    return "".join(chunks)

# -------------- API 路由定义 --------------

@app.post("/v1/audio/speech")
async def audio_speech_handler(payload: AudioSpeechRequest):
    if not state.active_clients:
        return Response("Gateway Error: 没有可用的内网节点", status_code=503)

    input_text = payload.input.strip()
    if not input_text:
        return JSONResponse({"error": {"message": "`input` 不能为空"}}, status_code=400)

    messages = []
    if isinstance(payload.instructions, str) and payload.instructions.strip():
        messages.append({"role": "user", "content": payload.instructions})
    messages.append({"role": "assistant", "content": input_text})

    mimo_payload = {
        "model": map_openai_tts_model(payload.model),
        "messages": messages,
        "audio": {"format": payload.response_format.lower(), "voice": map_openai_tts_voice(payload.voice)},
    }
    body_text = json.dumps(mimo_payload, ensure_ascii=False)
    
    max_retries = min(MAX_RETRIES, get_available_client_count())
    if max_retries == 0:
        return Response("Gateway Error: 没有可用的内网节点", status_code=503)
        
    retry_state = RetryState()
    route_key = "/v1/audio/speech"
    request_started_at = time.monotonic()
    record_request_started(route_key, is_streaming=False)

    for attempt in range(max_retries):
        req_id = "unknown"
        try:
            prepared = await prepare_forward_attempt(method="POST", path="/v1/chat/completions", body=body_text, log_label="TTS 映射请求", retry_state=retry_state, attempt_number=attempt + 1)
            if prepared is None:
                continue
            req_id = prepared.req_id
            queue = prepared.queue
            first_msg = prepared.first_msg
            first_byte_at = time.monotonic()

            raw_body = await collect_response_body(req_id, queue)
            status_code = first_msg.get("status", 200)
            
            if status_code >= 400:
                record_error(route_key, status_code, f"上游返回 {status_code}", detail=raw_body[:500])
                content_type, response_headers = normalize_response_headers(first_msg.get("headers", {}))
                record_request_finished(route_key=route_key, status_code=status_code, started_at=request_started_at, first_byte_at=first_byte_at, success=False)
                return Response(raw_body, status_code=status_code, media_type=content_type, headers=response_headers)

            try:
                response_json = json.loads(raw_body)
            except json.JSONDecodeError:
                record_request_finished(route_key=route_key, status_code=502, started_at=request_started_at, first_byte_at=first_byte_at, success=False)
                return JSONResponse({"error": {"message": "上游 TTS 返回了非法 JSON"}}, status_code=502)

            audio_b64, actual_format = extract_audio_payload(response_json)
            if not audio_b64:
                record_request_finished(route_key=route_key, status_code=502, started_at=request_started_at, first_byte_at=first_byte_at, success=False)
                return JSONResponse({"error": {"message": "上游 TTS 响应里没有音频数据"}}, status_code=502)

            try:
                audio_bytes = base64.b64decode(audio_b64, validate=True)
            except binascii.Error:
                try:
                    audio_bytes = base64.b64decode(audio_b64)
                except (binascii.Error, TypeError):
                    record_request_finished(route_key=route_key, status_code=502, started_at=request_started_at, first_byte_at=first_byte_at, success=False)
                    return JSONResponse({"error": {"message": "上游 TTS 音频数据损坏"}}, status_code=502)

            record_request_finished(route_key=route_key, status_code=200, started_at=request_started_at, first_byte_at=first_byte_at, success=True)
            return Response(audio_bytes, media_type=audio_media_type((actual_format or payload.response_format).lower()))

        except asyncio.TimeoutError:
            retry_state.status_code = 504
            retry_state.response_text = "Gateway Error: 请求内网节点超时 (30s)"
            cleanup_pending_request(req_id)
            continue
        except RuntimeError as exc:
            retry_state.status_code = 502
            retry_state.response_text = f"Gateway Error: {exc}"
            cleanup_pending_request(req_id)
            continue
        except Exception as e:
            cleanup_pending_request(req_id)
            raise e

    record_request_finished(route_key=route_key, status_code=retry_state.status_code, started_at=request_started_at, first_byte_at=None, success=False)
    return Response(retry_state.response_text, status_code=retry_state.status_code)

@app.post("/v1/responses")
async def responses_handler(request: Request):
    if not state.active_clients:
        return Response("Gateway Error: 没有可用的内网节点", status_code=503)

    body = await request.body()
    try:
        req_body = json.loads(body.decode("utf-8", "ignore").lstrip("\ufeff"))
        chat_req = responses_convert_request(req_body)
    except Exception as exc:
        record_error("/v1/responses", 400, f"请求解析/转换失败: {exc}")
        return JSONResponse({"error": {"message": f"请求解析失败: {exc}"}}, status_code=400)

    model = chat_req.get("model", "")
    is_streaming = chat_req.get("stream", False) is True
    if "stream" not in req_body:
        is_streaming = True
        chat_req["stream"] = True

    chat_body_text = json.dumps(chat_req, ensure_ascii=False)
    max_retries = min(MAX_RETRIES, get_available_client_count())
    if max_retries == 0:
        return Response("Gateway Error: 没有可用的内网节点", status_code=503)
        
    retry_state = RetryState()
    route_key = "/v1/responses"
    request_started_at = time.monotonic()
    record_request_started(route_key, is_streaming=is_streaming)

    for attempt in range(max_retries):
        req_id = "unknown"
        try:
            prepared = await prepare_forward_attempt(method="POST", path="/v1/chat/completions", body=chat_body_text, log_label="Responses 映射请求", retry_state=retry_state, attempt_number=attempt + 1)
            if prepared is None:
                continue
            req_id = prepared.req_id
            queue = prepared.queue
            first_msg = prepared.first_msg
            status_code = first_msg.get("status", 200)
            first_byte_at = time.monotonic()

            if status_code >= 400:
                content_type, response_headers = normalize_response_headers(first_msg.get("headers", {}))
                raw_body = await collect_response_body(req_id, queue)
                record_error("/v1/responses", status_code, f"上游返回 {status_code}", detail=raw_body[:500])
                record_request_finished(route_key=route_key, status_code=status_code, started_at=request_started_at, first_byte_at=first_byte_at, success=False)
                return Response(raw_body, status_code=status_code, media_type=content_type, headers=response_headers)

            if is_streaming:
                converter = ResponsesStreamConverter(model=model)

                async def responses_stream_generator(current_req_id, current_queue):
                    last_data_time = time.monotonic()
                    stream_succeeded = False
                    data_task = asyncio.ensure_future(current_queue.get())

                    async def _do_keepalive():
                        await asyncio.sleep(STREAM_KEEPALIVE_INTERVAL)
                        return b": keep-alive\n\n"
                    keepalive_task = asyncio.ensure_future(_do_keepalive())

                    try:
                        while True:
                            done, _ = await asyncio.wait({data_task, keepalive_task}, return_when=asyncio.FIRST_COMPLETED)

                            if keepalive_task in done:
                                elapsed = time.monotonic() - last_data_time
                                if elapsed > STREAM_CHUNK_TIMEOUT:
                                    logger.warning(f"⚠️ Responses 流式 {elapsed:.0f}s 无数据，节点可能已断开 [{current_req_id[:8]}]")
                                    break
                                yield keepalive_task.result()
                                keepalive_task = asyncio.ensure_future(_do_keepalive())
                                continue

                            last_data_time = time.monotonic()
                            data_task = asyncio.ensure_future(current_queue.get())
                            msg = done.pop().result()
                            if msg.get("type") == "finish":
                                stream_succeeded = True
                                for evt in converter.finalize():
                                    yield evt.encode("utf-8")
                                break
                            elif msg.get("type") == "error":
                                err_evt = f"event: error\ndata: {json.dumps({'type': 'error', 'message': msg.get('body')})}\n\n"
                                yield err_evt.encode("utf-8")
                                break
                            elif msg.get("type") == "chunk":
                                for line in msg.get("body", "").split("\n"):
                                    for evt in converter.process_chunk(line):
                                        yield evt.encode("utf-8")
                    finally:
                        data_task.cancel()
                        keepalive_task.cancel()
                        await asyncio.gather(data_task, keepalive_task, return_exceptions=True)
                        cleanup_pending_request(current_req_id)
                        usage_obj = getattr(converter, "_usage", None)
                        record_request_finished(route_key=route_key, status_code=status_code if stream_succeeded else 502, started_at=request_started_at, first_byte_at=first_byte_at, success=stream_succeeded, usage=usage_obj.model_dump() if usage_obj else None)

                return StreamingResponse(
                    responses_stream_generator(req_id, queue),
                    status_code=status_code,
                    media_type="text/event-stream",
                    headers={"cache-control": "no-cache", "x-accel-buffering": "no"},
                )
            else:
                raw_body = await collect_response_body(req_id, queue)
                try:
                    chat_resp = json.loads(raw_body)
                except json.JSONDecodeError:
                    record_request_finished(route_key=route_key, status_code=502, started_at=request_started_at, first_byte_at=first_byte_at, success=False)
                    return JSONResponse({"error": {"message": "上游返回了非法 JSON"}}, status_code=502)

                responses_resp = responses_convert_response(chat_resp)
                record_request_finished(route_key=route_key, status_code=status_code, started_at=request_started_at, first_byte_at=first_byte_at, success=True, usage=chat_resp.get("usage"))
                return JSONResponse(content=responses_resp)

        except asyncio.TimeoutError:
            retry_state.status_code = 504
            retry_state.response_text = "Gateway Error: 请求内网节点超时"
            cleanup_pending_request(req_id)
            continue
        except Exception as e:
            cleanup_pending_request(req_id)
            raise e

    record_request_finished(route_key=route_key, status_code=retry_state.status_code, started_at=request_started_at, first_byte_at=None, success=False)
    return Response(retry_state.response_text, status_code=retry_state.status_code)

_MODELS = [
    ("mimo-v2.5-pro", "MiMo V2.5 Pro", 1048576, 131072),
    ("mimo-v2.5", "MiMo V2.5", 1048576, 131072),
    ("mimo-v2.5-tts", "MiMo V2.5 TTS", 8192, 8192),
    ("mimo-v2-pro", "MiMo V2 Pro", 1048576, 131072),
    ("mimo-v2-flash", "MiMo V2 Flash", 256000, 131072),
    ("mimo-v2-omni", "MiMo V2 Omni", 256000, 131072),
    ("mimo-v2.5-tts-voicedesign", "MiMo V2.5 TTS VoiceDesign", 8192, 8192),
    ("mimo-v2.5-tts-voiceclone", "MiMo V2.5 TTS VoiceClone", 8192, 8192),
    ("mimo-v2-tts", "MiMo V2 TTS", 8192, 8192),
]


@app.get("/v1/models")
async def get_models():
    data = [{"id": m[0], "object": "model", "created": 1700000000, "owned_by": "mimo", "context_length": m[2], "max_tokens": m[2]} for m in _MODELS]
    return JSONResponse(content={"object": "list", "data": data})

@app.get("/anthropic/v1/models")
async def get_anthropic_models():
    data = [
        {
            "id": model_id,
            "display_name": display_name,
            "created_at": "2025-01-01T00:00:00Z",
            "type": "model",
            "max_input_tokens": context_length,
            "max_tokens": max_output_tokens,
        }
        for model_id, display_name, context_length, max_output_tokens in _MODELS
    ]
    return JSONResponse(content={"data": data, "has_more": False, "first_id": data[0]["id"], "last_id": data[-1]["id"]})

@app.post("/v1/chat/completions")
async def chat_completions_handler(request: Request):
    return await _forward_request(request, "/v1/chat/completions")

@app.post("/anthropic/v1/messages")
async def anthropic_messages_handler(request: Request):
    return await _forward_request(request, "/anthropic/v1/messages")

async def _forward_request(request: Request, path: str):
    if not state.active_clients:
        return Response("Gateway Error: 没有可用的内网节点", status_code=503)

    body = await request.body()
    method = request.method
    max_retries = min(MAX_RETRIES, get_available_client_count())
    if max_retries == 0:
        return Response("Gateway Error: 没有可用的内网节点", status_code=503)

    retry_state = RetryState()
    body_text = body.decode("utf-8", "ignore").lstrip("\ufeff")
    body_text = apply_model_mapping(body_text)
    route_key = path
    request_started_at = time.monotonic()

    is_streaming = False
    try:
        is_streaming = json.loads(body_text).get("stream", False) is True
    except (json.JSONDecodeError, AttributeError):
        pass
    record_request_started(route_key, is_streaming=is_streaming)

    for attempt in range(max_retries):
        req_id = "unknown"
        try:
            prepared = await prepare_forward_attempt(method=method, path=path, body=body_text, log_label="转发请求", retry_state=retry_state, attempt_number=attempt + 1)
            if prepared is None:
                continue
            req_id = prepared.req_id
            queue = prepared.queue
            first_msg = prepared.first_msg
            status_code = first_msg.get("status", 200)
            first_byte_at = time.monotonic()
            content_type, response_headers = normalize_response_headers(first_msg.get("headers", {}))

            async def stream_generator(current_req_id, current_queue, use_keepalive):
                last_data_time = time.monotonic()
                data_task = asyncio.ensure_future(current_queue.get())
                keepalive_task = None
                stream_succeeded = False
                usage_data = None

                async def _do_keepalive():
                    await asyncio.sleep(STREAM_KEEPALIVE_INTERVAL)
                    return b": keep-alive\n\n"
                if use_keepalive:
                    keepalive_task = asyncio.ensure_future(_do_keepalive())

                try:
                    while True:
                        pending = {data_task}
                        if keepalive_task is not None:
                            pending.add(keepalive_task)
                        done, _ = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)

                        if keepalive_task is not None and keepalive_task in done:
                            elapsed = time.monotonic() - last_data_time
                            if elapsed > STREAM_CHUNK_TIMEOUT:
                                logger.warning(f"⚠️ 流式 {elapsed:.0f}s 无数据，节点可能已断开 [{current_req_id[:8]}]")
                                break
                            yield keepalive_task.result()
                            keepalive_task = asyncio.ensure_future(_do_keepalive())
                            continue

                        last_data_time = time.monotonic()
                        data_task = asyncio.ensure_future(current_queue.get())
                        msg = done.pop().result()
                        if msg.get("type") == "finish":
                            stream_succeeded = True
                            break
                        elif msg.get("type") == "chunk":
                            chunk_body = msg.get("body", "")
                            if usage_data is None:
                                usage_data = extract_usage_from_sse_chunk(chunk_body)
                            yield chunk_body.encode("utf-8")
                finally:
                    data_task.cancel()
                    if keepalive_task is not None:
                        keepalive_task.cancel()
                    await asyncio.gather(*[t for t in (data_task, keepalive_task) if t is not None], return_exceptions=True)
                    cleanup_pending_request(current_req_id)
                    record_request_finished(route_key=route_key, status_code=status_code if stream_succeeded else 502, started_at=request_started_at, first_byte_at=first_byte_at, success=stream_succeeded and status_code < 400, usage=usage_data)

            if status_code >= 400:
                record_error(route_key, status_code, f"上游返回 {status_code}", detail=first_msg.get("body", "")[:300])

            return StreamingResponse(stream_generator(req_id, queue, use_keepalive=is_streaming), status_code=status_code, media_type=content_type, headers=response_headers)

        except asyncio.TimeoutError:
            retry_state.status_code = 504
            retry_state.response_text = "Gateway Error: 请求所有节点超时 (30s)"
            cleanup_pending_request(req_id)
            continue
        except Exception as e:
            cleanup_pending_request(req_id)
            raise e

    record_request_finished(route_key=route_key, status_code=retry_state.status_code, started_at=request_started_at, first_byte_at=None, success=False)
    return Response(retry_state.response_text, status_code=retry_state.status_code)

if __name__ == "__main__":
    logger.info("🚀 启动支持多节点的公网网关...")
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        ws_max_size=10**8,
        timeout_graceful_shutdown=int(SHUTDOWN_TASK_TIMEOUT),
    )
