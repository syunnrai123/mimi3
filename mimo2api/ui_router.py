import os
import json
import logging
import re
import time
import asyncio
import httpx
from fastapi import APIRouter, Request, Response
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from .auth import (
    create_webui_session_token,
    get_webui_cookie_name,
    get_webui_session_ttl,
    get_webui_username,
    is_ai_auth_enabled,
    is_web_auth_enabled,
    is_webui_authenticated,
    verify_webui_login,
    webui_cookie_secure,
)
from .gateway_state import state

router = APIRouter()
logger = logging.getLogger(__name__)

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
USERS_DIR = os.path.join(ROOT_DIR, "users")
LOGS_DIR = os.path.join(ROOT_DIR, "logs")
DEFAULT_LOG_FILE = "gateway.log"
MAX_LOG_READ_BYTES = 512 * 1024
SENSITIVE_LOG_PATTERNS = [
    re.compile(r'(?i)\b(serviceToken\s*["\']?\s*[:=]\s*["\']?)([^;,\s"\']+)'),
    re.compile(r'(?i)\b(xiaomichatbot_ph\s*["\']?\s*[:=]\s*["\']?)([^;,\s"\']+)'),
    re.compile(r'(?i)\b(authorization\s*[:=]\s*bearer\s+)([^;,\s"\']+)'),
    re.compile(r'(?i)\b((?:x-api-key|api-key)\s*[:=]\s*["\']?)([^;,\s"\']+)'),
]


@router.get("/")
async def root_page():
    return RedirectResponse(url="/webui", status_code=307)

@router.get("/webui")
async def webui_page():
    ui_path = os.path.join(os.path.dirname(__file__), "webui.html")
    if os.path.exists(ui_path):
        with open(ui_path, "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    return Response("webui.html not found", status_code=404)

@router.get("/api/system/status")
async def api_status():
    return JSONResponse({"active_clients": len(state.active_clients)})


def list_log_files() -> list[str]:
    if not os.path.isdir(LOGS_DIR):
        return []
    files = []
    for fn in os.listdir(LOGS_DIR):
        path = os.path.join(LOGS_DIR, fn)
        if not os.path.isfile(path):
            continue
        if fn == DEFAULT_LOG_FILE or fn.startswith(f"{DEFAULT_LOG_FILE}.") or fn.endswith(".log"):
            files.append(fn)
    return sorted(files)


def redact_log_line(line: str) -> str:
    redacted = line
    for pattern in SENSITIVE_LOG_PATTERNS:
        redacted = pattern.sub(r"\1***", redacted)
    return redacted


@router.get("/api/logs")
async def api_logs(request: Request, file: str = DEFAULT_LOG_FILE, limit: int = 300):
    if not is_web_auth_enabled():
        return JSONResponse(
            {"detail": "查看日志需要先配置 MIMO_WEBUI_PASSWORD 以启用 WebUI 鉴权"},
            status_code=403,
        )
    if not is_webui_authenticated(request):
        return JSONResponse({"detail": "Unauthorized"}, status_code=401)

    limit = max(20, min(int(limit or 300), 1000))
    files = list_log_files()
    selected_file = os.path.basename(file or DEFAULT_LOG_FILE)

    if selected_file not in files:
        if selected_file != DEFAULT_LOG_FILE:
            return JSONResponse({"detail": "日志文件不存在"}, status_code=404)
        if files:
            selected_file = files[0]
        else:
            return JSONResponse({
                "file": selected_file,
                "files": files,
                "lines": [],
                "line_count": 0,
                "truncated": False,
            })

    log_path = os.path.realpath(os.path.join(LOGS_DIR, selected_file))
    logs_root = os.path.realpath(LOGS_DIR)
    try:
        is_inside_logs_dir = os.path.commonpath([logs_root, log_path]) == logs_root
    except ValueError:
        is_inside_logs_dir = False
    if not is_inside_logs_dir:
        return JSONResponse({"detail": "非法日志路径"}, status_code=400)

    try:
        size = os.path.getsize(log_path)
        with open(log_path, "rb") as f:
            if size > MAX_LOG_READ_BYTES:
                f.seek(size - MAX_LOG_READ_BYTES)
            raw = f.read()
        text = raw.decode("utf-8", errors="replace")
        lines = [redact_log_line(line) for line in text.splitlines()[-limit:]]
        return JSONResponse({
            "file": selected_file,
            "files": files,
            "lines": lines,
            "line_count": len(lines),
            "truncated": size > MAX_LOG_READ_BYTES,
        })
    except OSError as exc:
        logger.warning("读取日志失败: %s", exc)
        return JSONResponse({"detail": "读取日志失败"}, status_code=500)


@router.get("/api/auth/session")
async def api_auth_session(request: Request):
    auth_enabled = is_web_auth_enabled()
    authenticated = is_webui_authenticated(request)
    return JSONResponse({
        "enabled": auth_enabled,
        "authenticated": authenticated,
        "username": get_webui_username(),
        "ai_auth_enabled": is_ai_auth_enabled(),
    })


@router.post("/api/auth/login")
async def api_auth_login(request: Request):
    if not is_web_auth_enabled():
        return JSONResponse({"ok": True, "enabled": False, "username": get_webui_username()})

    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"detail": "请求体不是合法 JSON"}, status_code=400)

    username = str(body.get("username", "")).strip()
    password = str(body.get("password", ""))
    if not verify_webui_login(username, password):
        return JSONResponse({"detail": "用户名或密码错误"}, status_code=401)

    response = JSONResponse({"ok": True, "enabled": True, "username": get_webui_username()})
    response.set_cookie(
        key=get_webui_cookie_name(),
        value=create_webui_session_token(get_webui_username()),
        max_age=get_webui_session_ttl(),
        httponly=True,
        samesite="lax",
        secure=webui_cookie_secure(),
        path="/",
    )
    return response


@router.post("/api/auth/logout")
async def api_auth_logout():
    response = JSONResponse({"ok": True})
    response.delete_cookie(key=get_webui_cookie_name(), path="/")
    return response

async def fetch_user_status(data: dict) -> dict:
    uid = data.get("userId")
    cookies = {
        "serviceToken": data.get("serviceToken", ""),
        "userId": uid,
        "xiaomichatbot_ph": data.get("xiaomichatbot_ph", "")
    }
    url = "https://aistudio.xiaomimimo.com/open-apis/user/mimo-claw/status"
    headers = {
        "Accept": "*/*",
        "Content-Type": "application/json",
        "Origin": "https://aistudio.xiaomimimo.com",
        "Referer": "https://aistudio.xiaomimimo.com/",
        "User-Agent": "Mozilla/5.0"
    }
    try:
        async with httpx.AsyncClient() as c:
            r = await c.get(url, cookies=cookies, headers=headers, timeout=5)
            if r.status_code == 401:
                return {**data, "claw_status": "EXPIRED(401)", "remain_sec": 0}
            r_data = r.json()
            st = r_data.get("data", {}).get("status", "UNKNOWN")
            expire_ms = r_data.get("data", {}).get("expireTime")
            remain_sec = max(0, int(int(expire_ms) / 1000 - time.time())) if expire_ms else 0
            return {**data, "claw_status": st, "remain_sec": remain_sec}
    except Exception:
        return {**data, "claw_status": "ERROR", "remain_sec": 0}

@router.get("/api/users/list")
async def api_users_list():
    raw_users = []
    if os.path.exists(USERS_DIR):
        for fn in os.listdir(USERS_DIR):
            if fn.startswith("user_") and fn.endswith(".json"):
                try:
                    with open(os.path.join(USERS_DIR, fn), "r", encoding="utf-8") as f:
                        raw_users.append(json.load(f))
                except:
                    pass

    # 并发查询所有用户的实例状态
    tasks = [fetch_user_status(rd) for rd in raw_users]
    results = await asyncio.gather(*tasks) if raw_users else []

    users = []
    for data in results:
        users.append({
            "userId": data.get("userId"),
            "name": data.get("name"),
            "serviceToken": data.get("serviceToken"),
            "claw_status": data.get("claw_status", "UNKNOWN"),
            "remain_sec": data.get("remain_sec", 0)
        })
    return JSONResponse({"users": users})

@router.post("/api/users/add")
async def api_users_add(request: Request):
    try:
        body = await request.json()
        raw_text = body.get("raw_text", "")
        # 解析正则提取
        parsed = {}
        for match in re.finditer(r'([a-zA-Z0-9_]+)="?([^;"]+)"?', raw_text):
            parsed[match.group(1)] = match.group(2)
            
        uid = parsed.get("userId")
        st = parsed.get("serviceToken")
        ph = parsed.get("xiaomichatbot_ph")
        
        if not uid or not st or not ph:
            return JSONResponse({"detail": "缺少必要字段 userId, serviceToken 或 xiaomichatbot_ph"}, status_code=400)
            
        os.makedirs(USERS_DIR, exist_ok=True)
        target_file = os.path.join(USERS_DIR, f"user_{uid}.json")
        
        user_data = {
            "userId": uid,
            "serviceToken": st,
            "xiaomichatbot_ph": ph,
            "name": f"Imported_{uid}"
        }
        with open(target_file, "w", encoding="utf-8") as f:
            json.dump(user_data, f, ensure_ascii=False, indent=2)
            
        return JSONResponse({"status": "ok", "userId": uid})
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=500)

@router.delete("/api/users/delete/{uid}")
async def api_users_delete(uid: str):
    target_file = os.path.join(USERS_DIR, f"user_{uid}.json")
    if os.path.exists(target_file):
        os.remove(target_file)
        return JSONResponse({"status": "ok"})
    return JSONResponse({"detail": "User not found"}, status_code=404)
