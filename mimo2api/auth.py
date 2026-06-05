import base64
import hashlib
import hmac
import json
import os
import secrets
import time

from fastapi import Request, WebSocket
from fastapi.responses import JSONResponse

AI_AUTH_ENV = "MIMO_RELAY_OPENAI_KEY"
WS_TUNNEL_KEY_ENV = "MIMO_WS_TUNNEL_KEY"
WEBUI_USERNAME_ENV = "MIMO_WEBUI_USERNAME"
WEBUI_PASSWORD_ENV = "MIMO_WEBUI_PASSWORD"
WEBUI_SECRET_ENV = "MIMO_WEBUI_SECRET"
WEBUI_SESSION_TTL_ENV = "MIMO_WEBUI_SESSION_TTL_SECONDS"
WEBUI_COOKIE_NAME_ENV = "MIMO_WEBUI_COOKIE_NAME"
WEBUI_COOKIE_SECURE_ENV = "MIMO_WEBUI_COOKIE_SECURE"


def _read_env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def is_ai_auth_enabled() -> bool:
    return bool(_read_env(AI_AUTH_ENV))


def is_ws_tunnel_auth_enabled() -> bool:
    return bool(_read_env(WS_TUNNEL_KEY_ENV))


def is_web_auth_enabled() -> bool:
    return bool(_read_env(WEBUI_PASSWORD_ENV))


def get_ai_api_key() -> str:
    return _read_env(AI_AUTH_ENV)


def get_ws_tunnel_key() -> str:
    return _read_env(WS_TUNNEL_KEY_ENV)


def get_webui_username() -> str:
    return _read_env(WEBUI_USERNAME_ENV, "admin") or "admin"


def get_webui_password() -> str:
    return _read_env(WEBUI_PASSWORD_ENV)


def get_webui_cookie_name() -> str:
    return _read_env(WEBUI_COOKIE_NAME_ENV, "mimo_webui_session") or "mimo_webui_session"


def get_webui_session_ttl() -> int:
    raw_value = _read_env(WEBUI_SESSION_TTL_ENV, "43200")
    try:
        return max(300, int(raw_value))
    except ValueError:
        return 43200


def webui_cookie_secure() -> bool:
    return _read_env(WEBUI_COOKIE_SECURE_ENV).lower() in {"1", "true", "yes", "on"}


def _get_webui_secret() -> str:
    secret_value = _read_env(WEBUI_SECRET_ENV)
    if secret_value:
        return secret_value
    password = get_webui_password()
    if password:
        return password
    return get_ai_api_key() or "mimo2-webui-fallback-secret"


def extract_ai_api_key(request: Request) -> str | None:
    auth_header = request.headers.get("authorization", "")
    if auth_header.lower().startswith("bearer "):
        return auth_header[7:].strip()

    for header_name in ("x-api-key", "api-key"):
        header_value = request.headers.get(header_name, "").strip()
        if header_value:
            return header_value
    return None


def verify_ai_api_key(candidate: str | None) -> bool:
    expected = get_ai_api_key()
    if not expected:
        return True
    if not candidate:
        return False
    return secrets.compare_digest(candidate, expected)


def extract_ws_tunnel_key(ws: WebSocket) -> str | None:
    auth_header = ws.headers.get("authorization", "")
    if auth_header.lower().startswith("bearer "):
        return auth_header[7:].strip()

    header_value = ws.headers.get("x-ws-token", "").strip()
    if header_value:
        return header_value

    query_value = ws.query_params.get("token", "").strip()
    if query_value:
        return query_value
    return None


def verify_ws_tunnel_key(candidate: str | None) -> bool:
    expected = get_ws_tunnel_key()
    if not expected:
        return True
    if not candidate:
        return False
    return secrets.compare_digest(candidate, expected)


def verify_ws_tunnel_request(ws: WebSocket) -> bool:
    return verify_ws_tunnel_key(extract_ws_tunnel_key(ws))


def require_ai_request(request: Request) -> JSONResponse | None:
    if not is_ai_auth_enabled():
        return None

    if verify_ai_api_key(extract_ai_api_key(request)):
        return None

    return JSONResponse(
        {
            "error": {
                "message": "Unauthorized: missing or invalid API key",
                "type": "invalid_request_error",
            }
        },
        status_code=401,
        headers={"WWW-Authenticate": "Bearer"},
    )


def verify_webui_login(username: str, password: str) -> bool:
    expected_username = get_webui_username()
    expected_password = get_webui_password()
    if not expected_password:
        return True
    return secrets.compare_digest(username or "", expected_username) and secrets.compare_digest(password or "", expected_password)


def _urlsafe_b64encode(raw_text: str) -> str:
    return base64.urlsafe_b64encode(raw_text.encode("utf-8")).decode("ascii").rstrip("=")


def _urlsafe_b64decode(encoded_text: str) -> str:
    padding = "=" * (-len(encoded_text) % 4)
    return base64.urlsafe_b64decode((encoded_text + padding).encode("ascii")).decode("utf-8")


def create_webui_session_token(username: str, now: int | None = None) -> str:
    issued_at = int(time.time() if now is None else now)
    payload = {
        "u": username,
        "iat": issued_at,
        "exp": issued_at + get_webui_session_ttl(),
    }
    payload_encoded = _urlsafe_b64encode(json.dumps(payload, separators=(",", ":"), ensure_ascii=False))
    signature = hmac.new(
        _get_webui_secret().encode("utf-8"),
        payload_encoded.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return f"{payload_encoded}.{signature}"


def parse_webui_session_token(token: str | None, now: int | None = None) -> dict | None:
    if not token or "." not in token:
        return None
    payload_encoded, provided_signature = token.split(".", 1)
    expected_signature = hmac.new(
        _get_webui_secret().encode("utf-8"),
        payload_encoded.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    if not secrets.compare_digest(provided_signature, expected_signature):
        return None

    try:
        payload = json.loads(_urlsafe_b64decode(payload_encoded))
    except (json.JSONDecodeError, ValueError, UnicodeDecodeError):
        return None

    current_time = int(time.time() if now is None else now)
    if int(payload.get("exp", 0)) < current_time:
        return None
    return payload


def is_webui_authenticated(request: Request) -> bool:
    if not is_web_auth_enabled():
        return True
    token = request.cookies.get(get_webui_cookie_name())
    payload = parse_webui_session_token(token)
    return bool(payload and payload.get("u") == get_webui_username())


def require_webui_request(request: Request) -> JSONResponse | None:
    if is_webui_authenticated(request):
        return None
    return JSONResponse(
        {"detail": "Unauthorized"},
        status_code=401,
    )
