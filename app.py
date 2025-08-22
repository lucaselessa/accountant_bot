# app.py
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, PlainTextResponse
from urllib.parse import parse_qs
from pathlib import Path
from dotenv import load_dotenv
import os, time, json, requests

# =============== CONFIG ===============
# carrega o .env na mesma pasta do app.py
load_dotenv(Path(__file__).resolve().parent / ".env")

API_BASE   = (os.getenv("SEATALK_API_BASE") or "https://openapi.seatalk.io").rstrip("/")
APP_ID     = os.getenv("SEATALK_APP_ID")
APP_SECRET = os.getenv("SEATALK_APP_SECRET")
BOT_ID     = os.getenv("SEATALK_BOT_ID")
TOKEN_URL  = os.getenv("SEATALK_TOKEN_URL") or f"{API_BASE}/auth/app_access_token"  # endpoint correto

app = FastAPI()
_token_cache = {"value": None, "exp": 0.0}

# =============== TOKEN ===============
def get_app_token() -> str:
    """Busca (e cacheia) o app_access_token."""
    now = time.time()
    if _token_cache["value"] and (_token_cache["exp"] - now) > 60:
        return _token_cache["value"]

    if not APP_ID or not APP_SECRET:
        raise RuntimeError("APP_ID/APP_SECRET não configurados")

    r = requests.post(
        TOKEN_URL,
        json={"app_id": APP_ID, "app_secret": APP_SECRET},
        headers={"Content-Type": "application/json"},
        timeout=10,
    )
    ct = (r.headers.get("content-type") or "").lower()
    print(f"TOKEN POST {TOKEN_URL} -> {r.status_code} ct={ct!r}")
    if r.status_code != 200 or "application/json" not in ct:
        raise RuntimeError(f"Falha ao obter token: status={r.status_code} body={r.text[:200]}")

    data = r.json()
    tok = data.get("app_access_token") or data.get("access_token")
    exp = int(data.get("expire") or data.get("expires_in") or 3600)
    if not tok:
        raise RuntimeError(f"Resposta sem token: {data}")
    _token_cache["value"] = tok
    _token_cache["exp"] = now + exp
    return tok

# =============== ENVIAR DM ===============
def send_text_dm(employee_code: str, text: str) -> bool:
    """
    Envia DM pro usuário (1:1).
    Endpoint v2 oficial: /messaging/v2/single_chat  (Authorization: Bearer <token>)
    Payload: { "employee_code": "...", "message": {"tag":"text","text":{"content":"..."}} }
    """
    token = get_app_token()
    url = f"{API_BASE}/messaging/v2/single_chat"
    payload = {
        "employee_code": employee_code,
        "message": {"tag": "text", "text": {"content": text}},
    }
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    r = requests.post(url, json=payload, headers=headers, timeout=10)
    print("SEND RESP:", r.status_code, r.text[:200])
    return r.status_code == 200

# =============== WEBHOOK / EVENTOS ===============
@app.post("/seatalk/events")
async def seatalk_events(request: Request):
    # Lê o corpo independente do Content-Type
    ct = (request.headers.get("content-type") or "").lower()
    raw = await request.body()
    body_text = raw.decode("utf-8", errors="ignore")
    payload = {}
    try:
        if "application/json" in ct and body_text.strip():
            payload = json.loads(body_text)
        elif "application/x-www-form-urlencoded" in ct:
            payload = {k: v[0] for k, v in parse_qs(body_text).items()}
        elif "multipart/form-data" in ct:
            form = await request.form()
            payload = dict(form)
    except Exception:
        payload = {}

    # Handshake (event_verification): responder com seatalk_challenge
    challenge = None
    if isinstance(payload, dict):
        challenge = payload.get("seatalk_challenge") or payload.get("challenge")
        if not challenge and isinstance(payload.get("event"), dict):
            challenge = payload["event"].get("seatalk_challenge") or payload["event"].get("challenge")
    if challenge:
        return JSONResponse({"seatalk_challenge": str(challenge)})

    # Eventos normais
    evt_type = payload.get("event_type")
    evt = payload.get("event", {}) if isinstance(payload, dict) else {}
    if evt_type and evt_type != "event_verification":
        # normalmente vem seatalk_id e employee_code no event
        seatalk_id = evt.get("seatalk_id")
        employee_code = evt.get("employee_code")
        msg = evt.get("message") or {}
        text_in = ((msg.get("text") or {}).get("content") or "").strip().lower()
        print("INCOMING:", {"seatalk_id": seatalk_id, "employee_code": employee_code, "text": text_in, "evt_type": evt_type})

        if employee_code and text_in == "ping":
            try:
                send_text_dm(employee_code, "pong")
            except Exception as e:
                print("SEND ERROR:", repr(e))

    return PlainTextResponse("ok")
