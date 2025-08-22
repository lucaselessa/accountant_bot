from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, PlainTextResponse
from urllib.parse import parse_qs
from pathlib import Path
from dotenv import load_dotenv
import os, time, json, requests, io, re
from datetime import datetime
import zoneinfo

# ---------- Extras (Drive + Data) ----------
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload

# =============== CONFIG GERAL ===============
load_dotenv(Path(__file__).resolve().parent / ".env")

API_BASE   = (os.getenv("SEATALK_API_BASE") or "https://openapi.seatalk.io").rstrip("/")
APP_ID     = os.getenv("SEATALK_APP_ID")
APP_SECRET = os.getenv("SEATALK_APP_SECRET")
BOT_ID     = os.getenv("SEATALK_BOT_ID")
TOKEN_URL  = os.getenv("SEATALK_TOKEN_URL") or f"{API_BASE}/auth/app_access_token"

# Google Drive
GDRIVE_SA_JSON = (os.getenv("GDRIVE_SA_JSON") or "").strip()
GDRIVE_FOLDER_GL = (os.getenv("GDRIVE_FOLDER_GL") or "").strip()                 # origem (GL_FP_YYYY_MM.xlsx)
GDRIVE_OUTPUT_FOLDER_ID = (os.getenv("GDRIVE_OUTPUT_FOLDER_ID") or "").strip()   # saída (onde salvar o .xlsx gerado)

app = FastAPI()
_token_cache = {"value": None, "exp": 0.0}
_started_at = time.time()
TZ_BRT = zoneinfo.ZoneInfo("America/Sao_Paulo")

def now_brt_str():
    return datetime.now(TZ_BRT).strftime("%Y-%m-%d %H:%M:%S %Z")

# =============== TOKEN ===============
def get_app_token() -> str:
    """Busca (e cacheia) o app_access_token do SeaTalk."""
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
    Endpoint v2 oficial: /messaging/v2/single_chat
    Payload: { "employee_code": "...", "message": {"tag":"text","text":{"content":"..."}} }
    """
    token = get_app_token()
    url = f"{API_BASE}/messaging/v2/single_chat"
    payload = {"employee_code": employee_code, "message": {"tag": "text", "text": {"content": text}}}
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    r = requests.post(url, json=payload, headers=headers, timeout=15)
    print("SEND RESP:", r.status_code, r.text[:200])
    return r.status_code == 200

# =============== GOOGLE DRIVE HELPERS ===============
def _svc(scope_readonly=True):
    if not GDRIVE_SA_JSON:
        raise RuntimeError("GDRIVE_SA_JSON não configurado.")
    info = json.loads(GDRIVE_SA_JSON)
    scopes = ["https://www.googleapis.com/auth/drive.readonly"] if scope_readonly \
             else ["https://www.googleapis.com/auth/drive"]
    creds = service_account.Credentials.from_service_account_info(info, scopes=scopes)
    return build("drive", "v3", credentials=creds, cache_discovery=False)

def _list_gl_files(limit=24):
    """Lista GL_FP_YYYY_MM.xlsx (mais recentes primeiro) na pasta origem."""
    if not GDRIVE_FOLDER_GL:
        raise RuntimeError("GDRIVE_FOLDER_GL não configurado.")
    svc = _svc(True)
    resp = svc.files().list(
        q=f"'{GDRIVE_FOLDER_GL}' in parents and trashed=false",
        fields="files(id,name,modifiedTime)",
        orderBy="modifiedTime desc",
        pageSize=limit,
        includeItemsFromAllDrives=True,
        supportsAllDrives=True,
    ).execute()
    pat = re.compile(r"^GL_FP_\d{4}_\d{2}\.xlsx$", re.I)
    files = [f for f in resp.get("files", []) if pat.match(f.get("name",""))]
    print(f"GL files found: {[f['name'] for f in files]}")
    return files

def _download_file_bytes(file_id: str) -> bytes:
    svc = _svc(True)
    req = svc.files().get_media(fileId=file_id, supportsAllDrives=True)
    buf = io.BytesIO()
    downloader = MediaIoBaseDownload(buf, req)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    return buf.getvalue()

def _upload_xlsx(local_path: str) -> str:
    if not GDRIVE_OUTPUT_FOLDER_ID:
        raise RuntimeError("GDRIVE_OUTPUT_FOLDER_ID não configurado.")
    svc = _svc(False)
    metadata = {
        "name": os.path.basename(local_path),
        "parents": [GDRIVE_OUTPUT_FOLDER_ID],
        "mimeType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    }
    media = MediaFileUpload(local_path, mimetype=metadata["mimeType"], resumable=True)
    f = svc.files().create(
        body=metadata, media_body=media,
        fields="id,webViewLink,webContentLink", supportsAllDrives=True
    ).execute()
    return f.get("webViewLink") or f.get("webContentLink")

def _read_report_output(xbytes: bytes) -> pd.DataFrame:
    # Cabeçalho começa na LINHA 2 -> header=1 (0-index no pandas)
    return pd.read_excel(io.BytesIO(xbytes), sheet_name="ReportOutput", header=1, engine="openpyxl")

# =============== BUSCAS (PR/PO e JOURNAL) ===============
def _norm_po_pr_tokens(raw: str) -> list[str]:
    s = (raw or "").strip()
    m = re.search(r"(?i)(?:SPXBR-)?(PO|PR)-?(\d+)", s)
    if m:
        typ, num = m.group(1).upper(), m.group(2)
    else:
        typ, num = None, re.sub(r"\D+", "", s)
    variants = []
    if num:
        if typ:
            variants += [f"{typ}-{num}", f"SPXBR-{typ}-{num}"]
        else:
            for t in ("PO", "PR"):
                variants += [f"{t}-{num}", f"SPXBR-{t}-{num}"]
    return [v.lower() for v in variants if v]

def _filter_po_pr(df: pd.DataFrame, query: str) -> pd.DataFrame:
    cols = []
    for c in df.columns:
        n = str(c).strip().lower()
        if n in ("po_number", "source desc", "line desc"):
            cols.append(c)
    if not cols:
        raise ValueError(f"Colunas esperadas não encontradas. Colunas: {list(df.columns)}")
    tokens = _norm_po_pr_tokens(query)
    if not tokens:
        return df.iloc[0:0].copy()
    mask = pd.Series(False, index=df.index)
    for c in cols:
        s = df[c].astype(str).str.lower()
        for t in tokens:
            mask = mask | s.str.contains(re.escape(t), na=False)
    out = df[mask].copy()
    return out

def _filter_journals(df: pd.DataFrame, journals: list[str]) -> pd.DataFrame:
    col = None
    for c in df.columns:
        if str(c).strip().upper() in ("JOURNAL_DOC_NO", "JOURNAL DOC NO", "JOURNAL_DOC"):
            col = c; break
    if not col:
        raise ValueError("Coluna JOURNAL_DOC_NO não encontrada.")
    jset = {j.strip() for j in journals if j.strip()}
    return df[df[col].astype(str).isin(jset)].copy()

def _scan_gls_apply(filter_fn, need_args, limit_files=18):
    rows = []
    files = _list_gl_files(limit_files)
    for f in files:
        try:
            xbytes = _download_file_bytes(f["id"])
            df = _read_report_output(xbytes)
            part = filter_fn(df, *need_args)
            if not part.empty:
                part["_GL_FILE_"] = f["name"]
                rows.append(part)
        except Exception as e:
            print("GL read error:", f.get("name"), repr(e))
    if rows:
        return pd.concat(rows, ignore_index=True)
    return pd.DataFrame()

def _save_and_upload(df: pd.DataFrame, tag: str) -> str:
    if df.empty:
        raise RuntimeError("Nenhuma linha para salvar.")
    safe = re.sub(r"[^A-Za-z0-9_-]+", "_", tag)[:50] or "resultado"
    fname = f"resultado_{safe}_{int(time.time())}.xlsx"
    df.to_excel(fname, index=False)
    link = _upload_xlsx(fname)
    try: os.remove(fname)
    except Exception: pass
    return link

# =============== COMANDOS TEXTO ===============
HELP_TEXT = (
    "🤖 *Seatalk Accountant Bot*\n"
    "Comandos:\n"
    "- `help` → mostra este menu\n"
    "- `status` → ver se estou online\n"
    "- `ping` → teste rápido\n"
    "- `po 12345` / `pr 12345` → busca PR/PO nos GL recentes e envia o Excel\n"
    "- `journal J12345 [J67890 ...]` → busca 1+ journals e envia Excel\n"
)

def handle_command(employee_code: str, text_in: str):
    t = (text_in or "").strip()
    tl = t.lower()

    if t in ("help", "/help", "ajuda"):
        send_text_dm(employee_code, HELP_TEXT); return

    if tl == "status":
        uptime = int(time.time() - _started_at)
        send_text_dm(employee_code, f"✅ Online — {now_brt_str()} | uptime {uptime}s"); return

    if tl == "ping":
        send_text_dm(employee_code, "pong"); return

    # ---- PR/PO ----
    m = re.search(r"(?i)\b(po|pr)\b[^0-9]*([0-9]{3,})", t)
    if m and GDRIVE_SA_JSON and GDRIVE_FOLDER_GL and GDRIVE_OUTPUT_FOLDER_ID:
        _type, _num = m.group(1).upper(), m.group(2)
        query = f"{_type}-{_num}"
        send_text_dm(employee_code, f"🔎 Buscando {_type}-{_num} nos GL mais recentes...")
        df = _scan_gls_apply(_filter_po_pr, [query], limit_files=12)
        if df.empty:
            send_text_dm(employee_code, f"❌ Não achei {_type}-{_num} nos últimos GLs.")
            return
        link = _save_and_upload(df, f"{_type}-{_num}")
        send_text_dm(employee_code, f"✅ Achei {_type}-{_num}. Baixe aqui:\n{link}")
        return

    # ---- JOURNAL ----
    if tl.startswith(("journal", "journals")) and GDRIVE_SA_JSON and GDRIVE_FOLDER_GL and GDRIVE_OUTPUT_FOLDER_ID:
        ids = re.findall(r"[A-Za-z]?\d+", t)
        journals = [j for j in ids if any(ch.isdigit() for ch in j)]
        if not journals:
            send_text_dm(employee_code, "Me diga os JOURNALs. Ex.: `journal J12345` ou `journal J123 J456`")
            return
        send_text_dm(employee_code, f"🔎 Buscando journals: {', '.join(journals)} ...")
        df = _scan_gls_apply(_filter_journals, [journals], limit_files=18)
        if df.empty:
            send_text_dm(employee_code, "❌ Não encontrei esses journals nos GLs recentes.")
            return
        link = _save_and_upload(df, "journals_" + "_".join(journals))
        send_text_dm(employee_code, f"✅ Arquivo com {len(df):,} linha(s) gerado. Baixe aqui:\n{link}")
        return

    # fallback
    send_text_dm(employee_code, "Não entendi 🤔. Digite `help` para ver os comandos.")

# =============== WEBHOOK / EVENTOS ===============
@app.post("/seatalk/events")
async def seatalk_events(request: Request):
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
        elif body_text.strip().startswith("{"):
            payload = json.loads(body_text)
    except Exception:
        payload = {}

    # Handshake (event_verification)
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
        employee_code = evt.get("employee_code")
        msg = evt.get("message") or {}
        text_in = ((msg.get("text") or {}).get("content") or "").strip()
        print("INCOMING:", {"employee_code": employee_code, "text": text_in, "evt_type": evt_type})

        if employee_code:
            try:
                handle_command(employee_code, text_in)
            except Exception as e:
                print("ERROR handle_command:", repr(e))
                send_text_dm(employee_code, "⚠️ Erro ao processar sua solicitação. Tente novamente ou fale com o time.")
    return PlainTextResponse("ok")

# =============== HEALTHCHECK ===============
@app.get("/health")
def health():
    return {"ok": True, "time_brt": now_brt_str(), "uptime_sec": int(time.time() - _started_at)}
