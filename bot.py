#!/usr/bin/env python3
"""
Bot Universal ToDus S3 — Streaming Edition
- CERO archivos temporales (streaming URL→S3 / TG→S3 directo)
- Memoria RAM para sesión (Evita errores de SQLite en la nube)
"""
import os, sys, uuid, asyncio, time, threading, logging, json, mimetypes, re, gc
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse, unquote, urljoin
from concurrent.futures import ThreadPoolExecutor
from flask import Flask, jsonify
from telethon import TelegramClient, events, Button
from telethon.sessions import MemorySession
from telethon.network import ConnectionTcpFull
import requests
import cloudscraper
import html as html_parser

LOCK_FILE = "/tmp/todus_bot.lock"
def check_lock():
    if os.path.exists(LOCK_FILE):
        try:
            with open(LOCK_FILE, "r") as f: pid = int(f.read().strip())
            os.kill(pid, 0)
            print(f"❌ Bot ya corriendo PID {pid}. Abortando."); sys.exit(1)
        except (OSError, ValueError): pass
    with open(LOCK_FILE, "w") as f: f.write(str(os.getpid()))
check_lock()

def env_or(name, default):
    v = os.environ.get(name, "").strip(); return v if v else default

API_ID     = int(env_or("API_ID", 29752600))
API_HASH   = env_or("API_HASH", "4840ecd4ccc76181dfcb09df79b4a624")
BOT_TOKEN  = env_or("BOT_TOKEN", "")
ADMIN_ID   = int(env_or("ADMIN_ID", "1731031539"))
S3_URL     = env_or("S3_URL", "https://s3.todus.cu/stream")

# Rutas en /tmp para plataformas en la nube
BASE_DIR      = os.path.dirname(os.path.abspath(__file__))
SESSION_FILE  = "/tmp/bot.session" # Ya no se usa, pero por compatibilidad
STATS_FILE    = env_or("STATS_FILE",    "/tmp/stats.json")
USERS_FILE    = env_or("USERS_FILE",    "/tmp/users.json")
PLANS_FILE    = env_or("USER_PLANS_FILE", "/tmp/user_plans.json")
TOKENS_FILE   = env_or("FILE_TOKENS_FILE", "/tmp/file_tokens.json")
AUTODEL_FILE  = env_or("AUTODELETE_FILE",  "/tmp/autodelete.json")

PORT       = int(env_or("PORT", "10000"))
PUBLIC_URL = env_or("PUBLIC_URL", "")

UPLOAD_WORKERS = int(env_or("UPLOAD_WORKERS", "4"))
UPLOAD_RETRIES = int(env_or("UPLOAD_RETRIES", "3"))
UPLOAD_BACKOFF = float(env_or("UPLOAD_BACKOFF", "2"))
UPLOAD_TIMEOUT = int(env_or("UPLOAD_TIMEOUT", "3600"))
FILES_PER_PAGE = int(env_or("FILES_PER_PAGE", "8"))

STREAM_CHUNK = 512 * 1024
QUEUE_SIZE   = 3

scraper = cloudscraper.create_scraper(
    browser={'browser': 'chrome', 'platform': 'windows', 'mobile': False}, delay=0)

session = requests.Session()
adapter = requests.adapters.HTTPAdapter(pool_connections=10, pool_maxsize=10, max_retries=2)
session.mount('http://', adapter); session.mount('https://', adapter)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "*/*", "Accept-Language": "es-ES,es;q=0.8,en;q=0.5",
    "Referer": "https://www.google.com/"
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("puente")

PLANS = {"FREE": 5, "STANDARD": 10, "PREMIUM": 30, "PLUS": 50, "SUPREME": float("inf")}
PLAN_ORDER = ["FREE", "STANDARD", "PREMIUM", "PLUS", "SUPREME"]
PLAN_ICON  = {"FREE": "⭐", "STANDARD": "✨", "PREMIUM": "💎", "PLUS": "👑", "SUPREME": "🔥"}

def load_users():
    try:
        with open(USERS_FILE, "r", encoding="utf-8") as f: return set(json.load(f))
    except (FileNotFoundError, json.JSONDecodeError): return set()
def save_users(users):
    try:
        with open(USERS_FILE, "w", encoding="utf-8") as f: json.dump(sorted(users), f, ensure_ascii=False, indent=2)
    except: pass

authorized_users = load_users()
if ADMIN_ID: authorized_users.add(ADMIN_ID); save_users(authorized_users)

def is_authorized(user_id): return user_id == ADMIN_ID or user_id in authorized_users

def load_user_plans():
    try:
        with open(PLANS_FILE, "r", encoding="utf-8") as f: return json.load(f)
    except: return {}
def save_user_plans():
    try:
        with open(PLANS_FILE, "w", encoding="utf-8") as f: json.dump(user_plans, f, ensure_ascii=False, indent=2)
    except: pass
user_plans = load_user_plans()

def ensure_user_plan(user_id):
    uid = str(user_id)
    if uid not in user_plans:
        user_plans[uid] = {"plan": "SUPREME" if user_id == ADMIN_ID else "FREE", "used": 0, "custom_limit": None}
        save_user_plans()
    else:
        user_plans[uid].setdefault("custom_limit", None)
        user_plans[uid].setdefault("used", 0)
    return user_plans[uid]

def get_user_plan(user_id): return ensure_user_plan(user_id)["plan"]
def get_user_used(user_id): return ensure_user_plan(user_id)["used"]

def get_user_limit(user_id):
    if user_id == ADMIN_ID: return float("inf")
    info = ensure_user_plan(user_id)
    cl = info.get("custom_limit")
    if cl == -1: return float("inf")
    if cl is not None: return int(cl)
    gb = PLANS.get(info.get("plan", "FREE"), 5)
    return float("inf") if gb == float("inf") else int(gb * 1024**3)

def check_quota(user_id, incoming=0):
    if user_id == ADMIN_ID: return True, 0, float("inf"), "SUPREME"
    used = get_user_used(user_id); limit = get_user_limit(user_id)
    if limit == float("inf"): return True, used, limit, get_user_plan(user_id)
    return (used + incoming <= limit), used, limit, get_user_plan(user_id)

def add_usage(user_id, delta):
    info = ensure_user_plan(user_id); info["used"] = max(0, info["used"] + delta); save_user_plans()

def set_plan(user_id, plan):
    info = ensure_user_plan(user_id); info["plan"] = plan; info["custom_limit"] = None; save_user_plans()

DEFAULT_STATS = {"start_time": time.time(), "archivos_subidos": 0, "total_bytes": 0, "ultimo_archivo": None}
def load_stats():
    try:
        with open(STATS_FILE, "r", encoding="utf-8") as f: data = json.load(f)
        for k, v in DEFAULT_STATS.items(): data.setdefault(k, v)
        return data
    except: return dict(DEFAULT_STATS)
def save_stats():
    try:
        with open(STATS_FILE, "w", encoding="utf-8") as f: json.dump(stats, f, ensure_ascii=False, indent=2)
    except: pass
stats = load_stats(); stats["start_time"] = time.time()

def load_tokens():
    try:
        with open(TOKENS_FILE, "r", encoding="utf-8") as f: return json.load(f)
    except: return {}
def save_tokens():
    try:
        with open(TOKENS_FILE, "w", encoding="utf-8") as f: json.dump(file_tokens, f, ensure_ascii=False, indent=2)
    except: pass
file_tokens = load_tokens()

def load_autodelete():
    try:
        with open(AUTODEL_FILE, "r", encoding="utf-8") as f: return json.load(f)
    except: return {"enabled": False, "seconds": 0}
def save_autodelete():
    try:
        with open(AUTODEL_FILE, "w", encoding="utf-8") as f: json.dump(autodelete_config, f, ensure_ascii=False, indent=2)
    except: pass
autodelete_config = load_autodelete()

def parse_duration(s):
    s = s.strip().lower()
    if s in ("off", "0", "none", "disable", "no"): return 0
    m = re.match(r'^(\d+(?:\.\d+)?)\s*([smhd]?)$', s)
    if not m: return None
    val = float(m.group(1)); mult = {'s': 1, 'm': 60, 'h': 3600, 'd': 86400}[m.group(2) or 'm']
    return int(val * mult)

def format_duration(seconds):
    seconds = int(seconds)
    if seconds < 60: return f"{seconds}s"
    if seconds < 3600: return f"{seconds//60}m"
    if seconds < 86400:
        h = seconds // 3600; m = (seconds % 3600) // 60
        return f"{h}h {m}m" if m else f"{h}h"
    d = seconds // 86400; h = (seconds % 86400) // 3600
    return f"{d}d {h}h" if h else f"{d}d"

pending_links = {}; pending_quality = {}; uploaded_cache = {}

try: asyncio.get_running_loop()
except RuntimeError:
    loop = asyncio.new_event_loop(); asyncio.set_event_loop(loop)

# ── AQUÍ ESTÁ LA MAGIA: Usar MemorySession para evitar SQLite ──
bot = TelegramClient(MemorySession(), API_ID, API_HASH,
    connection=ConnectionTcpFull, flood_sleep_threshold=5, timeout=60,
    request_retries=10, connection_retries=10, retry_delay=3)

CUBA_TZ = timezone(timedelta(hours=-5))

def get_greeting():
    hour = datetime.now(CUBA_TZ).hour
    if 5 <= hour < 12: return "🌅 Buenos días"
    if 12 <= hour < 19: return "☀️ Buenas tardes"
    return "🌙 Buenas noches"

def format_size(b):
    if b is None: return "—"
    if isinstance(b, float) and b == float("inf"): return "∞"
    if b < 1024: return f"{b} B"
    if b < 1024 * 1024: return f"{b/1024:.2f} KB"
    if b < 1024 * 1024 * 1024: return f"{b/(1024*1024):.2f} MB"
    return f"{b/(1024*1024*1024):.2f} GB"

def progress_bar(pct, width=14):
    filled = max(0, min(width, round(width * pct / 100)))
    return "▓" * filled + "░" * (width - filled)

def clean_url(url):
    url = url.strip("`'\" \n\r\t")
    m = re.search(r'(https?://[^\s]+)', url)
    return m.group(1).strip("`'\"") if m else url

def get_filename_and_ext(url, headers=None):
    if headers:
        cd = headers.get("content-disposition", "")
        if "filename=" in cd:
            fname = cd.split("filename=")[-1].strip("'\"").split(";")[0].strip()
            if fname: return fname, os.path.splitext(fname)[1] or ".bin"
    path = urlparse(url).path
    name = os.path.basename(unquote(path))
    if not name or name in ("/", ""): name = f"archivo_{uuid.uuid4().hex[:8]}"
    ext = os.path.splitext(name)[1]
    if not ext and headers:
        ctype = headers.get("content-type", "").split(";")[0]
        guessed = mimetypes.guess_extension(ctype) if ctype else None
        if guessed: ext = guessed; name += ext
    return name, ext if ext else ".bin"

def quota_text(user_id):
    used = get_user_used(user_id)
    if user_id == ADMIN_ID:
        return (f"💎 **Tu Cuota**\n━━━━━━━━━━━━━━━━━━━━\n"
                f"👑 Plan: **SUPREME**\n📦 Capacidad: `∞ Ilimitada`\n"
                f"💾 Usado: `{format_size(used)}`\n\n✅ **Sin límites**")
    plan = get_user_plan(user_id); limit = get_user_limit(user_id)
    if limit == float("inf"):
        return (f"💎 **Tu Cuota**\n━━━━━━━━━━━━━━━━━━━━\n"
                f"🔥 Plan: **{plan}**\n📦 Capacidad: `∞ Ilimitada`\n"
                f"💾 Usado: `{format_size(used)}`\n\n✅ **Sin límites**")
    pct = min(100, int(used * 100 / limit)) if limit else 0
    free = max(0, limit - used)
    icon = PLAN_ICON.get(plan, "⭐")
    return (f"💎 **Tu Cuota**\n━━━━━━━━━━━━━━━━━━━━\n"
            f"{icon} Plan: **{plan}**\n📦 Capacidad: `{format_size(limit)}`\n"
            f"💾 Consumido: `{format_size(used)}` ({pct}%)\n"
            f"`[{progress_bar(pct)}]`\n🎯 **Restante: `{format_size(free)}`**")

upload_semaphore = asyncio.Semaphore(UPLOAD_WORKERS)
upload_executor  = ThreadPoolExecutor(max_workers=UPLOAD_WORKERS, thread_name_prefix="up")

async def _stream_url_to_s3(url, s3_url, size, edit_msg, loop):
    def uploader():
        try:
            with session.get(url, headers=HEADERS, stream=True, timeout=60, allow_redirects=True) as r:
                r.raise_for_status()
                total = size or int(r.headers.get("content-length", 0))
                sent = 0; last_pct = -5
                def gen():
                    nonlocal sent, last_pct
                    for chunk in r.iter_content(chunk_size=STREAM_CHUNK):
                        if not chunk: continue
                        sent += len(chunk)
                        if total > 0:
                            pct = int(sent * 100 / total)
                            if pct - last_pct >= 5 or pct == 100:
                                last_pct = pct
                                try:
                                    asyncio.run_coroutine_threadsafe(
                                        edit_msg.edit(f"📤 **SUBIENDO**\n`[{progress_bar(pct)}]` **{pct}%**\n`{format_size(sent)} / {format_size(total)}`"),
                                        loop)
                                except: pass
                        yield chunk
                hdrs = {"Content-Type": "application/octet-stream"}
                if total > 0: hdrs["Content-Length"] = str(total)
                return requests.put(s3_url, data=gen(), headers=hdrs, timeout=UPLOAD_TIMEOUT)
        except Exception as e:
            log.error(f"url_stream: {e}"); return None
    result = await loop.run_in_executor(upload_executor, uploader)
    return result is not None and result.status_code in (200, 201, 204)

async def _stream_tg_to_s3(doc, s3_url, size, edit_msg, loop):
    queue = asyncio.Queue(maxsize=QUEUE_SIZE)
    async def producer():
        sent = 0; last_pct = -5
        try:
            async for chunk in bot.iter_download(doc, chunk_size=STREAM_CHUNK):
                if not chunk: continue
                await queue.put(chunk)
                sent += len(chunk)
                if size > 0:
                    pct = int(sent * 100 / size)
                    if pct - last_pct >= 5 or pct == 100:
                        last_pct = pct
                        try: await edit_msg.edit(f"📤 **SUBIENDO**\n`[{progress_bar(pct)}]` **{pct}%**\n`{format_size(sent)} / {format_size(size)}`")
                        except: pass
        except Exception as e:
            log.error(f"tg producer: {e}"); await queue.put(e); return
        finally: await queue.put(None)
    def uploader():
        def gen():
            while True:
                item = asyncio.run_coroutine_threadsafe(queue.get(), loop).result(timeout=300)
                if item is None: return
                if isinstance(item, Exception): raise item
                yield item
        hdrs = {"Content-Type": "application/octet-stream"}
        if size > 0: hdrs["Content-Length"] = str(size)
        return requests.put(s3_url, data=gen(), headers=hdrs, timeout=UPLOAD_TIMEOUT)
    prod = asyncio.create_task(producer())
    try:
        result = await loop.run_in_executor(upload_executor, uploader)
        await prod
        return result is not None and result.status_code in (200, 201, 204)
    except Exception as e:
        log.error(f"tg uploader: {e}"); prod.cancel()
        try: await prod
        except: pass
        return False

async def upload_url_with_retry(url, filename, size, edit_msg):
    for attempt in range(1, UPLOAD_RETRIES + 1):
        s3_url = f"{S3_URL}/{uuid.uuid4().hex[:8]}_{filename}"
        if attempt > 1:
            wait = UPLOAD_BACKOFF ** (attempt - 1)
            try: await edit_msg.edit(f"🔄 **Reintentando ({attempt}/{UPLOAD_RETRIES})**\n⏱ `{wait:.0f}s...`")
            except: pass
            await asyncio.sleep(wait)
        async with upload_semaphore:
            loop = asyncio.get_running_loop()
            if await _stream_url_to_s3(url, s3_url, size, edit_msg, loop): return s3_url
    return None

async def upload_tg_with_retry(doc, filename, size, edit_msg):
    for attempt in range(1, UPLOAD_RETRIES + 1):
        s3_url = f"{S3_URL}/{uuid.uuid4().hex[:8]}_{filename}"
        if attempt > 1:
            wait = UPLOAD_BACKOFF ** (attempt - 1)
            try: await edit_msg.edit(f"🔄 **Reintentando ({attempt}/{UPLOAD_RETRIES})**\n⏱ `{wait:.0f}s...`")
            except: pass
            await asyncio.sleep(wait)
        async with upload_semaphore:
            loop = asyncio.get_running_loop()
            if await _stream_tg_to_s3(doc, s3_url, size, edit_msg, loop): return s3_url
    return None

def extract_real_url(html_content, base_url):
    decoded = html_parser.unescape(html_content)
    for link in re.findall(r'(?:href|src)=["\']([^"\']+)["\']', decoded):
        if any(k in link.lower() for k in ('download', 'descargar', 'stream', 'hash=', '.mp4', '.mkv', '.zip')):
            return urljoin(base_url, link)
    direct = re.findall(r'(https?://[^\s"\'<>]+(?:\.mp4|\.mkv|\.zip|\.rar|\.pdf|\.apk|\?hash=[^\s"\'<>]+))', decoded)
    return direct[0].replace("\\", "") if direct else None

async def procesar_url(event, url, user_id):
    status_msg = await event.reply("⚡ **Analizando enlace...**")
    loop = asyncio.get_running_loop()
    def probe(u):
        try:
            r = session.get(u, headers=HEADERS, stream=True, timeout=30, allow_redirects=True)
            if r.status_code in (403, 503):
                r.close(); r = scraper.get(u, headers=HEADERS, stream=True, timeout=60, allow_redirects=True)
            return r
        except Exception as e: log.error(f"probe: {e}"); return None
    r_probe = await loop.run_in_executor(None, probe, url)
    if r_probe is None or r_probe.status_code != 200:
        code = getattr(r_probe, "status_code", "?")
        try: await status_msg.edit(f"❌ **Error HTTP {code}**\nEl enlace expiró o está bloqueado.")
        except: pass
        if r_probe: r_probe.close()
        return
    content_type = r_probe.headers.get("content-type", "").lower()
    if "text/html" in content_type:
        try: await status_msg.edit("🔍 **Buscando archivo real...**")
        except: pass
        try: html_text = r_probe.text
        except: html_text = ""
        r_probe.close()
        real_url = extract_real_url(html_text, url)
        if not real_url:
            try: await status_msg.edit("❌ **No se encontró el archivo original.**")
            except: pass
            return
        url = real_url
        r_probe = await loop.run_in_executor(None, probe, url)
        if r_probe is None or r_probe.status_code != 200:
            try: await status_msg.edit("❌ **Error al acceder al archivo real.**")
            except: pass
            if r_probe: r_probe.close()
            return
    total_size = int(r_probe.headers.get("content-length", 0))
    final_url = r_probe.url
    filename, ext = get_filename_and_ext(final_url, r_probe.headers)
    r_probe.close()
    if total_size > 0:
        ok, used, limit, plan = check_quota(user_id, total_size)
        if not ok:
            try: await status_msg.edit(f"🚫 **Límite excedido**\n{PLAN_ICON.get(plan,'⭐')} {plan} ({format_size(limit)})\n💾 Usado: {format_size(used)}\n📦 Archivo: {format_size(total_size)}")
            except: pass
            return
    s3_url = await upload_url_with_retry(final_url, filename.replace(" ", "_"), total_size, status_msg)
    if s3_url:
        size = total_size
        stats["archivos_subidos"] += 1; stats["total_bytes"] += size
        stats["ultimo_archivo"] = filename; save_stats()
        add_usage(user_id, size)
        try: await status_msg.delete()
        except: pass
        await enviar_mensaje_final(event, user_id, filename, size, s3_url)
    else:
        try: await status_msg.edit("❌ **Error subiendo al S3.**")
        except: pass

async def procesar_archivo_tg(event, doc, filename, user_id):
    size = getattr(doc, "size", 0) or 0
    if size > 0:
        ok, used, limit, plan = check_quota(user_id, size)
        if not ok:
            return await event.reply(f"🚫 **Límite excedido**\n{PLAN_ICON.get(plan,'⭐')} {plan} ({format_size(limit)})\n💾 Usado: {format_size(used)}\n📦 Archivo: {format_size(size)}")
    status_msg = await event.reply("📥 **Iniciando...**")
    s3_url = await upload_tg_with_retry(doc, filename.replace(" ", "_"), size, status_msg)
    if s3_url:
        stats["archivos_subidos"] += 1; stats["total_bytes"] += size
        stats["ultimo_archivo"] = filename; save_stats()
        add_usage(user_id, size)
        try: await status_msg.delete()
        except: pass
        await enviar_mensaje_final(event, user_id, filename, size, s3_url)
    else:
        try: await status_msg.edit("❌ **Error subiendo al S3.**")
        except: pass

async def enviar_mensaje_final(event, user_id, original_name, size, s3_url):
    token = uuid.uuid4().hex[:12]
    expires_at = time.time() + autodelete_config["seconds"] if (autodelete_config["enabled"] and autodelete_config["seconds"] > 0) else None
    file_tokens[token] = {"url": s3_url, "name": original_name, "size": size,
                          "user_id": user_id, "time": time.time(), "expires_at": expires_at}
    save_tokens(); uploaded_cache[user_id] = {"name": original_name, "url": s3_url, "size": size}
    expiry_line = f"\n⏰ Auto-elimina: `{format_duration(autodelete_config['seconds'])}`" if expires_at else ""
    texto = f"✅ **¡Subido con éxito!**\n\n📄 `{original_name}`\n📦 `{format_size(size)}`{expiry_line}\n\n🔗 **URL de descarga:**\n{s3_url}"
    botones = [
        [Button.inline("📋 Copiar URL", f"cp_{token}".encode())],
        [Button.inline("🗑 Eliminar archivo", f"del_{token}".encode())],
        [Button.inline("📂 Mis Archivos", b"panel_files"), Button.inline("🏠 Menú", b"panel_back")],
    ]
    await event.reply(texto, buttons=botones, link_preview=False)

async def autodelete_loop():
    while True:
        try:
            await asyncio.sleep(60); now = time.time()
            to_del = [(t, i) for t, i in list(file_tokens.items()) if i.get("expires_at") and now >= i["expires_at"]]
            if not to_del: continue
            loop = asyncio.get_running_loop()
            for token, info in to_del:
                try: await loop.run_in_executor(None, lambda u=info["url"]: requests.delete(u, timeout=30))
                except Exception as e: log.error(f"autodelete: {e}")
                add_usage(info["user_id"], -info["size"]); file_tokens.pop(token, None)
                try: await bot.send_message(info["user_id"], f"⏰ **Archivo auto-eliminado**\n\n📄 `{info['name']}`\n🎯 Liberado: `{format_size(info['size'])}`")
                except: pass
            save_tokens(); log.info(f"⏰ Auto-eliminados: {len(to_del)}")
        except asyncio.CancelledError: break
        except Exception as e: log.error(f"autodelete_loop: {e}")

def _user_files(user_id):
    now = time.time()
    items = [(t, i) for t, i in file_tokens.items() if i.get("user_id") == user_id and (not i.get("expires_at") or i["expires_at"] > now)]
    items.sort(key=lambda x: x[1].get("time", 0), reverse=True)
    return items

async def show_files_page(event, user_id, page=0):
    files = _user_files(user_id); total = len(files)
    if total == 0:
        return await event.edit(f"📂 **Mis Archivos**\n━━━━━━━━━━━━━━━━━━━━\n_No tienes archivos subidos._",
            buttons=[[Button.inline("◀️ Volver", b"panel_back")]])
    pages = (total + FILES_PER_PAGE - 1) // FILES_PER_PAGE
    page = max(0, min(page, pages - 1))
    start = page * FILES_PER_PAGE; end = min(start + FILES_PER_PAGE, total)
    buttons = []
    for token, info in files[start:end]:
        name = info["name"]
        if len(name) > 32: name = name[:29] + "..."
        icon = "⏰" if info.get("expires_at") else "📄"
        label = f"{icon} {name} ({format_size(info['size'])})"
        if len(label) > 60: label = label[:57] + "..."
        buttons.append([Button.inline(label, f"fview_{token}".encode())])
    if pages > 1:
        nav = []
        if page > 0: nav.append(Button.inline("◀️", f"fpg_{page-1}".encode()))
        nav.append(Button.inline(f"{page+1}/{pages}", b"noop"))
        if page < pages - 1: nav.append(Button.inline("▶️", f"fpg_{page+1}".encode()))
        buttons.append(nav)
    buttons.append([Button.inline("◀️ Volver", b"panel_back")])
    await event.edit(f"📂 **Mis Archivos** ({total})\n━━━━━━━━━━━━━━━━━━━━\nSelecciona uno:", buttons=buttons)

async def show_file_view(event, user_id, token):
    info = file_tokens.get(token)
    if not info or info.get("user_id") != user_id:
        return await event.answer("⚠️ Archivo no disponible", alert=True)
    time_ago = format_duration(time.time() - info.get("time", time.time()))
    if info.get("expires_at"):
        remaining = info["expires_at"] - time.time()
        expiry_str = f"en {format_duration(remaining)}" if remaining > 0 else "Expirado"
    else: expiry_str = "Nunca"
    await event.edit(
        f"📄 **{info['name']}**\n━━━━━━━━━━━━━━━━━━━━\n"
        f"📦 Tamaño: `{format_size(info['size'])}`\n📅 Subido: `hace {time_ago}`\n⏰ Auto-elimina: `{expiry_str}`\n\n"
        f"🔗 **URL:**\n{info['url']}",
        buttons=[
            [Button.inline("📋 Copiar URL", f"cp_{token}".encode())],
            [Button.inline("🗑 Eliminar archivo", f"fdel_{token}".encode())],
            [Button.inline("◀️ Volver", b"panel_files")]
        ], link_preview=False)

def panel_buttons(is_admin=False):
    rows = [
        [Button.inline("📂 Mis Archivos", b"panel_files"), Button.inline("💾 Mi Cuota", b"panel_quota")],
        [Button.inline("📊 Estadísticas", b"panel_stats")],
        [Button.inline("🔗 Subir Enlace", b"panel_link")],
    ]
    if is_admin: rows.append([Button.inline("👥 Gestionar Usuarios", b"panel_users")])
    rows.append([Button.inline("❓ Ayuda", b"panel_help")])
    return rows

def panel_main_text(user_id, name):
    greeting = get_greeting()
    plan = get_user_plan(user_id)
    icon = PLAN_ICON.get(plan, "⭐")
    if user_id == ADMIN_ID: icon = "👑"; plan = "ADMIN"
    return (f"{greeting}, **{name}**! 👋\n━━━━━━━━━━━━━━━━━━━━\n"
            f"{icon} Plan: **{plan}**\n\n{quota_text(user_id)}\n━━━━━━━━━━━━━━━━━━━━\n"
            f"✨ Envíame archivos o enlaces y los subo a la nube.")

@bot.on(events.NewMessage(pattern="/start"))
async def cmd_start(event):
    user_id = event.sender_id
    if not is_authorized(user_id):
        return await event.reply(f"⛔ **Acceso Denegado**\n\nTu ID: `{user_id}`")
    ensure_user_plan(user_id)
    await event.reply(panel_main_text(user_id, event.sender.first_name or "amigo"),
        buttons=panel_buttons(is_admin=(user_id == ADMIN_ID)))

@bot.on(events.CallbackQuery)
async def callback_handler(event):
    user_id = event.sender_id; data = event.data
    if not is_authorized(user_id): return await event.answer("⛔ No autorizado", alert=True)
    if data.startswith(b"cp_"):
        token = data[3:].decode(); info = file_tokens.get(token)
        if not info or info.get("user_id") != user_id: return await event.answer("⚠️ Enlace expirado", alert=True)
        await event.answer("✅ URL lista")
        try:
            eph = await event.respond(f"👇 **Toca para copiar:**\n`{info['url']}`", link_preview=False)
            async def _del():
                await asyncio.sleep(5)
                try: await eph.delete()
                except: pass
            asyncio.create_task(_del())
        except: pass
    elif data.startswith(b"del_"):
        token = data[4:].decode(); info = file_tokens.get(token)
        if not info or info.get("user_id") != user_id: return await event.answer("⚠️ Enlace expirado", alert=True)
        await event.answer("🗑 Eliminando...")
        loop = asyncio.get_running_loop()
        try: r = await loop.run_in_executor(None, lambda: requests.delete(info["url"], timeout=60))
        except: r = None
        if r is not None and r.status_code in (200, 204, 404):
            add_usage(user_id, -info["size"]); file_tokens.pop(token, None); save_tokens()
            try: await event.edit(f"🗑 **Archivo eliminado**\n\n`{info['name']}`\n\n🎯 Liberado: **{format_size(info['size'])}**\n\n{quota_text(user_id)}",
                buttons=[[Button.inline("📂 Mis Archivos", b"panel_files")], [Button.inline("◀️ Menú", b"panel_back")]])
            except: pass
        else:
            try: await event.edit("❌ **Error al eliminar**")
            except: pass
    elif data == b"panel_files": await show_files_page(event, user_id, 0); await event.answer()
    elif data.startswith(b"fpg_"): await show_files_page(event, user_id, int(data[4:].decode())); await event.answer()
    elif data.startswith(b"fview_"): await show_file_view(event, user_id, data[6:].decode()); await event.answer()
    elif data.startswith(b"fdel_"):
        token = data[5:].decode(); info = file_tokens.get(token)
        if not info or info.get("user_id") != user_id: return await event.answer("⚠️ Archivo no disponible", alert=True)
        await event.answer("🗑 Eliminando...")
        loop = asyncio.get_running_loop()
        try: r = await loop.run_in_executor(None, lambda: requests.delete(info["url"], timeout=60))
        except: r = None
        if r is not None and r.status_code in (200, 204, 404):
            add_usage(user_id, -info["size"]); file_tokens.pop(token, None); save_tokens()
            await event.edit(f"🗑 **Archivo eliminado**\n\n📄 `{info['name']}`\n🎯 Liberado: **{format_size(info['size'])}**\n\n{quota_text(user_id)}",
                buttons=[[Button.inline("📂 Volver", b"panel_files")], [Button.inline("◀️ Menú", b"panel_back")]])
        else: await event.answer("❌ Error al eliminar", alert=True)
    elif data == b"noop": await event.answer()
    elif data == b"panel_quota": await event.edit(quota_text(user_id), buttons=[[Button.inline("◀️ Volver", b"panel_back")]]); await event.answer()
    elif data == b"panel_stats":
        uptime = int(time.time() - stats["start_time"]); h, m = divmod(uptime, 3600); m, s = divmod(m, 60)
        n_files = len(_user_files(user_id))
        await event.edit(f"📊 **Tus Estadísticas**\n━━━━━━━━━━━━━━━━━━━━\n{quota_text(user_id)}\n━━━━━━━━━━━━━━━━━━━━\n"
            f"📂 **Tus Archivos:** `{n_files}`\n⏱ **Uptime:** `{h}h {m}m {s}s`\n"
            f"📈 **Global:** `{stats['archivos_subidos']}`\n💾 **Transferido:** `{format_size(stats['total_bytes'])}`\n"
            f"📄 **Último:** `{stats['ultimo_archivo'] or '—'}`",
            buttons=[[Button.inline("◀️ Volver", b"panel_back")]])
        await event.answer()
    elif data == b"panel_users":
        if user_id != ADMIN_ID: return await event.answer("⛔ Solo admin", alert=True)
        lines = [f"👥 **Usuarios** ({len(authorized_users)})\n━━━━━━━━━━━━━━━━━━━━"]
        for uid in sorted(authorized_users):
            info = ensure_user_plan(uid); plan = info.get("plan", "FREE")
            used = info.get("used", 0); limit = get_user_limit(uid)
            icon = "👑" if uid == ADMIN_ID else PLAN_ICON.get(plan, "⭐")
            limit_str = "∞" if limit == float("inf") else format_size(limit)
            pct_str = ""
            if limit != float("inf") and limit > 0:
                pct_str = f" ({min(100, int(used*100/limit))}%)"
            lines.append(f"\n{icon} `{uid}` — **{plan}**\n   💾 `{format_size(used)} / {limit_str}`{pct_str}")
        await event.edit("\n".join(lines), buttons=[[Button.inline("◀️ Volver", b"panel_back")]])
        await event.answer()
    elif data == b"panel_help":
        await event.edit(f"❓ **¿Cómo usar el bot?**\n━━━━━━━━━━━━━━━━━━━━\n"
            f"1️⃣ Envía archivos (video/doc/audio)\n2️⃣ O envía un enlace\n3️⃣ El bot sube a la nube y te da la URL\n\n"
            f"**Comandos:**\n`/start` — Menú\n`/myplan` — Tu cuota\n`/id` — Tu ID\n\n"
            f"**Admin:**\n`/add <id>` · `/remove <id>` · `/plan <id> <PLAN>`\n"
            f"`/setlimit <id> <GB|unlimited|reset>` · `/reset <id>`\n`/users` · `/autodelete <tiempo>`",
            buttons=[[Button.inline("◀️ Volver", b"panel_back")]])
        await event.answer()
    elif data == b"panel_link":
        await event.edit("🔗 **Subir un Enlace**\n\nEnvíame el enlace como mensaje.",
            buttons=[[Button.inline("❌ Cancelar", b"panel_back")]])
        await event.answer(); pending_links[user_id] = {"awaiting": True}
    elif data == b"panel_back":
        name = event.sender.first_name or "amigo"
        await event.edit(panel_main_text(user_id, name), buttons=panel_buttons(is_admin=(user_id == ADMIN_ID)))
        await event.answer(); pending_links.pop(user_id, None)
    elif data.startswith(b"quality_") and user_id in pending_quality:
        idx = int(data.split(b"_")[1]); info = pending_quality.pop(user_id)
        label, doc = info["options"][idx]
        await event.answer(f"✅ {label}")
        try: await event.delete()
        except: pass
        await procesar_archivo_tg(event, doc, info["filename"], user_id)
    elif data == b"cancel":
        pending_quality.pop(user_id, None); await event.answer("❌ Cancelado")
        try: await event.delete()
        except: pass

@bot.on(events.NewMessage(pattern="/add"))
async def cmd_add(event):
    if event.sender_id != ADMIN_ID: return
    try:
        uid = int(event.text.split()[1])
        authorized_users.add(uid); save_users(authorized_users); ensure_user_plan(uid)
        await event.reply(f"✅ Usuario `{uid}` autorizado.")
    except: await event.reply("Uso: `/add <user_id>`")

@bot.on(events.NewMessage(pattern="/remove"))
async def cmd_remove(event):
    if event.sender_id != ADMIN_ID: return
    try:
        uid = int(event.text.split()[1])
        authorized_users.discard(uid); save_users(authorized_users)
        await event.reply(f"✅ Usuario `{uid}` revocado.")
    except: await event.reply("Uso: `/remove <user_id>`")

@bot.on(events.NewMessage(pattern="/plan"))
async def cmd_plan(event):
    if event.sender_id != ADMIN_ID: return await event.reply("⛔ Solo admin.")
    parts = event.text.split()
    if len(parts) < 3: return await event.reply("Uso: `/plan <user_id> <PLAN>`")
    try: uid = int(parts[1]); plan = parts[2].upper()
    except: return await event.reply("❌ ID inválido.")
    if plan not in PLANS: return await event.reply(f"❌ Plan inválido.\nOpciones: {', '.join(PLAN_ORDER)}")
    ensure_user_plan(uid); set_plan(uid, plan)
    await event.reply(f"✅ Usuario `{uid}` → **{plan}**")

@bot.on(events.NewMessage(pattern="/setlimit"))
async def cmd_setlimit(event):
    if event.sender_id != ADMIN_ID: return
    parts = event.text.split()
    if len(parts) < 3:
        return await event.reply("Uso:\n`/setlimit <user_id> <GB>`\n`/setlimit <user_id> unlimited`\n`/setlimit <user_id> reset`")
    try: uid = int(parts[1])
    except: return await event.reply("❌ ID inválido.")
    val = parts[2].lower(); info = ensure_user_plan(uid)
    if val == "reset": info["custom_limit"] = None
    elif val in ("unlimited", "inf", "∞", "0"): info["custom_limit"] = -1
    else:
        try: gb = float(val); info["custom_limit"] = int(gb * 1024**3)
        except: return await event.reply("❌ Valor inválido.")
    save_user_plans()
    await event.reply(f"✅ Límite de `{uid}` → **{format_size(get_user_limit(uid))}**")

@bot.on(events.NewMessage(pattern="/reset"))
async def cmd_reset(event):
    if event.sender_id != ADMIN_ID: return
    try:
        uid = int(event.text.split()[1])
        info = ensure_user_plan(uid); info["used"] = 0; save_user_plans()
        await event.reply(f"✅ Uso reseteado para `{uid}`.")
    except: await event.reply("Uso: `/reset <user_id>`")

@bot.on(events.NewMessage(pattern="/users"))
async def cmd_users(event):
    if event.sender_id != ADMIN_ID: return
    lines = [f"👥 **Usuarios** ({len(authorized_users)})\n━━━━━━━━━━━━━━━━━━━━"]
    for uid in sorted(authorized_users):
        info = ensure_user_plan(uid); plan = info.get("plan", "FREE")
        used = info.get("used", 0); limit = get_user_limit(uid)
        icon = "👑" if uid == ADMIN_ID else PLAN_ICON.get(plan, "⭐")
        limit_str = "∞" if limit == float("inf") else format_size(limit)
        pct_str = ""
        if limit != float("inf") and limit > 0:
            pct_str = f" ({min(100, int(used*100/limit))}%)"
        lines.append(f"\n{icon} `{uid}` — **{plan}**\n   💾 `{format_size(used)} / {limit_str}`{pct_str}")
    await event.reply("\n".join(lines))

@bot.on(events.NewMessage(pattern="/autodelete"))
async def cmd_autodelete(event):
    if event.sender_id != ADMIN_ID: return
    parts = event.text.split(maxsplit=1)
    if len(parts) < 2: return await event.reply("Uso: `/autodelete <tiempo>` o `/autodelete off`")
    if parts[1].lower() == "off":
        autodelete_config["enabled"] = False; autodelete_config["seconds"] = 0; save_autodelete()
        return await event.reply("✅ Auto-eliminación desactivada.")
    seconds = parse_duration(parts[1])
    if seconds is None: return await event.reply("❌ Formato inválido.")
    autodelete_config["enabled"] = True; autodelete_config["seconds"] = seconds; save_autodelete()
    await event.reply(f"✅ Auto-eliminación: `{format_duration(seconds)}`")

@bot.on(events.NewMessage(pattern="/myplan"))
async def cmd_myplan(event):
    if not is_authorized(event.sender_id): return
    await event.reply(quota_text(event.sender_id))

@bot.on(events.NewMessage(pattern="/id"))
async def cmd_id(event): await event.reply(f"🆔 Tu ID: `{event.sender_id}`")

@bot.on(events.NewMessage)
async def handler(event):
    msg = event.message
    if not msg or (msg.text and msg.text.startswith("/")): return
    user_id = event.sender_id
    if not is_authorized(user_id):
        return await event.reply(f"⛔ **No autorizado.**\nTu ID: `{user_id}`")
    ensure_user_plan(user_id)
    if msg.video:
        doc = msg.video; options = [("ORIGINAL", doc)]
        alt_docs = getattr(msg.media, "alt_documents", None)
        if alt_docs:
            for alt in alt_docs:
                w = next((a.w for a in alt.attributes if hasattr(a, "w")), None)
                options.append((f"COMPRIMIDO {w}p" if w else "COMPRIMIDO", alt))
        if len(options) == 1:
            return await procesar_archivo_tg(event, doc, f"video_{doc.id}.mp4", user_id)
        buttons = [[Button.inline(f"{l} ({format_size(d.size)})", f"quality_{i}".encode())] for i, (l, d) in enumerate(options)]
        buttons.append([Button.inline("❌ Cancelar", b"cancel")])
        pending_quality[user_id] = {"options": options, "filename": f"video_{doc.id}.mp4"}
        return await event.reply(f"🎬 **Selecciona calidad**\n\n" + "\n".join(f"• {l}: {format_size(d.size)}" for l, d in options), buttons=buttons)
    if msg.document:
        name = next((a.file_name for a in msg.document.attributes if hasattr(a, "file_name")), f"doc_{msg.document.id}.bin")
        mime = getattr(msg.document, "mime_type", "") or ""
        if mime in ("image/jpeg", "image/png", "image/gif", "image/webp", "text/html") or name.lower().endswith((".jpg", ".jpeg", ".png", ".gif", ".webp", ".html", ".htm")):
            return await event.reply("⚠️ **Archivos de imagen o HTML no permitidos.**")
        return await procesar_archivo_tg(event, msg.document, name, user_id)
    if msg.audio: return await procesar_archivo_tg(event, msg.audio, f"audio_{msg.audio.id}.mp3", user_id)
    if msg.photo: return await event.reply("⚠️ **Imágenes no soportadas.**")
    if msg.gif: return await event.reply("⚠️ **GIFs no soportados.**")
    if msg.text:
        text = msg.text.strip()
        if user_id in pending_links:
            pending_links.pop(user_id, None)
            return await procesar_url(event, clean_url(text), user_id)
        if "http://" in text or "https://" in text:
            return await procesar_url(event, clean_url(text), user_id)
        return await event.reply("💡 Envíame un archivo o enlace.")
    return await event.reply("💡 Tipo no soportado.")

flask_app = Flask(__name__)
@flask_app.route("/health")
def health(): return jsonify({"status": "healthy", "streaming": True})

def run_flask(): flask_app.run(host="0.0.0.0", port=PORT, use_reloader=False, threaded=True)

async def main():
    await bot.start(bot_token=BOT_TOKEN)
    log.info(f"✅ BOT READY: @{(await bot.get_me()).username}")
    ad_task = asyncio.create_task(autodelete_loop())
    threading.Thread(target=run_flask, daemon=True).start()
    log.info(f"🌐 Web :{PORT} | ⚡ Workers: {UPLOAD_WORKERS} | 💾 Streaming: ON | 🧠 Chunk: {STREAM_CHUNK//1024}KB")
    try:
        await bot.run_until_disconnected()
    finally:
        ad_task.cancel()
        try: await ad_task
        except asyncio.CancelledError: pass
        upload_executor.shutdown(wait=False)
        await bot.disconnect()
        await asyncio.sleep(0.5)
        try:
            if os.path.exists(LOCK_FILE): os.remove(LOCK_FILE)
        except: pass

if __name__ == "__main__":
    try: loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop(); asyncio.set_event_loop(loop)
    try: loop.run_until_complete(main())
    except KeyboardInterrupt: log.info("Detenido por usuario.")
    except Exception as e: log.critical(f"Fatal: {e}")
    finally:
        try:
            pending = asyncio.all_tasks(loop)
            for t in pending: t.cancel()
            loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
        except: pass
        finally: loop.close()