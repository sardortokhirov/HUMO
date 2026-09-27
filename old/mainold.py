# telegram_api.py
import os
import re
import asyncio
import logging
from typing import Dict

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.errors import SessionPasswordNeededError, PhoneNumberInvalidError, FloodWaitError
from telethon.tl.functions.account import GetPasswordRequest

# ---------------- CONFIG ----------------
API_ID = 22962676
API_HASH = "543e9a4d695fe8c6aa4075c9525f7c57"
BOT_USERNAME = "@HUMOcardbot"
SESSIONS_DIR = "sessions"   # will contain only .sessionstr files from StringSession
os.makedirs(SESSIONS_DIR, exist_ok=True)

# ---------------- LOGGING ----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("humo-monitor")

# ---------------- APP & GLOBALS ----------------
app = FastAPI(title="Telegram HUMOcard Monitor (robust)", version="4.0")

# global lock for disk write operations
disk_lock = asyncio.Lock()
# per-phone locks to serialize operations per account
per_phone_locks: Dict[str, asyncio.Lock] = {}
# temporary login sessions (phone -> {"client": TelegramClient, "code_hash": ...})
sessions: Dict[str, Dict] = {}
# active monitoring clients (phone -> {"client": TelegramClient, "handler_added": bool})
active_clients: Dict[str, Dict] = {}

# ---------------- MODELS ----------------
class NewNumberRequest(BaseModel):
    phone: str

class SmsCodeRequest(BaseModel):
    phone: str
    code: str

class TwoStepRequest(BaseModel):
    phone: str
    password: str

# ---------------- HELPERS ----------------
def normalize_phone(phone: str) -> str:
    """Normalize phone to +[digits] (keep only digits and leading +)."""
    phone = phone.strip()
    if not phone:
        return phone
    # keep + and digits
    s = re.sub(r"[^\d+]", "", phone)
    if not s.startswith("+"):
        s = "+" + s
    return s

def phone_file_name(phone: str) -> str:
    """Safe filename for a phone (digits only)."""
    digits = re.sub(r"\D", "", phone)
    return os.path.join(SESSIONS_DIR, f"{digits}.sessionstr")

def get_per_phone_lock(phone: str) -> asyncio.Lock:
    if phone not in per_phone_locks:
        per_phone_locks[phone] = asyncio.Lock()
    return per_phone_locks[phone]

async def save_session_str(phone: str, session_str: str):
    path = phone_file_name(phone)
    async with disk_lock:
        with open(path, "w", encoding="utf-8") as f:
            f.write(session_str)

async def add_monitor_handler(client: TelegramClient, phone: str):
    """Add handler once per-phone."""
    meta = active_clients.get(phone)
    if meta and meta.get("handler_added"):
        return

    async def _on_new_message(event):
        # Telethon event.message may have .message or .text
        text = getattr(event.message, "message", None) or getattr(event.message, "text", None) or ""
        logger.info(f"[{phone}] Botdan xabar: {text}")

    # use add_event_handler to avoid closure issues
    client.add_event_handler(_on_new_message, events.NewMessage(from_users=BOT_USERNAME))
    active_clients.setdefault(phone, {})["handler_added"] = True
    logger.info(f"[{phone}] Monitoring handler qo'shildi.")

async def retry_on_sqlite_locked(fn, *args, retries=5, base_delay=0.5, **kwargs):
    """
    Retry wrapper for operations that might raise sqlite 'database is locked' or sqlite.OperationalError.
    """
    import sqlite3
    last_exc = None
    for attempt in range(1, retries + 1):
        try:
            return await fn(*args, **kwargs)
        except Exception as e:
            last_exc = e
            # check message contains 'database is locked' or exception is sqlite3.OperationalError
            msg = str(e).lower()
            if isinstance(e, sqlite3.OperationalError) or "database is locked" in msg or "sqlite" in msg:
                delay = base_delay * attempt
                logger.warning(f"Retry attempt {attempt}/{retries} after sqlite lock (sleep {delay}s). Error: {e}")
                await asyncio.sleep(delay)
                continue
            # otherwise re-raise immediately
            raise
    # if all retries failed, raise last exception
    raise last_exc

# ---------------- ROUTES ----------------
@app.post("/newNumber")
async def new_number(req: NewNumberRequest):
    phone = normalize_phone(req.phone)
    if not phone:
        raise HTTPException(status_code=400, detail="Telefon noto'g'ri.")

    # if already in progress or active, return early
    if phone in sessions:
        return {"message": "Login jarayoni allaqachon boshlangan."}
    if phone in active_clients:
        return {"message": "Bu hisob allaqachon monitoringda."}

    lock = get_per_phone_lock(phone)
    async with lock:
        # final check inside lock
        if phone in sessions or phone in active_clients:
            return {"message": "Jarayon yoki monitoring allaqachon mavjud (post-lock)."}

        # create StringSession client (no sqlite file)
        client = TelegramClient(StringSession(), API_ID, API_HASH)
        try:
            # connect with retry wrapper (in case of weird sqlite in internals)
            async def _connect():
                await client.connect()
                # send_code_request returns object with phone_code_hash
                return await client.send_code_request(phone)

            sent = await retry_on_sqlite_locked(_connect)
            sessions[phone] = {"client": client, "code_hash": sent.phone_code_hash}
            logger.info(f"Hisob {phone} uchun SMS kod so'raldi.")
            return {"message": "SMS kod yuborildi", "next": "POST /smscode"}
        except PhoneNumberInvalidError:
            try: await client.disconnect()
            except: pass
            raise HTTPException(status_code=400, detail="Noto'g'ri telefon raqami.")
        except FloodWaitError as e:
            try: await client.disconnect()
            except: pass
            raise HTTPException(status_code=429, detail=f"FloodWait: {e.seconds} soniya kuting.")
        except Exception as e:
            try: await client.disconnect()
            except: pass
            logger.exception("newNumber xato")
            raise HTTPException(status_code=500, detail=str(e))

@app.post("/smscode")
async def smscode(req: SmsCodeRequest):
    phone = normalize_phone(req.phone)
    if phone not in sessions:
        raise HTTPException(status_code=400, detail="Avval /newNumber chaqiring.")

    lock = get_per_phone_lock(phone)
    async with lock:
        data = sessions.get(phone)
        if not data:
            raise HTTPException(status_code=400, detail="Login sessiyasi topilmadi (post-lock).")

        client: TelegramClient = data["client"]
        code_hash = data["code_hash"]

        try:
            # sign_in with retry wrapper
            async def _sign():
                await client.sign_in(phone, req.code, phone_code_hash=code_hash)

            await retry_on_sqlite_locked(_sign)

            if not await client.is_user_authorized():
                # If it wasn't authorized but didn't raise SessionPasswordNeededError, explicit error
                raise HTTPException(status_code=500, detail="Avtorizatsiya yakunlanmadi.")

            # Save session string to disk (only here)
            session_str = str(client.session)
            await save_session_str(phone, session_str)

            # Move to active clients and add handler
            sessions.pop(phone, None)
            active_clients[phone] = {"client": client, "handler_added": False}
            await add_monitor_handler(client, phone)

            logger.info(f"{phone} monitoringga qo'yildi (smscode).")
            return {"message": f"{phone} hisobiga kirildi. Monitoring boshlandi."}

        except SessionPasswordNeededError:
            # fetch hint (may also raise; catch gracefully)
            try:
                pwd = await client(GetPasswordRequest())
                hint = getattr(pwd, "hint", "") or ""
            except Exception:
                hint = ""
            return {"message": "Ikkilamchi parol kerak", "hint": hint, "next": "POST /twostep"}
        except Exception as e:
            logger.exception(f"smscode xato for {phone}")
            # cleanup
            try: await client.disconnect()
            except: pass
            sessions.pop(phone, None)
            raise HTTPException(status_code=500, detail=f"Sessiya yoki sign_in xatosi: {e}")

@app.post("/twostep")
async def twostep(req: TwoStepRequest):
    phone = normalize_phone(req.phone)
    if phone not in sessions:
        raise HTTPException(status_code=400, detail="Avval /newNumber va /smscode chaqiring.")

    lock = get_per_phone_lock(phone)
    async with lock:
        data = sessions.get(phone)
        if not data:
            raise HTTPException(status_code=400, detail="Login sessiyasi topilmadi (post-lock).")

        client: TelegramClient = data["client"]
        try:
            async def _sign_pw():
                await client.sign_in(password=req.password)
            await retry_on_sqlite_locked(_sign_pw)

            if not await client.is_user_authorized():
                raise HTTPException(status_code=500, detail="Ikkilamchi parol bilan avtorizatsiya yakunlanmadi.")

            session_str = str(client.session)
            await save_session_str(phone, session_str)

            sessions.pop(phone, None)
            active_clients[phone] = {"client": client, "handler_added": False}
            await add_monitor_handler(client, phone)

            logger.info(f"{phone} ikkilamchi parol bilan monitoringga qo'yildi.")
            return {"message": f"{phone} hisobiga ikkilamchi parol bilan kirildi. Monitoring boshlandi."}
        except Exception as e:
            logger.exception(f"twostep xato for {phone}")
            try: await client.disconnect()
            except: pass
            sessions.pop(phone, None)
            raise HTTPException(status_code=500, detail=f"Ikkilamchi parol xatosi: {e}")

@app.get("/active")
async def list_active():
    return {"active": list(active_clients.keys())}

# ---------------- STARTUP / SHUTDOWN ----------------
@app.on_event("startup")
async def on_startup():
    logger.info("Startup: tozalash — eski sqlite .session fayllarni tekshirish va StringSession fayllarni yuklash...")

    # 1) remove legacy .session SQLite files (if any) to avoid Telethon picking them up accidentally.
    #    We only remove .session files that have no matching .sessionstr file.
    for fname in os.listdir(SESSIONS_DIR):
        # ignore .sessionstr
        if fname.endswith(".session"):
            path = os.path.join(SESSIONS_DIR, fname)
            digits = re.sub(r"\D", "", fname)
            sessionstr_path = os.path.join(SESSIONS_DIR, f"{digits}.sessionstr")
            if not os.path.exists(sessionstr_path):
                try:
                    os.remove(path)
                    logger.info(f"Deprecated sqlite session removed: {path}")
                except Exception as e:
                    logger.warning(f"Could not remove {path}: {e}")

    # 2) load .sessionstr files
    for fname in os.listdir(SESSIONS_DIR):
        if not fname.endswith(".sessionstr"):
            continue
        try:
            digits = re.sub(r"\D", "", fname)
            phone = "+" + digits
            path = os.path.join(SESSIONS_DIR, fname)
            with open(path, "r", encoding="utf-8") as f:
                session_str = f.read().strip()
            if not session_str:
                logger.warning(f"{phone} uchun session satri bo'sh, o'tkazildi.")
                continue
            client = TelegramClient(StringSession(session_str), API_ID, API_HASH)
            # connect with retry wrapper
            async def _connect_client():
                await client.connect()
            await retry_on_sqlite_locked(_connect_client)
            if await client.is_user_authorized():
                active_clients[phone] = {"client": client, "handler_added": False}
                await add_monitor_handler(client, phone)
                logger.info(f"{phone} diskdan yuklandi va monitoringga qo'yildi.")
            else:
                logger.warning(f"{phone} diskdagi session yaroqsiz (not authorized). Faylni o'chirish mumkin.")
        except Exception as e:
            logger.exception(f"Startup: sessiyani yuklashda xato ({fname}): {e}")

@app.on_event("shutdown")
async def on_shutdown():
    logger.info("Shutdown: disconnect all clients.")
    # disconnect active clients
    for phone, meta in list(active_clients.items()):
        client: TelegramClient = meta.get("client")
        try:
            await client.disconnect()
            logger.info(f"[{phone}] disconnected.")
        except Exception as e:
            logger.warning(f"[{phone}] disconnect error: {e}")
    active_clients.clear()
    # disconnect sessions in progress
    for phone, meta in list(sessions.items()):
        try:
            await meta["client"].disconnect()
        except:
            pass
    sessions.clear()

# Run with: uvicorn telegram_api:app --host 0.0.0.0 --port 2806
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("telegram_api:app", host="0.0.0.0", port=2806, log_level="info")
