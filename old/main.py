# main.py
import os
import re
import sys
import time
import json
import asyncio
import logging
import subprocess
from typing import Dict
from pathlib import Path

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import SessionPasswordNeededError, PhoneNumberInvalidError, FloodWaitError
from telethon.tl.functions.account import GetPasswordRequest

# ---------------- CONFIG ----------------
API_ID = 22962676
API_HASH = "543e9a4d695fe8c6aa4075c9525f7c57"
SESSIONS_DIR = Path("sessions")
SESSIONS_DIR.mkdir(exist_ok=True)

# python executable for spawning workers
PY = sys.executable

# ---------------- LOG ----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("main")

# ---------------- APP & GLOBALS ----------------
app = FastAPI(title="Coordinator - Telegram login + monitor", version="1.0")

# temp login map: phone -> {"client": TelegramClient, "code_hash": ...}
login_sessions: Dict[str, Dict] = {}
# monitor processes: phone -> subprocess.Popen
monitor_procs: Dict[str, subprocess.Popen] = {}

# per-phone asyncio lock to prevent concurrent ops on same phone
_per_phone_locks: Dict[str, asyncio.Lock] = {}

def normalize_phone(phone: str) -> str:
    s = phone.strip()
    s = re.sub(r"[^\d+]", "", s)
    if not s.startswith("+"):
        s = "+" + s
    return s

def phone_session_path(phone: str) -> Path:
    digits = re.sub(r"\D", "", phone)
    return SESSIONS_DIR / f"{digits}.sessionstr"

def get_lock(phone: str) -> asyncio.Lock:
    if phone not in _per_phone_locks:
        _per_phone_locks[phone] = asyncio.Lock()
    return _per_phone_locks[phone]

class NewNumber(BaseModel):
    phone: str

class SmsCode(BaseModel):
    phone: str
    code: str

class TwoStep(BaseModel):
    phone: str
    password: str

async def spawn_monitor(phone: str):
    """Start monitor_worker.py as subprocess for given phone. If already running, restart it."""
    path = str(phone_session_path(phone).absolute())
    if not os.path.exists(path):
        logger.error("Session file not found for spawn_monitor: %s", path)
        return False

    # If a process exists, terminate it (restart)
    proc = monitor_procs.get(phone)
    if proc and proc.poll() is None:
        logger.info("Terminating existing monitor process for %s (pid=%s)", phone, proc.pid)
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except Exception:
            proc.kill()

    cmd = [PY, "monitor_worker.py", phone, path]
    logger.info("Spawning monitor: %s", cmd)
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    monitor_procs[phone] = proc
    # optional: read a small amount of stdout asynchronously? For now, just log pid
    logger.info("Monitor started for %s with pid=%s", phone, proc.pid)
    return True

# --- ENDPOINTS ---
@app.post("/newNumber")
async def new_number(req: NewNumber):
    phone = normalize_phone(req.phone)
    if not phone:
        raise HTTPException(400, "Telefon noto'g'ri")

    lock = get_lock(phone)
    async with lock:
        if phone in login_sessions:
            return {"message": "Login jarayoni allaqachon mavjud."}
        if phone in monitor_procs and monitor_procs[phone].poll() is None:
            return {"message": "Monitoring allaqachon ishlamoqda."}

        # create StringSession client for login only
        client = TelegramClient(StringSession(), API_ID, API_HASH)
        try:
            await client.connect()
            sent = await client.send_code_request(phone)
            login_sessions[phone] = {"client": client, "code_hash": sent.phone_code_hash}
            logger.info("SMS kod so'raldi: %s", phone)
            return {"message": "SMS kod yuborildi", "next": "POST /smscode"}
        except PhoneNumberInvalidError:
            try: await client.disconnect()
            except: pass
            raise HTTPException(400, "Noto'g'ri telefon")
        except FloodWaitError as e:
            try: await client.disconnect()
            except: pass
            raise HTTPException(429, f"FloodWait: {e.seconds} soniya")
        except Exception as e:
            try: await client.disconnect()
            except: pass
            logger.exception("newNumber error")
            raise HTTPException(500, str(e))

@app.post("/smscode")
async def smscode(req: SmsCode):
    phone = normalize_phone(req.phone)
    if phone not in login_sessions:
        raise HTTPException(400, "Avval /newNumber chaqiring")

    lock = get_lock(phone)
    async with lock:
        data = login_sessions.get(phone)
        if not data:
            raise HTTPException(400, "Login sessiyasi topilmadi")
        client: TelegramClient = data["client"]
        code_hash = data["code_hash"]
        try:
            await client.sign_in(phone, req.code, phone_code_hash=code_hash)

            # If 2fa needed, Telethon raises SessionPasswordNeededError — handled below.
            if not await client.is_user_authorized():
                raise HTTPException(500, "Avtorizatsiya yakunlanmadi")

            # Grab StringSession and persist it to file
            session_str = str(client.session)
            path = phone_session_path(phone)
            # small delay to avoid instantaneous re-open collision
            await client.disconnect()
            # write sessionstr
            path.write_text(session_str, encoding="utf-8")

            # spawn monitor process (will read same .sessionstr file)
            # small sleep to make sure file is flushed on disk
            await asyncio.sleep(0.2)
            started = await spawn_monitor(phone)
            if not started:
                raise HTTPException(500, "Monitorni ishga tushirishda xato")

            # cleanup login_sessions
            login_sessions.pop(phone, None)
            logger.info("%s hisobiga kirildi va monitoring boshlandi.", phone)
            return {"message": f"{phone} hisobiga kirildi. Monitoring boshlandi."}

        except SessionPasswordNeededError:
            try:
                pwd = await client(GetPasswordRequest())
                hint = getattr(pwd, "hint", "") or ""
            except Exception:
                hint = ""
            # leave client connected in login_sessions (so /twostep can use it)
            return {"message": "Ikkilamchi parol kerak", "hint": hint, "next": "POST /twostep"}
        except Exception as e:
            logger.exception("smscode error")
            try: await client.disconnect()
            except: pass
            login_sessions.pop(phone, None)
            raise HTTPException(500, f"Sessiya/sign_in xatosi: {e}")

@app.post("/twostep")
async def twostep(req: TwoStep):
    phone = normalize_phone(req.phone)
    if phone not in login_sessions:
        raise HTTPException(400, "Avval /newNumber va /smscode chaqiring")

    lock = get_lock(phone)
    async with lock:
        data = login_sessions.get(phone)
        if not data:
            raise HTTPException(400, "Login sessiyasi yo'q")
        client: TelegramClient = data["client"]
        try:
            await client.sign_in(password=req.password)
            if not await client.is_user_authorized():
                raise HTTPException(500, "Ikkilamchi parol bilan avtorizatsiya yakunlanmadi")

            # persist sessionstr
            session_str = str(client.session)
            path = phone_session_path(phone)
            await client.disconnect()
            path.write_text(session_str, encoding="utf-8")

            await asyncio.sleep(0.2)
            started = await spawn_monitor(phone)
            if not started:
                raise HTTPException(500, "Monitorni ishga tushirishda xato")

            login_sessions.pop(phone, None)
            logger.info("%s ikkilamchi parol bilan monitoringga qo'yildi.", phone)
            return {"message": f"{phone} hisobiga ikkilamchi parol bilan kirildi. Monitoring boshlandi."}
        except Exception as e:
            logger.exception("twostep error")
            try: await client.disconnect()
            except: pass
            login_sessions.pop(phone, None)
            raise HTTPException(500, f"Ikkilamchi parol xatosi: {e}")

@app.get("/active")
async def list_active():
    running = {}
    for phone, proc in monitor_procs.items():
        running[phone] = {"pid": proc.pid, "alive": proc.poll() is None}
    return running

@app.post("/stop")
async def stop_monitor(phone: str):
    phone = normalize_phone(phone)
    proc = monitor_procs.get(phone)
    if not proc:
        raise HTTPException(404, "Monitor topilmadi")
    if proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except Exception:
            proc.kill()
    monitor_procs.pop(phone, None)
    return {"message": "Monitor to'xtatildi."}

# optional: cleanup of legacy sqlite .session files
@app.on_event("startup")
async def startup_cleanup():
    logger.info("Startup: tekshirish — sessions papkasi: %s", SESSIONS_DIR)
    for f in SESSIONS_DIR.iterdir():
        if f.suffix == ".session":  # legacy sqlite session
            logger.info("Topildi eski .session fayl (o'chirilyapti): %s", f)
            try:
                f.unlink()
            except Exception as e:
                logger.warning("O'chirishda xato: %s", e)
