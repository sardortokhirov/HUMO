import os
import re
import sys
import json
import asyncio
import logging
import sqlite3
from typing import Dict, Optional, List
from pathlib import Path
from datetime import datetime, timedelta
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware

from telethon import TelegramClient, events
from telethon.sessions import StringSession, SQLiteSession
from telethon.errors import SessionPasswordNeededError, PhoneNumberInvalidError, FloodWaitError
from telethon.tl.functions.account import GetPasswordRequest
from telethon.tl.functions.users import GetFullUserRequest

# ---------------- CONFIG ----------------
API_ID = 22962676
API_HASH = "543e9a4d695fe8c6aa4075c9525f7c57"

SESSIONS_DIR = Path("sessions")
SESSIONS_DIR.mkdir(exist_ok=True)
LOGS_DIR = Path("logs")
LOGS_DIR.mkdir(exist_ok=True)
DB_PATH = Path("transactions.db")

# ---------------- LOG ----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("main")

# ---------------- SQLite Setup ----------------
def init_db():
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                phone TEXT NOT NULL,
                amount REAL NOT NULL,
                transaction_time TEXT NOT NULL,
                details TEXT,
                created_at TEXT NOT NULL
            )
        """)
        conn.commit()
    logger.info("SQLite database initialized: %s", DB_PATH)

# ---------------- APP & GLOBALS ----------------
app = FastAPI(title="Coordinator - Telegram login + monitor", version="1.0")


# ---------------- CORS ----------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # yoki ["http://localhost:3000"] kabi aniq domenlar
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

login_sessions: Dict[str, Dict] = {}
monitor_tasks: Dict[str, asyncio.Task] = {}
monitor_clients: Dict[str, TelegramClient] = {}
_per_phone_locks: Dict[str, asyncio.Lock] = {}


# ---------------- Helpers ----------------
def normalize_phone(phone: str) -> str:
    if not phone:
        return ""
    s = str(phone).strip()
    s = re.sub(r"[^\d+]", "", s)
    if not s:
        return ""
    if not s.startswith("+"):
        s = "+" + s
    return s

def phone_digits(phone: str) -> str:
    return re.sub(r"\D", "", phone)

def phone_session_path(phone: str) -> Path:
    digits = phone_digits(phone)
    return SESSIONS_DIR / f"{digits}.sessionstr"

def sqlite_session_path(phone: str) -> Path:
    digits = phone_digits(phone)
    return SESSIONS_DIR / f"{digits}.session"

def get_lock(phone: str) -> asyncio.Lock:
    if phone not in _per_phone_locks:
        _per_phone_locks[phone] = asyncio.Lock()
    return _per_phone_locks[phone]

def atomic_write(path: Path, data: str):
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(data, encoding="utf-8")
    tmp.replace(path)

def create_client_from_session_file(file: Path) -> Optional[TelegramClient]:
    try:
        if file.suffix == ".sessionstr":
            session_data = file.read_text(encoding="utf-8").strip()
            if not session_data:
                raise ValueError("empty sessionstr")
            if session_data.startswith("{"):
                try:
                    j = json.loads(session_data)
                    for k in ("session", "string", "session_str", "sessionString", "value"):
                        if k in j and isinstance(j[k], str) and j[k].strip():
                            session_data = j[k].strip()
                            break
                except Exception:
                    pass
            try:
                return TelegramClient(StringSession(session_data), API_ID, API_HASH)
            except ValueError:
                alt = file.with_suffix(".session")
                if alt.exists():
                    return TelegramClient(SQLiteSession(str(alt)), API_ID, API_HASH)
                return None
        elif file.suffix == ".session":
            return TelegramClient(SQLiteSession(str(file)), API_ID, API_HASH)
    except Exception:
        logger.exception("create_client_from_session_file error for %s", file.name if file else "?")
    return None

# Recovery helpers
async def convert_sqlite_to_string(sql_path: Path, out_path: Path) -> bool:
    client: Optional[TelegramClient] = None
    try:
        client = TelegramClient(SQLiteSession(str(sql_path)), API_ID, API_HASH)
        await client.connect()
        if not await client.is_user_authorized():
            await client.disconnect()
            logger.warning("convert_sqlite_to_string: %s not authorized", sql_path.name)
            return False
        session_str = client.session.save()
        atomic_write(out_path, session_str)
        await client.disconnect()
        logger.info("Converted %s -> %s", sql_path.name, out_path.name)
        return True
    except Exception as e:
        logger.exception("convert_sqlite_to_string error for %s: %s", sql_path.name, e)
        try:
            if client:
                await client.disconnect()
        except:
            pass
        return False

def extract_possible_strings_from_text(text: str) -> List[str]:
    candidates = set()
    for m in re.finditer(r'([A-Za-z0-9_\-+/=]{80,})', text):
        candidates.add(m.group(1))
    for m in re.finditer(r'["\']([A-Za-z0-9_\-+/=]{40,})["\']', text):
        candidates.add(m.group(1))
    return list(candidates)

async def try_extract_and_validate_sessionstr(file: Path, out_path: Path) -> bool:
    text = file.read_text(encoding="utf-8", errors="ignore").strip()
    if text.startswith("{"):
        try:
            j = json.loads(text)
            def iter_vals(obj):
                if isinstance(obj, dict):
                    for v in obj.values():
                        yield from iter_vals(v)
                elif isinstance(obj, list):
                    for item in obj:
                        yield from iter_vals(item)
                else:
                    yield obj
            for val in iter_vals(j):
                if isinstance(val, str) and len(val) > 30:
                    cand = val.strip()
                    try:
                        TelegramClient(StringSession(cand), API_ID, API_HASH)
                        atomic_write(out_path, cand)
                        logger.info("Extracted session string from JSON in %s", file.name)
                        return True
                    except Exception:
                        continue
        except Exception:
            pass

    for cand in extract_possible_strings_from_text(text):
        try:
            TelegramClient(StringSession(cand), API_ID, API_HASH)
            atomic_write(out_path, cand)
            logger.info("Extracted session string by regex from %s", file.name)
            return True
        except Exception:
            continue
    return False

async def _get_profile_photo_base64(client: TelegramClient, user_id: int):
    import base64
    from io import BytesIO
    try:
        bio = BytesIO()
        res = await client.download_profile_photo(user_id, file=bio)
        if res is None:
            return None
        bio.seek(0)
        return base64.b64encode(bio.read()).decode("utf-8")
    except Exception:
        return None

# ---------------- Monitor worker (with HUMO handler) ----------------
async def monitor_worker_loop(phone: str, session_file: Path):
    logger.info("monitor_worker: starting for %s (session=%s)", phone, session_file.name)
    client = create_client_from_session_file(session_file)
    if client is None:
        logger.error("monitor_worker: cannot create client for %s", phone)
        return

    monitor_clients[phone] = client

    # handler closure so it captures phone
    async def humocard_handler(event):
        try:
            sender = await event.get_sender()
            username = getattr(sender, "username", None) if sender else None
            text = event.message.message if event.message else ""
            created_at = datetime.utcnow().isoformat() + "Z"
            
            # Parse HUMO transaction message
            if "🎉" in text:
                try:
                    # Extract amount (e.g., 270.000,00)
                    amount_match = re.search(r'➕\s*([\d.,]+)\s*UZS', text)
                    amount = float(amount_match.group(1).replace(".", "").replace(",", ".")) if amount_match else None
                    
                    # Extract transaction time (e.g., 08:57 10.08.2025)
                    time_match = re.search(r'🕓\s*(\d{2}:\d{2}\s*\d{2}\.\d{2}\.\d{4})', text)
                    transaction_time = None
                    if time_match:
                        try:
                            dt = datetime.strptime(time_match.group(1), "%H:%M %d.%m.%Y")
                            transaction_time = dt.isoformat() + "Z"
                        except ValueError:
                            logger.warning("Invalid time format in message for %s: %s", phone, time_match.group(1))
                    
                    # Extract details (e.g., CORE 4 ECOM POPOL KA)
                    details_match = re.search(r'📍\s*(.*?)\s*(?=(💳|\Z))', text, re.DOTALL)
                    details = details_match.group(1).strip() if details_match else None

                    if amount and transaction_time and details:
                        with sqlite3.connect(DB_PATH) as conn:
                            cursor = conn.cursor()
                            cursor.execute("""
                                INSERT INTO transactions (phone, amount, transaction_time, details, created_at)
                                VALUES (?, ?, ?, ?, ?)
                            """, (phone, amount, transaction_time, details, created_at))
                            conn.commit()
                        logger.info("Saved transaction for %s: amount=%s, time=%s, details=%s", 
                                   phone, amount, transaction_time, details)
                    else:
                        logger.warning("Incomplete transaction data for %s: amount=%s, time=%s, details=%s", 
                                      phone, amount, transaction_time, details)
                except Exception as e:
                    logger.exception("Error parsing transaction for %s: %s", phone, e)

            # Log to file as before
            entry = {
                "ts": created_at,
                "phone": phone,
                "sender_id": sender.id if sender else None,
                "username": username,
                "text": text
            }
            humo_log_path = LOGS_DIR / f"{phone_digits(phone)}_humocard.log"
            with open(humo_log_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(entry, ensure_ascii=False) + "\n")
            logger.info("HUMO msg logged for %s: %s", phone, (text[:80] + "...") if len(text)>80 else text)
        except Exception:
            logger.exception("humocard_handler error for %s", phone)

    try:
        await client.connect()
        if not await client.is_user_authorized():
            logger.warning("monitor_worker: %s not authorized, stopping", phone)
            await client.disconnect()
            return

        logger.info("monitor_worker: %s connected and authorized", phone)

        # Register handler for Humo bot messages
        try:
            client.add_event_handler(humocard_handler, events.NewMessage(from_users='HUMOcardbot'))
            logger.info("HUMO handler registered for %s", phone)
        except Exception:
            client.add_event_handler(humocard_handler, events.NewMessage(incoming=True))
            logger.warning("HUMO handler registered as generic incoming for %s (couldn't bind by username)", phone)

        while True:
            try:
                await client(GetFullUserRequest("me"))
            except Exception as e:
                logger.warning("monitor_worker: ping failed for %s: %s", phone, e)
                try:
                    await client.disconnect()
                except:
                    pass
                await asyncio.sleep(2)
                try:
                    await client.connect()
                except Exception as e2:
                    logger.error("monitor_worker: reconnect failed for %s: %s", phone, e2)
            await asyncio.sleep(6)

    except asyncio.CancelledError:
        logger.info("monitor_worker: cancelled for %s", phone)
    except Exception as e:
        logger.exception("monitor_worker: unexpected error for %s: %s", phone, e)
    finally:
        try:
            client.remove_event_handler(humocard_handler)
        except Exception:
            pass
        try:
            await client.disconnect()
        except Exception:
            pass
        monitor_clients.pop(phone, None)
        monitor_tasks.pop(phone, None)
        logger.info("monitor_worker: stopped for %s", phone)

# spawn/cancel monitor tasks
async def spawn_monitor(phone: str, session_file: Optional[Path] = None) -> bool:
    phone = normalize_phone(phone)
    if not phone:
        logger.error("spawn_monitor: invalid phone")
        return False

    if session_file is None:
        s_path = phone_session_path(phone)
        sq_path = sqlite_session_path(phone)
        if s_path.exists():
            session_file = s_path
        elif sq_path.exists():
            session_file = sq_path
        else:
            logger.error("spawn_monitor: no session file for %s", phone)
            return False

    t = monitor_tasks.get(phone)
    if t and not t.done():
        logger.info("spawn_monitor: cancelling existing task for %s", phone)
        t.cancel()
        try:
            await asyncio.wait_for(t, timeout=5)
        except Exception:
            pass

    task = asyncio.create_task(monitor_worker_loop(phone, session_file))
    monitor_tasks[phone] = task
    logger.info("spawn_monitor: task created for %s (file=%s)", phone, session_file.name)
    return True

async def stop_monitor_task(phone: str) -> bool:
    phone = normalize_phone(phone)
    t = monitor_tasks.get(phone)
    if not t:
        return False
    if not t.done():
        t.cancel()
        try:
            await asyncio.wait_for(t, timeout=5)
        except Exception:
            pass
    monitor_tasks.pop(phone, None)
    c = monitor_clients.get(phone)
    if c:
        try: await c.disconnect()
        except: pass
        monitor_clients.pop(phone, None)
    return True

# ---------------- Models & Endpoints ----------------
class NewNumber(BaseModel):
    phone: str

class SmsCode(BaseModel):
    phone: str
    code: str

class TwoStep(BaseModel):
    phone: str
    password: str

@app.post("/newNumber")
async def new_number(req: NewNumber):
    phone = normalize_phone(req.phone)
    if not phone:
        raise HTTPException(status_code=400, detail="Telefon noto'g'ri")
    lock = get_lock(phone)
    async with lock:
        if phone in login_sessions:
            return {"message": "Login jarayoni allaqachon mavjud."}
        t = monitor_tasks.get(phone)
        if t and not t.done():
            return {"message": "Monitoring allaqachon ishlamoqda."}
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
            raise HTTPException(status_code=400, detail="Noto'g'ri telefon")
        except FloodWaitError as e:
            try: await client.disconnect()
            except: pass
            raise HTTPException(status_code=429, detail=f"FloodWait: {e.seconds} soniya")
        except Exception as e:
            try: await client.disconnect()
            except: pass
            logger.exception("newNumber error")
            raise HTTPException(status_code=500, detail=str(e))

@app.post("/smscode")
async def smscode(req: SmsCode):
    phone = normalize_phone(req.phone)
    if phone not in login_sessions:
        raise HTTPException(status_code=400, detail="Avval /newNumber chaqiring")
    lock = get_lock(phone)
    async with lock:
        data = login_sessions.get(phone)
        if not data:
            raise HTTPException(status_code=400, detail="Login sessiyasi topilmadi")
        client: TelegramClient = data["client"]
        code_hash = data.get("code_hash")
        try:
            await client.sign_in(phone, req.code, phone_code_hash=code_hash)
            if not await client.is_user_authorized():
                raise HTTPException(status_code=500, detail="Avtorizatsiya yakunlanmadi")
            session_str = client.session.save()
            path = phone_session_path(phone)
            atomic_write(path, session_str)
            await asyncio.sleep(0.2)
            ok = await spawn_monitor(phone, session_file=path)
            if not ok:
                raise HTTPException(status_code=500, detail="Monitorni ishga tushirishda xato")
            login_sessions.pop(phone, None)
            try: await client.disconnect()
            except: pass
            logger.info("%s hisobiga kirildi va monitoring boshlandi.", phone)
            return {"message": f"{phone} hisobiga kirildi. Monitoring boshlandi."}
        except SessionPasswordNeededError:
            try:
                pwd = await client(GetPasswordRequest())
                hint = getattr(pwd, "hint", "") or ""
            except Exception:
                hint = ""
            return {"message": "Ikkilamchi parol kerak", "hint": hint, "next": "POST /twostep"}
        except Exception as e:
            logger.exception("smscode error")
            try: await client.disconnect()
            except: pass
            login_sessions.pop(phone, None)
            raise HTTPException(status_code=500, detail=f"Sessiya/sign_in xatosi: {e}")

@app.post("/twostep")
async def twostep(req: TwoStep):
    phone = normalize_phone(req.phone)
    if phone not in login_sessions:
        raise HTTPException(status_code=400, detail="Avval /newNumber va /smscode chaqiring")
    lock = get_lock(phone)
    async with lock:
        data = login_sessions.get(phone)
        if not data:
            raise HTTPException(status_code=400, detail="Login sessiyasi yo'q")
        client: TelegramClient = data["client"]
        try:
            await client.sign_in(password=req.password)
            if not await client.is_user_authorized():
                raise HTTPException(status_code=500, detail="Ikkilamchi parol bilan avtorizatsiya yakunlanmadi")
            session_str = client.session.save()
            path = phone_session_path(phone)
            atomic_write(path, session_str)
            await asyncio.sleep(0.2)
            ok = await spawn_monitor(phone, session_file=path)
            if not ok:
                raise HTTPException(status_code=500, detail="Monitorni ishga tushirishda xato")
            login_sessions.pop(phone, None)
            try: await client.disconnect()
            except: pass
            logger.info("%s ikkilamchi parol bilan monitoringga qo'yildi.", phone)
            return {"message": f"{phone} hisobiga ikkilamchi parol bilan kirildi. Monitoring boshlandi."}
        except Exception as e:
            logger.exception("twostep error")
            try: await client.disconnect()
            except: pass
            login_sessions.pop(phone, None)
            raise HTTPException(status_code=500, detail=f"Ikkilamchi parol xatosi: {e}")

@app.get("/active")
async def list_active(include_photo: bool = Query(False)):
    result = []
    for phone, task in list(monitor_tasks.items()):
        if task.done():
            continue
        client = monitor_clients.get(phone)
        if not client:
            logger.warning("active: no client for %s", phone)
            continue
        try:
            try:
                await client.connect()
            except Exception:
                pass
            if not await client.is_user_authorized():
                continue
            me = await client(GetFullUserRequest("me"))
            user = me.users[0]
            profile_data = {
                "id": user.id,
                "name": f"{(user.first_name or '')} {(user.last_name or '')}".strip(),
                "username": user.username,
                "phone": phone,
                "status": "running"
            }
            if include_photo:
                profile_data["profilePicture"] = await _get_profile_photo_base64(client, user.id)
            result.append(profile_data)
        except Exception as e:
            logger.exception("active: error for %s: %s", phone, e)
            try: await client.disconnect()
            except: pass
    return result

@app.post("/stop")
async def stop_monitor(phone: str):
    phone = normalize_phone(phone)
    if not phone:
        raise HTTPException(status_code=400, detail="Telefon noto'g'ri")
    ok = await stop_monitor_task(phone)
    if not ok:
        raise HTTPException(status_code=404, detail="Monitor topilmadi")
    return {"message": "Monitor to'xtatildi."}

@app.delete("/delete")
async def delete_account(phone: str):
    phone = normalize_phone(phone)
    if not phone:
        raise HTTPException(status_code=400, detail="Telefon noto'g'ri")
    
    lock = get_lock(phone)
    async with lock:
        # Stop monitor if running
        await stop_monitor_task(phone)
        
        # Delete session string file
        s_path = phone_session_path(phone)
        if s_path.exists():
            s_path.unlink()
            logger.info("Session deleted for %s", phone)
            return {"message": "Account o'chirildi"}
        else:
            raise HTTPException(status_code=404, detail="Sessiya topilmadi")
        
@app.get("/last_transactions")
async def last_transactions(amount: float = Query(...)):
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            fifteen_minutes_ago = (datetime.utcnow() - timedelta(minutes=15)).isoformat() + "Z"
            cursor.execute("""
                SELECT phone, amount, transaction_time, details
                FROM transactions
                WHERE amount = ? AND created_at >= ?
                ORDER BY created_at DESC
            """, (amount, fifteen_minutes_ago))
            rows = cursor.fetchall()
            result = [
                {
                    "phone": row[0],
                    "amount": row[1],
                    "transaction_time": row[2],
                    "details": row[3]
                }
                for row in rows
            ]
            return {"transactions": result}
    except Exception as e:
        logger.exception("last_transactions error: %s", e)
        raise HTTPException(status_code=500, detail=f"Tranzaksiyalarni qidirishda xato: {e}")

# ---------------- Startup / Shutdown ----------------
@app.on_event("startup")
async def on_startup():
    init_db()  # Initialize SQLite database
    logger.info("Startup: yuklanmoqda — eski sessiyalarni tekshirish & recover...")
    for file in SESSIONS_DIR.glob("*"):
        phone_digits_stem = file.stem
        phone = normalize_phone("+" + phone_digits_stem)
        try:
            client = create_client_from_session_file(file)
            if client is not None:
                await client.connect()
                if await client.is_user_authorized():
                    logger.info("startup: %s authorized — launching monitor (file=%s)", phone, file.name)
                    await spawn_monitor(phone, session_file=file)
                else:
                    logger.warning("startup: %s not authorized (file=%s)", phone, file.name)
                await client.disconnect()
                continue

            logger.warning("startup: invalid session file detected: %s", file.name)
            alt_sql = file.with_suffix(".session")
            out_sessionstr = file.with_suffix(".sessionstr")
            converted = False
            if alt_sql.exists():
                logger.info("startup: trying convert sqlite %s -> %s", alt_sql.name, out_sessionstr.name)
                converted = await convert_sqlite_to_string(alt_sql, out_sessionstr)
                if converted:
                    await spawn_monitor(phone, session_file=out_sessionstr)
                    continue

            logger.info("startup: trying to extract session string from %s", file.name)
            extracted = await try_extract_and_validate_sessionstr(file, out_sessionstr)
            if extracted:
                await spawn_monitor(phone, session_file=out_sessionstr)
                continue

            invalid_name = file.with_suffix(file.suffix + ".invalid")
            try:
                file.replace(invalid_name)
                logger.warning("Moved invalid session %s -> %s", file.name, invalid_name.name)
            except Exception:
                logger.warning("Could not rename invalid file %s", file.name)
        except Exception as e:
            logger.exception("startup: error processing %s: %s", file.name, e)

@app.on_event("shutdown")
async def on_shutdown():
    logger.info("Shutdown: stopping monitors and disconnecting clients...")
    for phone, info in list(login_sessions.items()):
        client = info.get("client")
        try:
            if client:
                await client.disconnect()
        except:
            pass
        login_sessions.pop(phone, None)
    for phone in list(monitor_tasks.keys()):
        try:
            await stop_monitor_task(phone)
        except Exception:
            pass
    logger.info("Shutdown complete.")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=2806, log_level="info")