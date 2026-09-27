import asyncio
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from telethon import TelegramClient, events
from telethon.errors import SessionPasswordNeededError, PhoneNumberInvalidError, FloodWaitError
import os
import uvicorn
import logging
import sqlite3
from contextlib import AsyncExitStack

# Logging sozlamalari
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# API ma'lumotlari
API_ID = 22962676
API_HASH = '543e9a4d695fe8c6aa4075c9525f7c57'
BOT_USERNAME = '@HUMOcardbot'

# Sessionlar uchun papka
SESSION_DIR = "sessions"
os.makedirs(SESSION_DIR, exist_ok=True)

# FastAPI ilovasi
app = FastAPI(
    title="Telegram Login and HUMOcardbot Monitor API",
    description="Telegram hisoblariga kirish va @HUMOcardbot dan kelgan xabarlarni kuzatish uchun API.",
    version="1.0.0"
)

# Sessiya ma'lumotlarini saqlash
sessions = {}
active_clients = {}  # Faol klientlarni saqlash

async def monitor_messages(client, phone):
    """Botdan kelgan xabarlarni kuzatish va chop etish"""
    try:
        @client.on(events.NewMessage(from_users=BOT_USERNAME))
        async def handler(event):
            logger.info(f"Hisob: {phone} | Botdan xabar: {event.message.message} | Vaqt: {event.message.date}")
        
        logger.info(f"Hisob {phone} uchun xabar kuzatish jarayoni ishga tushdi.")
        await client.run_until_disconnected()
    except Exception as e:
        logger.error(f"Xabar kuzatishda xato (hisob: {phone}): {str(e)}")

async def start_client(phone):
    """Hisob uchun klientni ishga tushirish"""
    if phone in active_clients:
        logger.info(f"Hisob {phone} allaqachon faol, qayta ishga tushirish shart emas.")
        return active_clients[phone]
    
    async with AsyncExitStack() as stack:
        try:
            client = TelegramClient(f'{SESSION_DIR}/{phone}', API_ID, API_HASH)
            stack.push_async_callback(client.disconnect)  # Har doim yopishni ta'minlash
            await client.connect()
            if not await client.is_user_authorized():
                logger.warning(f"Hisob {phone} uchun kirish talab qilinadi.")
                return None
            active_clients[phone] = client
            logger.info(f"Hisob {phone} uchun klient muvaffaqiyatli ulandi.")
            asyncio.create_task(monitor_messages(client, phone))
            stack.pop_all()  # Muvaffaqiyatli bo'lsa, yopishni bekor qilamiz
            return client
        except FloodWaitError as e:
            logger.error(f"Flood cheklovi: {phone} uchun {e.seconds} soniya kutish kerak.")
            return None
        except sqlite3.OperationalError as e:
            logger.error(f"SQLite xatosi (hisob: {phone}): {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Klientni ishga tushirishda xato (hisob: {phone}): {str(e)}")
            return None

async def check_and_clean_sessions():
    """Mavjud sessiyalarni tekshirish va yaroqsizlarini o'chirish"""
    existing_sessions = [f.replace('.session', '') for f in os.listdir(SESSION_DIR) if f.endswith('.session')]
    for phone in existing_sessions:
        async with AsyncExitStack() as stack:
            try:
                client = TelegramClient(f'{SESSION_DIR}/{phone}', API_ID, API_HASH)
                stack.push_async_callback(client.disconnect)  # Har doim yopishni ta'minlash
                await client.connect()
                if await client.is_user_authorized():
                    logger.info(f"Hisob {phone} ulandi")
                    active_clients[phone] = client
                    asyncio.create_task(monitor_messages(client, phone))
                    stack.pop_all()  # Muvaffaqiyatli bo'lsa, yopishni bekor qilamiz
                else:
                    logger.warning(f"Hisob {phone} uchun kirish talab qilinadi, sessiya o'chiriladi.")
                    os.remove(f'{SESSION_DIR}/{phone}.session')
                    logger.info(f"Hisob {phone} chiqarib yuborildi")
            except Exception as e:
                logger.error(f"Hisob {phone} tekshirishda xato: {str(e)}")
                try:
                    os.remove(f'{SESSION_DIR}/{phone}.session')
                    logger.info(f"Hisob {phone} chiqarib yuborildi")
                except Exception as rm_e:
                    logger.error(f"Hisob {phone} sessiya faylini o'chirishda xato: {str(rm_e)}")

# Pydantic modellari
class NewNumberRequest(BaseModel):
    phone: str
    class Config:
        json_schema_extra = {
            "example": {
                "phone": "+998901234567"
            }
        }

class SmsCodeRequest(BaseModel):
    phone: str
    code: str
    class Config:
        json_schema_extra = {
            "example": {
                "phone": "+998901234567",
                "code": "12345"
            }
        }

class TwoStepRequest(BaseModel):
    phone: str
    password: str
    class Config:
        json_schema_extra = {
            "example": {
                "phone": "+998901234567",
                "password": "your_password"
            }
        }

@app.post("/newNumber", summary="Yangi telefon raqamini kiritish", response_description="SMS kod so'rovi yuborilganligi haqida javob")
async def new_number(request: NewNumberRequest):
    """Yangi telefon raqamini qayta ishlash"""
    phone = request.phone
    async with AsyncExitStack() as stack:
        try:
            client = TelegramClient(f'{SESSION_DIR}/{phone}', API_ID, API_HASH)
            stack.push_async_callback(client.disconnect)  # Har doim yopishni ta'minlash
            await client.connect()
            sent_code = await client.send_code_request(phone)
            sessions[phone] = {'client': client, 'code_hash': sent_code.phone_code_hash}
            logger.info(f"Hisob {phone} uchun SMS kod so'raldi.")
            stack.pop_all()  # Muvaffaqiyatli bo'lsa, yopishni bekor qilamiz
            return {"message": "SMS kod jo'natildi.", "next_step": f"/smscode {phone} <kod>"}
        except PhoneNumberInvalidError:
            raise HTTPException(status_code=400, detail="Noto'g'ri telefon raqami.")
        except FloodWaitError as e:
            raise HTTPException(status_code=429, detail=f"Telegram cheklovi: {e.seconds} soniya kutish kerak.")
        except sqlite3.OperationalError as e:
            raise HTTPException(status_code=500, detail=f"Sessiya fayli qulflangan: {str(e)}. Iltimos, sessions papkasini tozalang yoki qayta urinib ko'ring.")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Xato: {str(e)}")

@app.post("/smscode", summary="SMS kodini kiritish", response_description="Hisobga kirish yoki ikkilamchi parol so'rovi")
async def sms_code(request: SmsCodeRequest):
    """SMS kodini qayta ishlash"""
    phone = request.phone
    if phone not in sessions:
        raise HTTPException(status_code=400, detail="Avval /newNumber bilan telefon raqamini kiriting.")
    
    client = sessions[phone]['client']
    code_hash = sessions[phone]['code_hash']
    
    async with AsyncExitStack() as stack:
        try:
            stack.push_async_callback(client.disconnect)  # Har doim yopishni ta'minlash
            await client.sign_in(phone, request.code, phone_code_hash=code_hash)
            new_client = await start_client(phone)
            if new_client is None:
                logger.error(f"Hisob {phone} uchun xabar kuzatishni boshlash muvaffaqiyatsiz.")
                raise HTTPException(status_code=500, detail="Xabar kuzatishni boshlashda xato yuz berdi. Iltimos, sessions papkasini tozalang yoki qayta urinib ko'ring.")
            del sessions[phone]
            logger.info(f"Hisob {phone} muvaffaqiyatli kiritildi va xabar kuzatish boshlandi.")
            return {"message": f"Hisobga muvaffaqiyatli kirdingiz: {phone}. Session fayli saqlandi. @HUMOcardbot xabar kuzatish boshlandi."}
        except SessionPasswordNeededError:
            stack.pop_all()  # Ikkilamchi parol talab qilinsa, yopishni bekor qilamiz
            return {"message": "Ikkilamchi himoya yoqilgan.", "next_step": f"/towstep {phone} <parol>"}
        except FloodWaitError as e:
            raise HTTPException(status_code=429, detail=f"Telegram cheklovi: {e.seconds} soniya kutish kerak.")
        except sqlite3.OperationalError as e:
            raise HTTPException(status_code=500, detail=f"Sessiya fayli qulflangan: {str(e)}. Iltimos, sessions papkasini tozalang yoki qayta urinib ko'ring.")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Xato: {str(e)}")

@app.post("/towstep", summary="Ikkilamchi parolni kiritish", response_description="Hisobga kirish yakunlanganligi haqida javob")
async def two_step(request: TwoStepRequest):
    """Ikkilamchi himoya parolini qayta ishlash"""
    phone = request.phone
    if phone not in sessions:
        raise HTTPException(status_code=400, detail="Avval /newNumber va /smscode bilan jarayonni boshlang.")
    
    client = sessions[phone]['client']
    
    async with AsyncExitStack() as stack:
        try:
            stack.push_async_callback(client.disconnect)  # Har doim yopishni ta'minlash
            await client.sign_in(password=request.password)
            new_client = await start_client(phone)
            if new_client is None:
                logger.error(f"Hisob {phone} uchun xabar kuzatishni boshlash muvaffaqiyatsiz.")
                raise HTTPException(status_code=500, detail="Xabar kuzatishni boshlashda xato yuz berdi. Iltimos, sessions papkasini tozalang yoki qayta urinib ko'ring.")
            del sessions[phone]
            logger.info(f"Hisob {phone} muvaffaqiyatli kiritildi va xabar kuzatish boshlandi.")
            return {"message": f"Hisobga muvaffaqiyatli kirdingiz: {phone}. Session fayli saqlandi. @HUMOcardbot xabar kuzatish boshlandi."}
        except FloodWaitError as e:
            raise HTTPException(status_code=429, detail=f"Telegram cheklovi: {e.seconds} soniya kutish kerak.")
        except sqlite3.OperationalError as e:
            raise HTTPException(status_code=500, detail=f"Sessiya fayli qulflangan: {str(e)}. Iltimos, sessions papkasini tozalang yoki qayta urinib ko'ring.")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Xato: {str(e)}")

@app.on_event("startup")
async def startup_event():
    """Dastur ishga tushganda mavjud sessionlarni tekshirish va yuklash"""
    await check_and_clean_sessions()

@app.on_event("shutdown")
async def shutdown_event():
    """Dastur yopilganda barcha klientlarni to'g'ri yopish"""
    for phone, client in active_clients.items():
        try:
            await client.disconnect()
            logger.info(f"Klient {phone} yopildi.")
        except Exception as e:
            logger.error(f"Klient {phone} yopishda xato: {str(e)}")
    active_clients.clear()

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=2806)