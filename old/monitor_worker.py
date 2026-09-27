# monitor_worker.py
import os
import sys
import time
import logging
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from pathlib import Path

API_ID = 22962676
API_HASH = "543e9a4d695fe8c6aa4075c9525f7c57"
BOT_USERNAME = "@HUMOcardbot"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("monitor")

def read_session_str(path: str) -> str:
    p = Path(path)
    return p.read_text(encoding="utf-8").strip()

def add_handler(client: TelegramClient, phone: str):
    async def on_new(event):
        text = getattr(event.message, "message", None) or getattr(event.message, "text", None) or ""
        logger.info("[%s] Botdan xabar: %s", phone, text)
    client.add_event_handler(on_new, events.NewMessage(from_users=BOT_USERNAME))
    logger.info("[%s] Handler qo'shildi.", phone)

def monitor_loop(phone: str, session_path: str):
    # The monitor process will try to (re)connect forever with backoff.
    while True:
        try:
            session_str = read_session_str(session_path)
            if not session_str:
                logger.error("Session string bo'sh: %s", session_path)
                time.sleep(2)
                continue

            client = TelegramClient(StringSession(session_str), API_ID, API_HASH)

            logger.info("[%s] Client yaratildi, connect qilinmoqda...", phone)
            client.start()  # start connects and ensures authorized
            if not client.is_user_authorized():
                logger.error("[%s] Client authorized emas. Exit.", phone)
                client.disconnect()
                return

            add_handler(client, phone)
            logger.info("[%s] Monitoring boshlandi — run_until_disconnected() chaqirilmoqda", phone)
            client.run_until_disconnected()
            logger.warning("[%s] run_until_disconnected() tugadi, qayta ulanmoqda...", phone)
            # if disconnected normally, loop will restart and try to reconnect using same session file
            time.sleep(1)
        except Exception as e:
            logger.exception("[%s] Monitor xato: %s", phone, e)
            try:
                client.disconnect()
            except:
                pass
            # small backoff then retry (also re-read session file)
            time.sleep(3)

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python monitor_worker.py <phone> <session_path>")
        sys.exit(1)
    phone = sys.argv[1]
    session_path = sys.argv[2]
    monitor_loop(phone, session_path)
