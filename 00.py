import os
import logging
import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
from fastapi import FastAPI, Request
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters
)
from telegram.ext import ApplicationBuilder

# === CẤU HÌNH ===
BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHANNEL_ID = int(os.environ.get("CHANNEL_ID", "-100..."))
WEBHOOK_PATH = f"/webhook/{BOT_TOKEN}"
APP_URL = os.environ.get("RENDER_EXTERNAL_URL", "")  # dùng trên Render

# === LOGGING ===
logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

# === ĐỌC GOOGLE SHEET ===
def load_key_map_from_sheet():
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        json_key = os.environ.get("GOOGLE_SHEET_JSON")
        if not json_key:
            raise Exception("⚠️ Thiếu GOOGLE_SHEET_JSON")

        with open("temp_key.json", "w", encoding="utf-8") as f:
            f.write(json_key)

        credentials = ServiceAccountCredentials.from_json_keyfile_name("temp_key.json", scope)
        gc = gspread.authorize(credentials)

        SHEET_NAME = os.environ.get("SHEET_NAME", "KeyData")
        tabs = os.environ.get("SHEET_TABS", "1").split(",")
        tabs = [tab.strip() for tab in tabs]

        combined_df = pd.DataFrame()
        sheet_file = gc.open(SHEET_NAME)

        for tab in tabs:
            try:
                worksheet = sheet_file.worksheet(tab)
                df = pd.DataFrame(worksheet.get_all_records())
                df["key"] = df["key"].astype(str).str.strip().str.lower()
                combined_df = pd.concat([combined_df, df], ignore_index=True)
            except Exception as e:
                logger.warning(f"❗ Tab lỗi: {tab} - {e}")

        return {
            key: group[["name_file", "message_id"]].to_dict("records")
            for key, group in combined_df.groupby("key")
        }

    except Exception as e:
        logger.error(f"❌ Lỗi tải sheet: {e}")
        return {}

KEY_MAP = load_key_map_from_sheet()

# === HANDLERS ===
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("♥️ Please send KEY UExxxxxx to receive file.")

async def handle_key(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_input = update.message.text.strip().lower()
    chat_id = update.effective_chat.id

    if user_input in KEY_MAP:
        errors = 0
        for file_info in KEY_MAP[user_input]:
            try:
                # Forward để lấy document có metadata
                msg = await context.bot.forward_message(
                    chat_id=chat_id,
                    from_chat_id=CHANNEL_ID,
                    message_id=int(file_info["message_id"])
                )
                if msg.document:
                    file = await context.bot.get_file(msg.document.file_id)
                    if file.file_size < 100_000:
                        await update.message.reply_text("⚠️ File nhỏ hơn 100KB. Liên hệ admin: https://t.me/A911Studio")

                await update.message.reply_text(f"♥️ Your File \"{file_info['name_file']}\"")
            except Exception as e:
                errors += 1
                logger.error(f"❌ Gửi file lỗi: {file_info['name_file']} - {e}")

        if errors:
            await update.message.reply_text("⚠️ Một số file lỗi. Liên hệ admin: https://t.me/A911Studio")
    else:
        await update.message.reply_text("❌ KEY is incorrect.")

# === FASTAPI APP + TELEGRAM BOT ===
app = FastAPI()
telegram_app = ApplicationBuilder().token(BOT_TOKEN).build()
telegram_app.add_handler(CommandHandler("start", start))
telegram_app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_key))

@app.post(WEBHOOK_PATH)
async def telegram_webhook(req: Request):
    json_data = await req.json()
    update = Update.de_json(json_data, telegram_app.bot)
    await telegram_app.update_queue.put(update)
    return {"ok": True}

@app.on_event("startup")
async def on_startup():
    # Đặt webhook khi khởi chạy
    if APP_URL:
        url = f"{APP_URL}{WEBHOOK_PATH}"
        await telegram_app.bot.set_webhook(url=url)
        logger.info(f"✅ Webhook set to: {url}")
