import os
import logging
import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from telegram import Update
from telegram.ext import (
    Application, 
    ApplicationBuilder, 
    CommandHandler, 
    MessageHandler, 
    ContextTypes, 
    filters
)
from contextlib import asynccontextmanager
import uvicorn

# === CẤU HÌNH ===
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHANNEL_ID = int(os.getenv("CHANNEL_ID", "-100..."))
APP_URL = os.getenv("RENDER_EXTERNAL_URL", "").rstrip("/")
PORT = int(os.getenv("PORT", 10000))
WEBHOOK_PATH = f"/webhook/{BOT_TOKEN}"

# === LOGGING ===
logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

# === GOOGLE SHEET ===
def load_key_map_from_sheet():
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        json_key = os.getenv("GOOGLE_SHEET_JSON")
        if not json_key:
            raise Exception("Thiếu GOOGLE_SHEET_JSON")

        with open("temp_key.json", "w", encoding="utf-8") as f:
            f.write(json_key)

        credentials = ServiceAccountCredentials.from_json_keyfile_name("temp_key.json", scope)
        gc = gspread.authorize(credentials)

        sheet_name = os.getenv("SHEET_NAME", "KeyData")
        tabs = os.getenv("SHEET_TABS", "1").split(",")

        combined_df = pd.DataFrame()
        sheet_file = gc.open(sheet_name)

        for tab in tabs:
            worksheet = sheet_file.worksheet(tab.strip())
            df = pd.DataFrame(worksheet.get_all_records())
            df["key"] = df["key"].astype(str).str.strip().str.lower()
            combined_df = pd.concat([combined_df, df], ignore_index=True)

        return {
            key: group[["name_file", "message_id"]].to_dict("records")
            for key, group in combined_df.groupby("key")
        }

    except Exception as e:
        logger.error(f"Lỗi tải sheet: {e}")
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
                sent_message = await context.bot.copy_message(
                    chat_id=chat_id,
                    from_chat_id=CHANNEL_ID,
                    message_id=int(file_info["message_id"]),
                    protect_content=True
                )

                if sent_message.document:
                    file = await context.bot.get_file(sent_message.document.file_id)
                    if file.file_size < 100_000:
                        await update.message.reply_text("⚠️ File nhỏ hơn 100KB. Liên hệ admin: https://t.me/A911Studio")

                await update.message.reply_text(f"♥️ Your File \"{file_info['name_file']}\"")
            except Exception as e:
                errors += 1
                logger.error(f"Lỗi gửi file: {file_info['name_file']} – {e}")

        if errors:
            await update.message.reply_text("⚠️ Một số file bị lỗi. Liên hệ admin: https://t.me/A911Studio")
    else:
        await update.message.reply_text("❌ KEY không đúng. Vui lòng kiểm tra lại.")

# === FastAPI App & Bot ===
@asynccontextmanager
async def lifespan(app: FastAPI):
    if APP_URL:
        await telegram_app.bot.set_webhook(f"{APP_URL}{WEBHOOK_PATH}")
        logger.info(f"Webhook set: {APP_URL}{WEBHOOK_PATH}")
    yield

app = FastAPI(lifespan=lifespan)

telegram_app: Application = ApplicationBuilder().token(BOT_TOKEN).build()
telegram_app.add_handler(CommandHandler("start", start))
telegram_app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_key))

@app.post(WEBHOOK_PATH)
async def telegram_webhook(request: Request):
    try:
        data = await request.json()
        update = Update.de_json(data, telegram_app.bot)
        await telegram_app.update_queue.put(update)
        return JSONResponse({"ok": True})
    except Exception as e:
        logger.error(f"Webhook error: {e}")
        return JSONResponse({"ok": False}, status_code=400)

# === KHỞI CHẠY ===
if __name__ == "__main__":
    print("🚀 Starting bot with Webhook...")
    uvicorn.run("main:app", host="0.0.0.0", port=PORT)
