import os
import logging
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters
)

# === ⚙️ CẤU HÌNH BOT ===
BOT_TOKEN = os.environ.get("BOT_TOKEN")  # 🔐 Token từ biến môi trường
CHANNEL_ID = int(os.environ.get("CHANNEL_ID", "-1002635653671"))

# === 🔍 LOGGING ===
logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)

# === 🔑 ĐỌC GOOGLE SHEET ===
def load_key_map_from_sheet():
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        json_key = os.environ.get("GOOGLE_SHEET_JSON")
        if not json_key:
            raise Exception("⚠️ Thiếu biến môi trường GOOGLE_SHEET_JSON")

        # Ghi file tạm nếu JSON là chuỗi
        with open("temp_key.json", "w", encoding="utf-8") as f:
            f.write(json_key)

        credentials = ServiceAccountCredentials.from_json_keyfile_name("temp_key.json", scope)
        gc = gspread.authorize(credentials)

        SHEET_NAME = os.environ.get("SHEET_NAME", "KeyData")
        sheet = gc.open(SHEET_NAME).sheet1
        data = sheet.get_all_records()

        df = pd.DataFrame(data)
        df["key"] = df["key"].astype(str).str.strip().str.lower()

        key_map = {
            key: group[["name_file", "message_id"]].to_dict("records")
            for key, group in df.groupby("key")
        }

        return key_map
    except Exception as e:
        logger.error(f"Lỗi tải sheet: {e}")
        return {}

KEY_MAP = load_key_map_from_sheet()

# === /start ===
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("👋 Gửi mã key sản phẩm để nhận file.")

# === Xử lý key người dùng ===
async def handle_key(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_input = update.message.text.strip().lower()
    chat_id = update.effective_chat.id

    if user_input in KEY_MAP:
        files_info = KEY_MAP[user_input]
        errors = 0

        for file_info in files_info:
            try:
                await context.bot.copy_message(
                    chat_id=chat_id,
                    from_chat_id=CHANNEL_ID,
                    message_id=int(file_info["message_id"]),
                    protect_content=True
                )
                await update.message.reply_text(f"✅ Đã gửi \"{file_info['name_file']}\"")
            except Exception as e:
                logger.error(f"[ERROR] Gửi file '{file_info['name_file']}': {e}")
                errors += 1

        if errors:
            await update.message.reply_text("⚠️ Một số file lỗi khi gửi.")
    else:
        await update.message.reply_text("❌ Key không đúng. Vui lòng kiểm tra lại.")

# === MAIN ===
def main():
    if not BOT_TOKEN:
        logger.error("❌ Thiếu BOT_TOKEN.")
        return

    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_key))

    print("🤖 Bot đang chạy...")
    app.run_polling()

if __name__ == "__main__":
    main()
