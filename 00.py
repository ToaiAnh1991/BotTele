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
CHANNEL_ID = int(os.environ.get("CHANNEL_ID", "-100..."))

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

        with open("temp_key.json", "w", encoding="utf-8") as f:
            f.write(json_key)

        credentials = ServiceAccountCredentials.from_json_keyfile_name("temp_key.json", scope)
        gc = gspread.authorize(credentials)

        SHEET_NAME = os.environ.get("SHEET_NAME", "KeyData")
        sheet_file = gc.open(SHEET_NAME)

        # 🟡 Đọc danh sách các tab cần load từ biến môi trường
        tabs = os.environ.get("SHEET_TABS", "1").split(",")
        tabs = [tab.strip() for tab in tabs]

        combined_df = pd.DataFrame()

        for tab_name in tabs:
            try:
                worksheet = sheet_file.worksheet(tab_name)
                data = worksheet.get_all_records()
                df = pd.DataFrame(data)
                df["key"] = df["key"].astype(str).str.strip().str.lower()
                combined_df = pd.concat([combined_df, df], ignore_index=True)
            except Exception as tab_error:
                logger.warning(f"⚠️ Không thể đọc tab: {tab_name} – {tab_error}")

        # 🔑 Nhóm dữ liệu theo key
        key_map = {
            key: group[["name_file", "message_id"]].to_dict("records")
            for key, group in combined_df.groupby("key")
        }

        return key_map

    except Exception as e:
        logger.error(f"Lỗi tải sheet: {e}")
        return {}

KEY_MAP = load_key_map_from_sheet()

# === /start ===
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("♥️ please send KEY UExxxxxx to receive file.")

# === Xử lý key người dùng ===
async def handle_key(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_input = update.message.text.strip().lower()
    chat_id = update.effective_chat.id

    if user_input in KEY_MAP:
        files_info = KEY_MAP[user_input]
        errors = 0

        for file_info in files_info:
            try:
                # Gửi file
                sent_message = await context.bot.copy_message(
                    chat_id=chat_id,
                    from_chat_id=CHANNEL_ID,
                    message_id=int(file_info["message_id"]),
                    protect_content=True
                )

                # Kiểm tra kích thước file nếu là tài liệu
                if sent_message.document:
                    file_id = sent_message.document.file_id
                    file = await context.bot.get_file(file_id)

                    if file.file_size < 100_000:  # dưới 100KB
                        await update.message.reply_text(
                            "⚠️ File nhỏ hơn 100KB. Vui lòng liên hệ admin để nhận bản cập nhật mới.\n👉 https://t.me/A911Studio"
                        )

                # Thông báo file đã gửi
                await update.message.reply_text(f"♥️ Your File \"{file_info['name_file']}\"")

            except Exception as e:
                logger.error(f"[ERROR] Your File '{file_info['name_file']}': {e}")
                errors += 1  # Chỉ tăng khi có ngoại lệ

        if errors:
            await update.message.reply_text(
                "⚠️ Một số file bị lỗi khi gửi. Vui lòng liên hệ admin để được hỗ trợ.\n👉 https://t.me/A911Studio"
            )
    else:
        await update.message.reply_text("❌ KEY is incorrect. Please check again.")

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
