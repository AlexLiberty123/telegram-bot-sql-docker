import asyncio
import logging
import os
import psycopg2
import re
from aiogram import Bot, Dispatcher, types
from aiogram.types import Message
from openai import AsyncOpenAI
from db_init import init_db

# --- КОНФИГУРАЦИЯ ---
TOKEN = os.getenv("TG_TOKEN")
OPENROUTER_API_KEY = os.getenv("OPENROUTER_KEY")
MODEL_NAME = "arcee-ai/trinity-large-preview:free"

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASS")
}

logging.basicConfig(level=logging.INFO)

aclient = AsyncOpenAI(
    api_key=OPENROUTER_API_KEY,
    base_url="https://openrouter.ai/api/v1",
)

bot = Bot(token=TOKEN)
dp = Dispatcher()

# --- СИСТЕМНЫЙ ПРОМПТ (ФИНАЛЬНЫЙ) ---
SYSTEM_PROMPT = """
You are an intelligent SQL generator for PostgreSQL.
Your goal: Return a SINGLE NUMBER based on the user's question.

DATABASE SCHEMA:
1. `videos` (id, creator_id, video_created_at, views_count, likes_count, comments_count, reports_count).
   - Use this for TOTAL stats and Filtering by Author.
   - Use `video_created_at` for questions about "New/Published/Appeared" videos.
2. `snapshots` (video_id, delta_views_count, delta_likes_count, created_at).
   - Use this ONLY for GROWTH/CHANGE (набрало, выросло) on a specific DATE.

RULES:
1. Return ONLY the raw SQL query. No markdown.
2. If asking for ID/Name/Text -> Return: IGNORE
3. Combine filters using AND.
   - Example: "Creator X AND > 1000 views" -> `WHERE creator_id = 'X' AND views_count > 1000`.
4. Handle numbers with spaces: "10 000" -> 10000.
5. "По итоговой статистике" -> Use table `videos`.
6. "Вышло/Появилось" (Published) -> Use `video_created_at` from `videos`.

EXAMPLES:
User: "Сколько видео у креатора abc... набрали больше 10 000 просмотров?"
AI: SELECT COUNT(*) FROM videos WHERE creator_id = 'abc...' AND views_count > 10000;

User: "Сколько видео появилось в мае 2025?"
AI: SELECT COUNT(*) FROM videos WHERE TO_CHAR(video_created_at, 'YYYY-MM') = '2025-05';

User: "Сколько просмотров набрали видео 26 ноября?" (Growth)
AI: SELECT SUM(delta_views_count) FROM snapshots WHERE created_at::DATE = '2025-11-26';

User: "Сколько всего видео?"
AI: SELECT COUNT(*) FROM videos;
"""

async def generate_sql(text: str):
    try:
        response = await aclient.chat.completions.create(
            model=MODEL_NAME,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": text}
            ],
            temperature=0,
            max_tokens=200
        )
        content = response.choices[0].message.content.strip()
        content = re.sub(r'```sql|```', '', content).strip()
        
        if "IGNORE" in content.upper():
            return None
            
        return content
    except Exception as e:
        logging.error(f"AI Error: {e}")
        return None

def execute_sql(sql: str):
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute(sql)
        result = cursor.fetchone()
        conn.close()
        
        if result and result[0] is not None:
            val = result[0]
            # Игнорируем строки (ID), если вдруг проскочили
            if isinstance(val, str) and not val.replace('.', '', 1).isdigit():
                return None
            
            if isinstance(val, (int, float)):
                if float(val).is_integer():
                    return int(val)
                return round(val, 2)
            return val
        return 0
    except Exception as e:
        logging.error(f"SQL Error: {e}")
        return None
    finally:
        if conn: conn.close()

@dp.message()
async def handler(msg: Message):
    sql_query = await generate_sql(msg.text)
    
    if not sql_query: 
        return # Молчим

    result = execute_sql(sql_query)
    
    if result is not None:
        await msg.answer(str(result))

async def main():
    init_db()
    await bot.delete_webhook(drop_pending_updates=True)
    logging.info("Бот запущен v3 (Fix: Filters & Spaces)")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())