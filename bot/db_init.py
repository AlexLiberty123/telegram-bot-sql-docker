import json
import psycopg2
import os
import time
import logging

# Настройки из переменных окружения
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "my_video_data")
DB_USER = os.getenv("DB_USER", "myuser")
DB_PASS = os.getenv("DB_PASS", "mypassword")
JSON_FILE = 'videos.json'

def get_conn():
    while True:
        try:
            conn = psycopg2.connect(
                host=DB_HOST,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASS
            )
            return conn
        except psycopg2.OperationalError:
            logging.info("Ожидание запуска базы данных...")
            time.sleep(2)

def init_db():
    conn = get_conn()
    cursor = conn.cursor()

    # 1. Создаем таблицы (типы данных адаптированы под Postgres)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS videos (
        id UUID PRIMARY KEY,
        creator_id TEXT,
        video_created_at TIMESTAMPTZ,
        views_count BIGINT,
        likes_count BIGINT,
        comments_count BIGINT,
        reports_count BIGINT,
        created_at TIMESTAMPTZ,
        updated_at TIMESTAMPTZ
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS snapshots (
        id UUID PRIMARY KEY,
        video_id UUID REFERENCES videos(id),
        views_count BIGINT,
        likes_count BIGINT,
        comments_count BIGINT,
        reports_count BIGINT,
        delta_views_count BIGINT,
        delta_likes_count BIGINT,
        delta_comments_count BIGINT,
        delta_reports_count BIGINT,
        created_at TIMESTAMPTZ,
        updated_at TIMESTAMPTZ
    );
    """)
    conn.commit()

    # 2. Читаем JSON
    try:
        with open(JSON_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except FileNotFoundError:
        logging.error("videos.json не найден!")
        return

    videos_list = data.get("videos", [])
    logging.info(f"Начинаем импорт {len(videos_list)} видео в PostgreSQL...")

    # 3. Импорт данных
    try:
        for video in videos_list:
            cursor.execute("""
                INSERT INTO videos (id, creator_id, video_created_at, views_count, likes_count, comments_count, reports_count, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                views_count = EXCLUDED.views_count,
                likes_count = EXCLUDED.likes_count,
                updated_at = EXCLUDED.updated_at;
            """, (
                video.get('id'), video.get('creator_id'), video.get('video_created_at'),
                video.get('views_count'), video.get('likes_count'), video.get('comments_count'),
                video.get('reports_count'), video.get('created_at'), video.get('updated_at')
            ))

            for snap in video.get('snapshots', []):
                cursor.execute("""
                    INSERT INTO snapshots (id, video_id, views_count, likes_count, comments_count, reports_count, 
                     delta_views_count, delta_likes_count, delta_comments_count, delta_reports_count, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO NOTHING;
                """, (
                    snap.get('id'), video.get('id'),
                    snap.get('views_count'), snap.get('likes_count'), snap.get('comments_count'), snap.get('reports_count'),
                    snap.get('delta_views_count'), snap.get('delta_likes_count'), snap.get('delta_comments_count'), snap.get('delta_reports_count'),
                    snap.get('created_at'), snap.get('updated_at')
                ))

        conn.commit()
        logging.info("Импорт завершен успешно!")

    except Exception as e:
        logging.error(f"Ошибка при импорте: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    init_db()