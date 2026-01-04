import asyncio
import logging
import os
import sys
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
from typing import Optional, Dict, Any, List

from aiogram import Bot, Dispatcher, F, Router, types
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    InlineKeyboardButton, InlineKeyboardMarkup,
    ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
)
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.memory import MemoryJobStore
from dotenv import load_dotenv
import asyncpg
from asyncpg.pool import Pool

# ========== НАСТРОЙКА ЛОГИРОВАНИЯ ==========
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('bot.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

# Загружаем переменные окружения
load_dotenv()

# ========== КОНСТАНТЫ И ПЕРЕМЕННЫЕ ==========
class Config:
    """Конфигурация приложения"""
    BOT_TOKEN = os.getenv("BOT_TOKEN")
    ADMIN_ID = int(os.getenv("ADMIN_ID", 0))
    DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///bot.db")
    
    # Настройки тарифа
    TARIFF_NAME = "PRO"
    TARIFF_PRICE = "299 звезд"
    TARIFF_CHANNELS_LIMIT = 2
    TARIFF_POSTS_PER_DAY = 8
    PAYMENT_LINK = os.getenv("PAYMENT_LINK", "https://t.me/your_channel")
    
    # Проверка обязательных переменных
    @classmethod
    def validate(cls):
        if not cls.BOT_TOKEN:
            raise ValueError("BOT_TOKEN не указан в переменных окружения")
        if cls.ADMIN_ID == 0:
            raise ValueError("ADMIN_ID не указан в переменных окружения")
        return True

# Валидируем конфигурацию
try:
    Config.validate()
except ValueError as e:
    logger.error(f"Ошибка конфигурации: {e}")
    sys.exit(1)

# ========== ИНИЦИАЛИЗАЦИЯ БОТА ==========
bot = Bot(token=Config.BOT_TOKEN, parse_mode=ParseMode.HTML)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)
router = Router()
dp.include_router(router)

# ========== БАЗА ДАННЫХ PostgreSQL ==========
class Database:
    """Класс для работы с PostgreSQL"""
    
    def __init__(self):
        self.pool: Optional[Pool] = None
        self.is_sqlite = False
        
    async def connect(self):
        """Подключение к базе данных"""
        try:
            if Config.DATABASE_URL.startswith("postgres"):
                # PostgreSQL на Railway
                self.pool = await asyncpg.create_pool(
                    Config.DATABASE_URL,
                    min_size=5,
                    max_size=20,
                    command_timeout=60
                )
                logger.info("✅ Подключено к PostgreSQL")
                await self._create_tables_pg()
            else:
                # SQLite для разработки
                import aiosqlite
                self.is_sqlite = True
                self.conn = await aiosqlite.connect("bot_database.db")
                logger.info("✅ Подключено к SQLite")
                await self._create_tables_sqlite()
                
        except Exception as e:
            logger.error(f"❌ Ошибка подключения к БД: {e}")
            raise
    
    async def _create_tables_pg(self):
        """Создание таблиц в PostgreSQL"""
        async with self.pool.acquire() as conn:
            # Таблица пользователей
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id SERIAL PRIMARY KEY,
                    telegram_id BIGINT UNIQUE NOT NULL,
                    username VARCHAR(255),
                    full_name TEXT,
                    channels_limit INTEGER DEFAULT 1,
                    posts_per_day_limit INTEGER DEFAULT 3,
                    subscribed BOOLEAN DEFAULT FALSE,
                    subscription_until TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Таблица каналов
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS channels (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
                    channel_id VARCHAR(255) NOT NULL,
                    channel_title TEXT,
                    is_active BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(user_id, channel_id)
                )
            """)
            
            # Таблица запланированных постов
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS scheduled_posts (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
                    channel_id VARCHAR(255),
                    message_text TEXT NOT NULL,
                    photo_id TEXT,
                    scheduled_time TIMESTAMP NOT NULL,
                    is_published BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    published_at TIMESTAMP
                )
            """)
            
            # Таблица рассылок
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS broadcasts (
                    id SERIAL PRIMARY KEY,
                    message_text TEXT NOT NULL,
                    sent_count INTEGER DEFAULT 0,
                    total_count INTEGER DEFAULT 0,
                    sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Создание индексов для производительности
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_users_telegram_id 
                ON users(telegram_id)
            """)
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_scheduled_posts_time 
                ON scheduled_posts(scheduled_time) WHERE is_published = FALSE
            """)
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_channels_user 
                ON channels(user_id, is_active)
            """)
            
            logger.info("✅ Таблицы созданы/проверены")
    
    async def _create_tables_sqlite(self):
        """Создание таблиц в SQLite"""
        await self.conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                telegram_id INTEGER UNIQUE NOT NULL,
                username TEXT,
                full_name TEXT,
                channels_limit INTEGER DEFAULT 1,
                posts_per_day_limit INTEGER DEFAULT 3,
                subscribed BOOLEAN DEFAULT FALSE,
                subscription_until DATETIME,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        await self.conn.execute("""
            CREATE TABLE IF NOT EXISTS channels (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
                channel_id TEXT NOT NULL,
                channel_title TEXT,
                is_active BOOLEAN DEFAULT TRUE,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(user_id, channel_id)
            )
        """)
        
        await self.conn.execute("""
            CREATE TABLE IF NOT EXISTS scheduled_posts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
                channel_id TEXT,
                message_text TEXT NOT NULL,
                photo_id TEXT,
                scheduled_time DATETIME NOT NULL,
                is_published BOOLEAN DEFAULT FALSE,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                published_at DATETIME
            )
        """)
        
        await self.conn.execute("""
            CREATE TABLE IF NOT EXISTS broadcasts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                message_text TEXT NOT NULL,
                sent_count INTEGER DEFAULT 0,
                total_count INTEGER DEFAULT 0,
                sent_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        await self.conn.commit()
    
    # ========== МЕТОДЫ ДЛЯ ПОЛЬЗОВАТЕЛЕЙ ==========
    async def get_or_create_user(self, telegram_id: int, username: str = None, full_name: str = None) -> Optional[Dict[str, Any]]:
        """Получить или создать пользователя"""
        try:
            if not self.is_sqlite and self.pool:
                async with self.pool.acquire() as conn:
                    # Пробуем найти пользователя
                    user = await conn.fetchrow(
                        "SELECT * FROM users WHERE telegram_id = $1",
                        telegram_id
                    )
                    
                    if not user:
                        # Создаем нового пользователя
                        user = await conn.fetchrow(
                            """
                            INSERT INTO users (telegram_id, username, full_name)
                            VALUES ($1, $2, $3)
                            RETURNING *
                            """,
                            telegram_id, username, full_name
                        )
                    else:
                        # Обновляем username если изменился
                        if username and user['username'] != username:
                            await conn.execute(
                                "UPDATE users SET username = $1, updated_at = CURRENT_TIMESTAMP WHERE id = $2",
                                username, user['id']
                            )
                    
                    return dict(user) if user else None
                    
            else:
                # SQLite версия
                cursor = await self.conn.execute(
                    "SELECT * FROM users WHERE telegram_id = ?",
                    (telegram_id,)
                )
                user = await cursor.fetchone()
                
                if not user:
                    cursor = await self.conn.execute(
                        """
                        INSERT INTO users (telegram_id, username, full_name)
                        VALUES (?, ?, ?)
                        """,
                        (telegram_id, username, full_name)
                    )
                    await self.conn.commit()
                    
                    cursor = await self.conn.execute(
                        "SELECT * FROM users WHERE telegram_id = ?",
                        (telegram_id,)
                    )
                    user = await cursor.fetchone()
                
                if user:
                    columns = [desc[0] for desc in cursor.description]
                    return dict(zip(columns, user))
                    
        except Exception as e:
            logger.error(f"Ошибка в get_or_create_user: {e}")
            return None
    
    async def update_user_subscription(self, telegram_id: int, subscribed: bool = True, days: int = 30) -> bool:
        """Обновить подписку пользователя"""
        try:
            subscription_until = datetime.now() + timedelta(days=days)
            
            if not self.is_sqlite and self.pool:
                async with self.pool.acquire() as conn:
                    result = await conn.execute(
                        """
                        UPDATE users 
                        SET subscribed = $1, 
                            subscription_until = $2,
                            channels_limit = $3,
                            posts_per_day_limit = $4,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE telegram_id = $5
                        """,
                        subscribed, subscription_until,
                        Config.TARIFF_CHANNELS_LIMIT, Config.TARIFF_POSTS_PER_DAY,
                        telegram_id
                    )
                    return result == "UPDATE 1"
            else:
                await self.conn.execute(
                    """
                    UPDATE users 
                    SET subscribed = ?, 
                        subscription_until = ?,
                        channels_limit = ?,
                        posts_per_day_limit = ?,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE telegram_id = ?
                    """,
                    (subscribed, subscription_until,
                     Config.TARIFF_CHANNELS_LIMIT, Config.TARIFF_POSTS_PER_DAY,
                     telegram_id)
                )
                await self.conn.commit()
                return True
                
        except Exception as e:
            logger.error(f"Ошибка в update_user_subscription: {e}")
            return False
    
    async def get_user_stats(self, telegram_id: int) -> Optional[Dict[str, Any]]:
        """Получить статистику пользователя"""
        try:
            user = await self.get_or_create_user(telegram_id)
            if not user:
                return None
            
            if not self.is_sqlite and self.pool:
                async with self.pool.acquire() as conn:
                    # Количество каналов
                    channels_count = await conn.fetchval(
                        "SELECT COUNT(*) FROM channels WHERE user_id = $1 AND is_active = TRUE",
                        user['id']
                    )
                    
                    # Посты на сегодня
                    posts_today = await conn.fetchval(
                        """
                        SELECT COUNT(*) FROM scheduled_posts 
                        WHERE user_id = $1 
                        AND DATE(scheduled_time) = CURRENT_DATE
                        AND is_published = FALSE
                        """,
                        user['id']
                    )
            else:
                # SQLite версия
                cursor = await self.conn.execute(
                    "SELECT COUNT(*) FROM channels WHERE user_id = ? AND is_active = TRUE",
                    (user['id'],)
                )
                channels_count = (await cursor.fetchone())[0]
                
                cursor = await self.conn.execute(
                    """
                    SELECT COUNT(*) FROM scheduled_posts 
                    WHERE user_id = ? 
                    AND DATE(scheduled_time) = DATE('now')
                    AND is_published = 0
                    """,
                    (user['id'],)
                )
                posts_today = (await cursor.fetchone())[0]
            
            return {
                'user': user,
                'channels_count': channels_count or 0,
                'posts_today': posts_today or 0,
                'channels_limit': user.get('channels_limit', 1),
                'posts_limit': user.get('posts_per_day_limit', 3)
            }
            
        except Exception as e:
            logger.error(f"Ошибка в get_user_stats: {e}")
            return None
    
    # ========== МЕТОДЫ ДЛЯ КАНАЛОВ ==========
    async def add_channel(self, user_id: int, channel_id: str, channel_title: str) -> bool:
        """Добавить канал пользователя"""
        try:
            if not self.is_sqlite and self.pool:
                async with self.pool.acquire() as conn:
                    await conn.execute(
                        """
                        INSERT INTO channels (user_id, channel_id, channel_title)
                        VALUES ($1, $2, $3)
                        ON CONFLICT (user_id, channel_id) 
                        DO UPDATE SET is_active = TRUE, channel_title = EXCLUDED.channel_title
                        """,
                        user_id, channel_id, channel_title
                    )
            else:
                await self.conn.execute(
                    """
                    INSERT OR REPLACE INTO channels (user_id, channel_id, channel_title, is_active)
                    VALUES (?, ?, ?, TRUE)
                    """,
                    (user_id, channel_id, channel_title)
                )
                await self.conn.commit()
            
            return True
            
        except Exception as e:
            logger.error(f"Ошибка в add_channel: {e}")
            return False
    
    async def get_user_channels(self, user_id: int) -> List[Dict[str, Any]]:
        """Получить каналы пользователя"""
        try:
            if not self.is_sqlite and self.pool:
                async with self.pool.acquire() as conn:
                    channels = await conn.fetch(
                        "SELECT * FROM channels WHERE user_id = $1 AND is_active = TRUE ORDER BY created_at",
                        user_id
                    )
                    return [dict(channel) for channel in channels]
            else:
                cursor = await self.conn.execute(
                    "SELECT * FROM channels WHERE user_id = ? AND is_active = TRUE ORDER BY created_at",
                    (user_id,)
                )
                rows = await cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                return [dict(zip(columns, row)) for row in rows]
                
        except Exception as e:
            logger.error(f"Ошибка в get_user_channels: {e}")
            return []
    
    # ========== МЕТОДЫ ДЛЯ ПОСТОВ ==========
    async def add_scheduled_post(self, user_id: int, channel_id: str, message_text: str, 
                                scheduled_time: datetime, photo_id: str = None) -> Optional[int]:
        """Добавить запланированный пост"""
        try:
            if not self.is_sqlite and self.pool:
                async with self.pool.acquire() as conn:
                    post_id = await conn.fetchval(
                        """
                        INSERT INTO scheduled_posts 
                        (user_id, channel_id, message_text, photo_id, scheduled_time)
                        VALUES ($1, $2, $3, $4, $5)
                        RETURNING id
                        """,
                        user_id, channel_id, message_text, photo_id, scheduled_time
                    )
                    return post_id
            else:
                cursor = await self.conn.execute(
                    """
                    INSERT INTO scheduled_posts 
                    (user_id, channel_id, message_text, photo_id, scheduled_time)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (user_id, channel_id, message_text, photo_id, scheduled_time)
                )
                await self.conn.commit()
                return cursor.lastrowid
                
        except Exception as e:
            logger.error(f"Ошибка в add_scheduled_post: {e}")
            return None
    
    async def get_todays_posts(self, user_id: int) -> List[Dict[str, Any]]:
        """Получить сегодняшние посты пользователя"""
        try:
            if not self.is_sqlite and self.pool:
                async with self.pool.acquire() as conn:
                    posts = await conn.fetch(
                        """
                        SELECT * FROM scheduled_posts 
                        WHERE user_id = $1 
                        AND DATE(scheduled_time) = CURRENT_DATE
                        AND is_published = FALSE
                        ORDER BY scheduled_time
                        """,
                        user_id
                    )
                    return [dict(post) for post in posts]
            else:
                cursor = await self.conn.execute(
                    """
                    SELECT * FROM scheduled_posts 
                    WHERE user_id = ? 
                    AND DATE(scheduled_time) = DATE('now')
                    AND is_published = 0
                    ORDER BY scheduled_time
                    """,
                    (user_id,)
                )
                rows = await cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                return [dict(zip(columns, row)) for row in rows]
                
        except Exception as e:
            logger.error(f"Ошибка в get_todays_posts: {e}")
            return []
    
    async def get_posts_to_publish(self) -> List[Dict[str, Any]]:
        """Получить посты для публикации"""
        try:
            if not self.is_sqlite and self.pool:
                async with self.pool.acquire() as conn:
                    posts = await conn.fetch(
                        """
                        SELECT sp.*, u.telegram_id, c.channel_id as channel_ident
                        FROM scheduled_posts sp
                        JOIN users u ON sp.user_id = u.id
                        JOIN channels c ON sp.channel_id = c.channel_id AND c.user_id = u.id
                        WHERE sp.scheduled_time <= NOW() + INTERVAL '5 minutes'
                        AND sp.is_published = FALSE
                        AND c.is_active = TRUE
                        ORDER BY sp.scheduled_time
                        """,
                    )
                    return [dict(post) for post in posts]
            else:
                cursor = await self.conn.execute(
                    """
                    SELECT sp.*, u.telegram_id, c.channel_id as channel_ident
                    FROM scheduled_posts sp
                    JOIN users u ON sp.user_id = u.id
                    JOIN channels c ON sp.channel_id = c.channel_id AND c.user_id = u.id
                    WHERE sp.scheduled_time <= datetime('now', '+5 minutes')
                    AND sp.is_published = 0
                    AND c.is_active = 1
                    ORDER BY sp.scheduled_time
                    """
                )
                rows = await cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                return [dict(zip(columns, row)) for row in rows]
                
        except Exception as e:
            logger.error(f"Ошибка в get_posts_to_publish: {e}")
            return []
    
    async def mark_post_published(self, post_id: int) -> bool:
        """Отметить пост как опубликованный"""
        try:
            if not self.is_sqlite and self.pool:
                async with self.pool.acquire() as conn:
                    await conn.execute(
                        """
                        UPDATE scheduled_posts 
                        SET is_published = TRUE, published_at = CURRENT_TIMESTAMP
                        WHERE id = $1
                        """,
                        post_id
                    )
            else:
                await self.conn.execute(
                    """
                    UPDATE scheduled_posts 
                    SET is_published = 1, published_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                    """,
                    (post_id,)
                )
                await self.conn.commit()
            return True
            
        except Exception as e:
            logger.error(f"Ошибка в mark_post_published: {e}")
            return False
    
    # ========== АДМИН МЕТОДЫ ==========
    async def get_all_users(self) -> List[Dict[str, Any]]:
        """Получить всех пользователей"""
        try:
            if not self.is_sqlite and self.pool:
                async with self.pool.acquire() as conn:
                    users = await conn.fetch("SELECT * FROM users ORDER BY created_at DESC")
                    return [dict(user) for user in users]
            else:
                cursor = await self.conn.execute("SELECT * FROM users ORDER BY created_at DESC")
                rows = await cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                return [dict(zip(columns, row)) for row in rows]
                
        except Exception as e:
            logger.error(f"Ошибка в get_all_users: {e}")
            return []
    
    async def get_subscribed_users(self) -> List[Dict[str, Any]]:
        """Получить пользователей с подпиской"""
        try:
            if not self.is_sqlite and self.pool:
                async with self.pool.acquire() as conn:
                    users = await conn.fetch(
                        "SELECT * FROM users WHERE subscribed = TRUE ORDER BY subscription_until DESC"
                    )
                    return [dict(user) for user in users]
            else:
                cursor = await self.conn.execute(
                    "SELECT * FROM users WHERE subscribed = 1 ORDER BY subscription_until DESC"
                )
                rows = await cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                return [dict(zip(columns, row)) for row in rows]
                
        except Exception as e:
            logger.error(f"Ошибка в get_subscribed_users: {e}")
            return []
    
    async def save_broadcast(self, message_text: str) -> Optional[int]:
        """Сохранить рассылку"""
        try:
            if not self.is_sqlite and self.pool:
                async with self.pool.acquire() as conn:
                    broadcast_id = await conn.fetchval(
                        "INSERT INTO broadcasts (message_text, total_count) VALUES ($1, (SELECT COUNT(*) FROM users)) RETURNING id",
                        message_text
                    )
                    return broadcast_id
            else:
                cursor = await self.conn.execute(
                    "INSERT INTO broadcasts (message_text, total_count) VALUES (?, (SELECT COUNT(*) FROM users))",
                    (message_text,)
                )
                await self.conn.commit()
                return cursor.lastrowid
                
        except Exception as e:
            logger.error(f"Ошибка в save_broadcast: {e}")
            return None
    
    async def update_broadcast_stats(self, broadcast_id: int, sent_count: int) -> bool:
        """Обновить статистику рассылки"""
        try:
            if not self.is_sqlite and self.pool:
                async with self.pool.acquire() as conn:
                    await conn.execute(
                        "UPDATE broadcasts SET sent_count = $1 WHERE id = $2",
                        sent_count, broadcast_id
                    )
            else:
                await self.conn.execute(
                    "UPDATE broadcasts SET sent_count = ? WHERE id = ?",
                    (sent_count, broadcast_id)
                )
                await self.conn.commit()
            return True
            
        except Exception as e:
            logger.error(f"Ошибка в update_broadcast_stats: {e}")
            return False
    
    async def close(self):
        """Закрыть соединение с БД"""
        try:
            if not self.is_sqlite and self.pool:
                await self.pool.close()
            elif self.is_sqlite:
                await self.conn.close()
        except Exception as e:
            logger.error(f"Ошибка при закрытии БД: {e}")

# Инициализация БД
db = Database()

# ========== ПЛАНИРОВЩИК ==========
scheduler = AsyncIOScheduler(
    jobstores={'default': MemoryJobStore()},
    timezone='UTC'
)

# ========== СОСТОЯНИЯ FSM ==========
class AddChannelStates(StatesGroup):
    waiting_for_channel_link = State()

class SchedulePostStates(StatesGroup):
    waiting_for_channel = State()
    waiting_for_text = State()
    waiting_for_time = State()

class AdminBroadcastStates(StatesGroup):
    waiting_for_message = State()

class AdminAddSubscriptionStates(StatesGroup):
    waiting_for_user_id = State()

# ========== КЛАВИАТУРЫ ==========
def get_main_keyboard(user_id: int = 0, has_subscription: bool = False) -> ReplyKeyboardMarkup:
    """Основная клавиатура"""
    builder = ReplyKeyboardBuilder()
    
    builder.row(
        KeyboardButton(text="📊 Моя статистика"),
        KeyboardButton(text="📢 Мои каналы")
    )
    builder.row(
        KeyboardButton(text="➕ Добавить канал"),
        KeyboardButton(text="🕐 Запланировать пост")
    )
    builder.row(KeyboardButton(text="📅 Мои запланированные посты"))
    
    if not has_subscription:
        builder.row(KeyboardButton(text=f"💎 Купить {Config.TARIFF_NAME}"))
    else:
        builder.row(KeyboardButton(text="✅ Подписка активна"))
    
    if user_id == Config.ADMIN_ID:
        builder.row(KeyboardButton(text="👑 Админ панель"))
    
    return builder.as_markup(resize_keyboard=True)

def get_admin_keyboard() -> InlineKeyboardMarkup:
    """Админ клавиатура"""
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="📢 Рассылка", callback_data="admin_broadcast"),
        InlineKeyboardButton(text="👥 Все пользователи", callback_data="admin_users")
    )
    builder.row(
        InlineKeyboardButton(text="⭐ Подписчики", callback_data="admin_subscribers"),
        InlineKeyboardButton(text="➕ Выдать подписку", callback_data="admin_add_subscription")
    )
    builder.row(
        InlineKeyboardButton(text="📊 Статистика", callback_data="admin_stats"),
        InlineKeyboardButton(text="🔄 Обновить", callback_data="admin_refresh")
    )
    builder.row(InlineKeyboardButton(text="🔙 Назад", callback_data="admin_back"))
    return builder.as_markup()

def get_channels_keyboard(channels: List[Dict[str, Any]]) -> InlineKeyboardMarkup:
    """Клавиатура с каналами"""
    builder = InlineKeyboardBuilder()
    
    for channel in channels:
        title = channel.get('channel_title', 'Канал')[:20]
        builder.row(InlineKeyboardButton(
            text=f"📢 {title}",
            callback_data=f"channel_select_{channel.get('channel_id')}"
        ))
    
    if channels:
        builder.row(InlineKeyboardButton(
            text="🗑 Удалить канал",
            callback_data="channel_delete"
        ))
    
    builder.row(
        InlineKeyboardButton(text="➕ Добавить канал", callback_data="channel_add"),
        InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_main")
    )
    
    return builder.as_markup()

def get_posts_keyboard(posts: List[Dict[str, Any]]) -> InlineKeyboardMarkup:
    """Клавиатура с постами"""
    builder = InlineKeyboardBuilder()
    
    for post in posts:
        time_str = ""
        if post.get('scheduled_time'):
            if isinstance(post['scheduled_time'], str):
                dt = datetime.fromisoformat(post['scheduled_time'].replace('Z', '+00:00'))
            else:
                dt = post['scheduled_time']
            time_str = dt.strftime("%H:%M")
        
        text_preview = post.get('message_text', '')[:15]
        builder.row(InlineKeyboardButton(
            text=f"🕐 {time_str} - {text_preview}...",
            callback_data=f"post_view_{post.get('id')}"
        ))
    
    builder.row(
        InlineKeyboardButton(text="➕ Новый пост", callback_data="post_new"),
        InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_main")
    )
    
    return builder.as_markup()

def get_confirm_keyboard(action: str) -> InlineKeyboardMarkup:
    """Клавиатура подтверждения"""
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="✅ Да", callback_data=f"confirm_{action}"),
        InlineKeyboardButton(text="❌ Нет", callback_data="cancel_action")
    )
    return builder.as_markup()

# ========== ФУНКЦИИ ПОМОЩНИКИ ==========
async def check_bot_admin(channel_id: str) -> bool:
    """Проверить, является ли бот администратором канала"""
    try:
        chat_member = await bot.get_chat_member(chat_id=channel_id, user_id=bot.id)
        return chat_member.status in ['administrator', 'creator'] and chat_member.can_post_messages
    except Exception as e:
        logger.error(f"Ошибка проверки прав бота: {e}")
        return False

async def notify_user(telegram_id: int, message: str) -> bool:
    """Отправить уведомление пользователю"""
    try:
        await bot.send_message(chat_id=telegram_id, text=message)
        return True
    except Exception as e:
        logger.error(f"Не удалось отправить уведомление пользователю {telegram_id}: {e}")
        return False

# ========== ОСНОВНЫЕ КОМАНДЫ ==========
@router.message(Command("start"))
async def cmd_start(message: types.Message):
    """Команда /start"""
    user = await db.get_or_create_user(
        telegram_id=message.from_user.id,
        username=message.from_user.username,
        full_name=message.from_user.full_name
    )
    
    if not user:
        await message.answer("❌ Ошибка при регистрации. Попробуйте позже.")
        return
    
    welcome_text = (
        f"👋 <b>Привет, {message.from_user.full_name or 'друг'}!</b>\n\n"
        f"Я бот для автоматической публикации постов в Telegram каналах.\n\n"
        f"<b>📊 Бесплатный тариф:</b>\n"
        f"• 1 канал\n"
        f"• 3 поста в день\n\n"
        f"<b>💎 Тариф {Config.TARIFF_NAME}:</b>\n"
        f"• {Config.TARIFF_CHANNELS_LIMIT} канала\n"
        f"• {Config.TARIFF_POSTS_PER_DAY} постов в день\n"
        f"• Цена: {Config.TARIFF_PRICE}\n\n"
        f"<i>Используйте кнопки ниже для управления</i>"
    )
    
    has_subscription = user.get('subscribed', False)
    keyboard = get_main_keyboard(message.from_user.id, has_subscription)
    
    await message.answer(welcome_text, reply_markup=keyboard)

@router.message(Command("help"))
async def cmd_help(message: types.Message):
    """Команда /help"""
    help_text = (
        "🆘 <b>Помощь по боту</b>\n\n"
        "<b>Основные команды:</b>\n"
        "• /start - Начать работу с ботом\n"
        "• /help - Показать это сообщение\n"
        "• /admin - Админ панель (только для админа)\n\n"
        
        "<b>Как добавить канал:</b>\n"
        "1. Добавьте бота @ваш_бот администратором в канал\n"
        "2. Дайте права на отправку сообщений\n"
        "3. Нажмите '➕ Добавить канал' в боте\n"
        "4. Перешлите любое сообщение из канала\n\n"
        
        "<b>Как запланировать пост:</b>\n"
        "1. Нажмите '🕐 Запланировать пост'\n"
        "2. Выберите канал\n"
        "3. Отправьте текст поста\n"
        "4. Укажите время в формате ЧЧ:ММ\n\n"
        
        "<b>Проблемы?</b>\n"
        "Если бот не публикует посты:\n"
        "1. Проверьте, что бот все еще администратор\n"
        "2. Проверьте, что у бота есть права на отправку\n"
        "3. Перезапустите бота командой /start"
    )
    
    await message.answer(help_text)

@router.message(Command("admin"))
async def cmd_admin(message: types.Message):
    """Админ панель"""
    if message.from_user.id != Config.ADMIN_ID:
        await message.answer("⛔ У вас нет доступа к админ панели!")
        return
    
    admin_text = (
        f"👑 <b>Админ панель</b>\n\n"
        f"Администратор: {message.from_user.full_name}\n"
        f"ID: {message.from_user.id}\n\n"
        f"<i>Выберите действие:</i>"
    )
    
    await message.answer(admin_text, reply_markup=get_admin_keyboard())

@router.message(Command("health"))
async def cmd_health(message: types.Message):
    """Проверка здоровья бота"""
    health_text = (
        "✅ <b>Бот работает нормально!</b>\n\n"
        f"<b>Время сервера:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"<b>Версия Python:</b> {sys.version.split()[0]}\n"
        f"<b>Пользователей в БД:</b> {len(await db.get_all_users())}\n"
        f"<b>Запланированных постов:</b> {len(await db.get_posts_to_publish())}"
    )
    
    await message.answer(health_text)

# ========== ОБРАБОТЧИКИ ТЕКСТОВЫХ СООБЩЕНИЙ ==========
@router.message(F.text == "📊 Моя статистика")
async def handle_stats(message: types.Message):
    """Показать статистику пользователя"""
    stats = await db.get_user_stats(message.from_user.id)
    
    if not stats:
        await message.answer("❌ Не удалось получить статистику. Попробуйте позже.")
        return
    
    user = stats['user']
    
    # Форматирование даты подписки
    subscription_text = "❌ Нет подписки"
    if user.get('subscribed') and user.get('subscription_until'):
        try:
            if isinstance(user['subscription_until'], str):
                until_date = datetime.fromisoformat(user['subscription_until'].replace('Z', '+00:00'))
            else:
                until_date = user['subscription_until']
            subscription_text = f"✅ До {until_date.strftime('%d.%m.%Y')}"
        except:
            subscription_text = "✅ Активна"
    
    stats_text = (
        f"📊 <b>Ваша статистика</b>\n\n"
        f"<b>👤 Профиль:</b>\n"
        f"• Имя: {user.get('full_name', 'Не указано')}\n"
        f"• Username: @{user.get('username', 'нет')}\n"
        f"• Подписка: {subscription_text}\n\n"
        
        f"<b>📈 Лимиты:</b>\n"
        f"• Каналы: {stats['channels_count']}/{stats['channels_limit']}\n"
        f"• Посты сегодня: {stats['posts_today']}/{stats['posts_limit']}\n\n"
    )
    
    if stats['channels_count'] >= stats['channels_limit']:
        stats_text += "⚠️ <i>Лимит каналов достигнут</i>\n"
    if stats['posts_today'] >= stats['posts_limit']:
        stats_text += "⚠️ <i>Лимит постов на сегодня достигнут</i>\n"
    
    await message.answer(stats_text)

@router.message(F.text == "📢 Мои каналы")
async def handle_channels(message: types.Message):
    """Показать каналы пользователя"""
    user = await db.get_or_create_user(message.from_user.id)
    if not user:
        await message.answer("❌ Пользователь не найден.")
        return
    
    channels = await db.get_user_channels(user['id'])
    
    if not channels:
        await message.answer(
            "📭 <b>У вас пока нет каналов</b>\n\n"
            "Нажмите '➕ Добавить канал' чтобы добавить первый канал!",
            reply_markup=get_channels_keyboard([])
        )
        return
    
    channels_text = "📢 <b>Ваши каналы:</b>\n\n"
    for i, channel in enumerate(channels, 1):
        channels_text += f"{i}. <b>{channel.get('channel_title', 'Без названия')}</b>\n"
    
    await message.answer(channels_text, reply_markup=get_channels_keyboard(channels))

@router.message(F.text == "➕ Добавить канал")
async def handle_add_channel(message: types.Message, state: FSMContext):
    """Начать добавление канала"""
    stats = await db.get_user_stats(message.from_user.id)
    if not stats:
        await message.answer("❌ Ошибка получения данных.")
        return
    
    # Проверка лимита каналов
    if stats['channels_count'] >= stats['channels_limit']:
        await message.answer(
            f"❌ <b>Достигнут лимит каналов!</b>\n\n"
            f"У вас {stats['channels_count']}/{stats['channels_limit']} каналов.\n"
            f"Для добавления большего количества приобретите тариф {Config.TARIFF_NAME}.\n\n"
            f"Цена: {Config.TARIFF_PRICE}"
        )
        return
    
    await state.set_state(AddChannelStates.waiting_for_channel_link)
    
    await message.answer(
        "📝 <b>Добавление канала</b>\n\n"
        "Чтобы добавить канал:\n"
        "1. Добавьте меня администратором в ваш канал\n"
        "2. Дайте права на отправку сообщений\n"
        "3. Перешлите любое сообщение из канала сюда\n\n"
        "<i>Или отправьте @username канала</i>\n\n"
        "❌ <b>Отмена:</b> /cancel"
    )

@router.message(F.text == "🕐 Запланировать пост")
async def handle_schedule_post(message: types.Message, state: FSMContext):
    """Начать планирование поста"""
    stats = await db.get_user_stats(message.from_user.id)
    if not stats:
        await message.answer("❌ Ошибка получения данных.")
        return
    
    # Проверка лимита постов
    if stats['posts_today'] >= stats['posts_limit']:
        await message.answer(
            f"❌ <b>Достигнут лимит постов на сегодня!</b>\n\n"
            f"У вас {stats['posts_today']}/{stats['posts_limit']} постов.\n"
            f"Лимит обновится в 00:00 по UTC.\n\n"
            f"Для увеличения лимита приобретите тариф {Config.TARIFF_NAME}.\n"
            f"Цена: {Config.TARIFF_PRICE}"
        )
        return
    
    # Получаем каналы пользователя
    channels = await db.get_user_channels(stats['user']['id'])
    
    if not channels:
        await message.answer(
            "❌ <b>У вас нет добавленных каналов!</b>\n\n"
            "Сначала добавьте канал через '➕ Добавить канал'"
        )
        return
    
    await state.set_state(SchedulePostStates.waiting_for_channel)
    await state.update_data(user_id=stats['user']['id'])
    
    # Создаем клавиатуру с каналами
    builder = InlineKeyboardBuilder()
    for channel in channels:
        title = channel.get('channel_title', 'Канал')[:20]
        builder.row(InlineKeyboardButton(
            text=title,
            callback_data=f"select_channel_{channel.get('channel_id')}"
        ))
    
    builder.row(InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_schedule"))
    
    await message.answer(
        "📝 <b>Планирование поста</b>\n\n"
        "Выберите канал для публикации:",
        reply_markup=builder.as_markup()
    )

@router.message(F.text == "📅 Мои запланированные посты")
async def handle_scheduled_posts(message: types.Message):
    """Показать запланированные посты"""
    user = await db.get_or_create_user(message.from_user.id)
    if not user:
        await message.answer("❌ Пользователь не найден.")
        return
    
    posts = await db.get_todays_posts(user['id'])
    
    if not posts:
        await message.answer(
            "📭 <b>Нет запланированных постов на сегодня</b>\n\n"
            "Нажмите '🕐 Запланировать пост' чтобы создать первый пост!"
        )
        return
    
    posts_text = "📅 <b>Запланированные посты на сегодня:</b>\n\n"
    for i, post in enumerate(posts, 1):
        time_str = ""
        if post.get('scheduled_time'):
            if isinstance(post['scheduled_time'], str):
                dt = datetime.fromisoformat(post['scheduled_time'].replace('Z', '+00:00'))
            else:
                dt = post['scheduled_time']
            time_str = dt.strftime("%H:%M")
        
        posts_text += f"{i}. <b>{time_str}</b>\n"
        posts_text += f"   {post.get('message_text', '')[:50]}...\n\n"
    
    await message.answer(posts_text, reply_markup=get_posts_keyboard(posts))

@router.message(F.text.startswith(f"💎 Купить {Config.TARIFF_NAME}"))
async def handle_buy_subscription(message: types.Message):
    """Обработка покупки подписки"""
    user = await db.get_or_create_user(message.from_user.id)
    
    if user and user.get('subscribed'):
        await message.answer("✅ У вас уже активна подписка!")
        return
    
    payment_text = (
        f"💎 <b>Тариф {Config.TARIFF_NAME}</b>\n\n"
        f"<b>Преимущества:</b>\n"
        f"• До {Config.TARIFF_CHANNELS_LIMIT} каналов\n"
        f"• До {Config.TARIFF_POSTS_PER_DAY} постов в день\n"
        f"• Приоритетная поддержка\n\n"
        f"<b>Цена:</b> {Config.TARIFF_PRICE}\n\n"
        f"<b>Как купить:</b>\n"
        f"1. Перейдите по ссылке: {Config.PAYMENT_LINK}\n"
        f"2. Оплатите подписку\n"
        f"3. После оплаты отправьте скриншот чека @ваш_админ\n"
        f"4. Админ активирует подписку в течение 24 часов\n\n"
        f"<i>Для теста администратор может активировать подписку вручную через админ панель</i>"
    )
    
    await message.answer(payment_text)

@router.message(F.text == "👑 Админ панель")
async def handle_admin_panel(message: types.Message):
    """Открыть админ панель"""
    await cmd_admin(message)

# ========== FSM ХЕНДЛЕРЫ ==========
@router.message(AddChannelStates.waiting_for_channel_link)
async def process_channel_link(message: types.Message, state: FSMContext):
    """Обработка ссылки на канал"""
    user = await db.get_or_create_user(message.from_user.id)
    if not user:
        await message.answer("❌ Ошибка получения пользователя.")
        await state.clear()
        return
    
    # Получаем channel_id из пересланного сообщения или username
    channel_id = None
    channel_title = "Неизвестный канал"
    
    if message.forward_from_chat:
        # Если переслали из канала
        if message.forward_from_chat.type in ["channel", "supergroup"]:
            channel_id = str(message.forward_from_chat.id)
            channel_title = message.forward_from_chat.title or "Без названия"
    elif message.text and message.text.startswith("@"):
        # Если отправили username
        channel_id = message.text
        channel_title = message.text
    
    if not channel_id:
        await message.answer("❌ Не удалось определить канал. Перешлите сообщение из канала или отправьте @username")
        return
    
    # Проверяем, есть ли бот администратором в канале
    is_admin = await check_bot_admin(channel_id)
    if not is_admin:
        await message.answer(
            "❌ <b>У меня нет прав на отправку сообщений в этом канале!</b>\n\n"
            "Пожалуйста:\n"
            "1. Добавьте меня в канал как администратора\n"
            "2. Дайте права на отправку сообщений\n"
            "3. Попробуйте снова"
        )
        await state.clear()
        return
    
    # Добавляем канал в БД
    success = await db.add_channel(user['id'], channel_id, channel_title)
    
    if success:
        await message.answer(f"✅ <b>Канал добавлен!</b>\n\nНазвание: {channel_title}")
    else:
        await message.answer("❌ Ошибка при добавлении канала. Возможно, он уже добавлен.")
    
    await state.clear()

@router.message(SchedulePostStates.waiting_for_text)
async def process_post_text(message: types.Message, state: FSMContext):
    """Обработка текста поста"""
    if not message.text and not message.caption:
        await message.answer("❌ Пожалуйста, отправьте текстовое сообщение или медиа с подписью!")
        return
    
    message_text = message.text or message.caption or ""
    photo_id = None
    
    if message.photo:
        photo_id = message.photo[-1].file_id
    
    await state.update_data(message_text=message_text, photo_id=photo_id)
    await state.set_state(SchedulePostStates.waiting_for_time)
    
    await message.answer(
        "⏰ <b>Укажите время публикации</b>\n\n"
        "Отправьте время в формате:\n"
        "<code>ЧЧ:ММ</code> (например, <code>14:30</code>)\n\n"
        "Пост будет опубликован сегодня в указанное время по UTC.\n\n"
        "❌ <b>Отмена:</b> /cancel"
    )

@router.message(SchedulePostStates.waiting_for_time)
async def process_post_time(message: types.Message, state: FSMContext):
    """Обработка времени поста"""
    if not message.text:
        await message.answer("❌ Пожалуйста, отправьте время в формате ЧЧ:ММ")
        return
    
    time_str = message.text.strip()
    
    try:
        # Парсим время
        post_time = datetime.strptime(time_str, "%H:%M").time()
        
        # Собираем полную дату (сегодня + указанное время)
        today = datetime.utcnow().date()
        scheduled_datetime = datetime.combine(today, post_time)
        
        # Проверяем, что время в будущем (добавляем 2 минуты буфера)
        if scheduled_datetime < datetime.utcnow() + timedelta(minutes=2):
            await message.answer("❌ Нельзя запланировать пост в прошлом или ближайшие 2 минуты! Укажите будущее время.")
            return
        
        # Проверяем, что не позже чем через 24 часа
        if scheduled_datetime > datetime.utcnow() + timedelta(days=1):
            await message.answer("❌ Можно планировать посты только на ближайшие 24 часа!")
            return
        
    except ValueError:
        await message.answer("❌ Неправильный формат времени! Используйте ЧЧ:ММ (например, 14:30)")
        return
    
    # Получаем данные из состояния
    data = await state.get_data()
    user_id = data.get('user_id')
    channel_id = data.get('channel_id')
    message_text = data.get('message_text')
    photo_id = data.get('photo_id')
    
    if not all([user_id, channel_id, message_text]):
        await message.answer("❌ Ошибка данных. Начните заново.")
        await state.clear()
        return
    
    # Добавляем пост в БД
    post_id = await db.add_scheduled_post(
        user_id=user_id,
        channel_id=channel_id,
        message_text=message_text,
        scheduled_time=scheduled_datetime,
        photo_id=photo_id
    )
    
    if not post_id:
        await message.answer("❌ Ошибка при сохранении поста. Попробуйте позже.")
        await state.clear()
        return
    
    # Планируем задачу
    scheduler.add_job(
        publish_scheduled_post,
        'date',
        run_date=scheduled_datetime,
        args=[post_id],
        id=f"post_{post_id}",
        replace_existing=True
    )
    
    time_formatted = scheduled_datetime.strftime("%H:%M UTC")
    success_text = (
        f"✅ <b>Пост успешно запланирован!</b>\n\n"
        f"<b>Время публикации:</b> {time_formatted}\n"
        f"<b>Текст:</b>\n{message_text[:100]}..."
    )
    
    if photo_id:
        success_text += "\n<b>Медиа:</b> Фото"
    
    await message.answer(success_text)
    await state.clear()

# ========== CALLBACK ОБРАБОТЧИКИ ==========
@router.callback_query(F.data.startswith("select_channel_"))
async def callback_select_channel(callback: types.CallbackQuery, state: FSMContext):
    """Выбор канала для поста"""
    channel_id = callback.data.replace("select_channel_", "")
    await state.update_data(channel_id=channel_id)
    await state.set_state(SchedulePostStates.waiting_for_text)
    
    await callback.message.edit_text(
        "📝 <b>Планирование поста</b>\n\n"
        "Отправьте текст поста (можно с хештегами и форматированием):\n\n"
        "<i>Поддерживается HTML разметка. Можно отправить фото с подписью.</i>\n\n"
        "❌ <b>Отмена:</b> /cancel"
    )
    await callback.answer()

@router.callback_query(F.data == "cancel_schedule")
async def callback_cancel_schedule(callback: types.CallbackQuery, state: FSMContext):
    """Отмена планирования"""
    await state.clear()
    await callback.message.edit_text("❌ Планирование отменено.")
    await callback.answer()

# ========== АДМИН CALLBACK ОБРАБОТЧИКИ ==========
@router.callback_query(F.data == "admin_broadcast")
async def callback_admin_broadcast(callback: types.CallbackQuery, state: FSMContext):
    """Начать рассылку"""
    if callback.from_user.id != Config.ADMIN_ID:
        await callback.answer("⛔ Нет доступа!", show_alert=True)
        return
    
    await state.set_state(AdminBroadcastStates.waiting_for_message)
    await callback.message.edit_text(
        "📢 <b>Рассылка сообщения</b>\n\n"
        "Отправьте сообщение для рассылки всем пользователям:\n\n"
        "<i>Поддерживается текст, фото, видео и другие медиа</i>\n\n"
        "❌ <b>Отмена:</b> /cancel"
    )
    await callback.answer()

@router.message(AdminBroadcastStates.waiting_for_message)
async def admin_broadcast_send(message: types.Message, state: FSMContext):
    """Отправить рассылку"""
    if message.from_user.id != Config.ADMIN_ID:
        await message.answer("⛔ Нет доступа!")
        await state.clear()
        return
    
    all_users = await db.get_all_users()
    if not all_users:
        await message.answer("❌ В базе нет пользователей для рассылки.")
        await state.clear()
        return
    
    # Сохраняем рассылку в БД
    message_text = message.text or message.caption or "Медиа-сообщение"
    broadcast_id = await db.save_broadcast(message_text)
    
    if not broadcast_id:
        await message.answer("❌ Ошибка при сохранении рассылки.")
        await state.clear()
        return
    
    sent_count = 0
    failed_count = 0
    
    progress_msg = await message.answer(f"📤 Начинаю рассылку для {len(all_users)} пользователей...")
    
    for user in all_users:
        try:
            telegram_id = user.get('telegram_id')
            if not telegram_id:
                continue
            
            # Пробуем отправить сообщение
            if message.text:
                await bot.send_message(
                    chat_id=telegram_id,
                    text=message.text,
                    parse_mode=ParseMode.HTML
                )
            elif message.photo:
                await bot.send_photo(
                    chat_id=telegram_id,
                    photo=message.photo[-1].file_id,
                    caption=message.caption,
                    parse_mode=ParseMode.HTML
                )
            elif message.video:
                await bot.send_video(
                    chat_id=telegram_id,
                    video=message.video.file_id,
                    caption=message.caption,
                    parse_mode=ParseMode.HTML
                )
            
            sent_count += 1
            
            # Обновляем прогресс каждые 10 пользователей
            if sent_count % 10 == 0:
                try:
                    await progress_msg.edit_text(
                        f"📤 Рассылка: {sent_count}/{len(all_users)} отправлено..."
                    )
                except:
                    pass
            
            await asyncio.sleep(0.1)  # Защита от лимитов
            
        except Exception as e:
            failed_count += 1
            logger.error(f"Не удалось отправить пользователю {user.get('telegram_id')}: {e}")
    
    # Обновляем статистику рассылки
    await db.update_broadcast_stats(broadcast_id, sent_count)
    
    result_text = (
        f"✅ <b>Рассылка завершена!</b>\n\n"
        f"<b>Всего пользователей:</b> {len(all_users)}\n"
        f"<b>✅ Успешно отправлено:</b> {sent_count}\n"
        f"<b>❌ Не удалось отправить:</b> {failed_count}\n\n"
        f"<i>ID рассылки: {broadcast_id}</i>"
    )
    
    await progress_msg.edit_text(result_text)
    await state.clear()

@router.callback_query(F.data == "admin_add_subscription")
async def callback_admin_add_subscription(callback: types.CallbackQuery, state: FSMContext):
    """Добавить подписку пользователю"""
    if callback.from_user.id != Config.ADMIN_ID:
        await callback.answer("⛔ Нет доступа!", show_alert=True)
        return
    
    await state.set_state(AdminAddSubscriptionStates.waiting_for_user_id)
    await callback.message.edit_text(
        "⭐ <b>Выдать подписку</b>\n\n"
        "Отправьте Telegram ID пользователя, которому нужно выдать подписку.\n\n"
        "<i>Чтобы узнать ID пользователя, используйте команду /id в боте или посмотрите в списке пользователей</i>\n\n"
        "❌ <b>Отмена:</b> /cancel"
    )
    await callback.answer()

@router.message(AdminAddSubscriptionStates.waiting_for_user_id)
async def admin_add_subscription_process(message: types.Message, state: FSMContext):
    """Обработка выдачи подписки"""
    if message.from_user.id != Config.ADMIN_ID:
        await message.answer("⛔ Нет доступа!")
        await state.clear()
        return
    
    try:
        user_id = int(message.text)
    except ValueError:
        await message.answer("❌ Неверный формат ID! Отправьте числовой ID.")
        return
    
    # Находим пользователя
    user = await db.get_or_create_user(user_id)
    
    if not user:
        await message.answer(f"❌ Пользователь с ID {user_id} не найден в базе!")
        return
    
    if user.get('subscribed'):
        await message.answer(f"✅ У пользователя {user_id} уже есть активная подписка!")
        return
    
    # Выдаем подписку
    success = await db.update_user_subscription(user_id, subscribed=True, days=30)
    
    if not success:
        await message.answer("❌ Ошибка при выдаче подписки.")
        await state.clear()
        return
    
    # Уведомляем пользователя
    user_notified = await notify_user(
        user_id,
        f"🎉 <b>Поздравляем!</b>\n\n"
        f"Вам была активирована подписка {Config.TARIFF_NAME}!\n\n"
        f"<b>Теперь вам доступно:</b>\n"
        f"• {Config.TARIFF_CHANNELS_LIMIT} каналов\n"
        f"• {Config.TARIFF_POSTS_PER_DAY} постов в день\n\n"
        f"Подписка действительна 30 дней.\n\n"
        f"<i>Перезапустите бота командой /start для обновления меню</i>"
    )
    
    notification_status = "✅ Пользователь уведомлен" if user_notified else "⚠️ Не удалось уведомить"
    
    await message.answer(
        f"✅ <b>Подписка успешно выдана!</b>\n\n"
        f"<b>ID пользователя:</b> {user_id}\n"
        f"<b>Имя:</b> {user.get('full_name', 'Не указано')}\n"
        f"<b>Username:</b> @{user.get('username', 'нет')}\n\n"
        f"{notification_status}"
    )
    
    await state.clear()

@router.callback_query(F.data == "admin_users")
async def callback_admin_users(callback: types.CallbackQuery):
    """Показать всех пользователей"""
    if callback.from_user.id != Config.ADMIN_ID:
        await callback.answer("⛔ Нет доступа!", show_alert=True)
        return
    
    users = await db.get_all_users()
    
    if not users:
        await callback.message.edit_text("📭 <b>В базе нет пользователей</b>")
        return
    
    text = f"👥 <b>Все пользователи:</b> {len(users)}\n\n"
    
    for i, user in enumerate(users[:20], 1):  # Показываем первые 20
        status = "⭐" if user.get('subscribed') else "👤"
        username = f"@{user.get('username')}" if user.get('username') else "без username"
        text += f"{i}. {status} ID: {user.get('telegram_id')} | {username}\n"
    
    if len(users) > 20:
        text += f"\n...и еще {len(users) - 20} пользователей"
    
    await callback.message.edit_text(text)
    await callback.answer()

@router.callback_query(F.data == "admin_subscribers")
async def callback_admin_subscribers(callback: types.CallbackQuery):
    """Показать подписчиков"""
    if callback.from_user.id != Config.ADMIN_ID:
        await callback.answer("⛔ Нет доступа!", show_alert=True)
        return
    
    subscribers = await db.get_subscribed_users()
    
    if not subscribers:
        await callback.message.edit_text("📭 <b>Нет активных подписчиков</b>")
        return
    
    text = f"⭐ <b>Активные подписчики:</b> {len(subscribers)}\n\n"
    
    for i, user in enumerate(subscribers, 1):
        until_date = ""
        if user.get('subscription_until'):
            try:
                if isinstance(user['subscription_until'], str):
                    dt = datetime.fromisoformat(user['subscription_until'].replace('Z', '+00:00'))
                else:
                    dt = user['subscription_until']
                until_date = dt.strftime("до %d.%m.%Y")
            except:
                until_date = "активна"
        
        username = f"@{user.get('username')}" if user.get('username') else "без username"
        text += f"{i}. ID: {user.get('telegram_id')} | {username} {until_date}\n"
    
    await callback.message.edit_text(text)
    await callback.answer()

@router.callback_query(F.data == "admin_stats")
async def callback_admin_stats(callback: types.CallbackQuery):
    """Показать статистику админа"""
    if callback.from_user.id != Config.ADMIN_ID:
        await callback.answer("⛔ Нет доступа!", show_alert=True)
        return
    
    all_users = await db.get_all_users()
    subscribers = await db.get_subscribed_users()
    
    # Получаем статистику по постам
    try:
        if not db.is_sqlite and db.pool:
            async with db.pool.acquire() as conn:
                total_posts = await conn.fetchval("SELECT COUNT(*) FROM scheduled_posts")
                published_posts = await conn.fetchval("SELECT COUNT(*) FROM scheduled_posts WHERE is_published = TRUE")
                active_channels = await conn.fetchval("SELECT COUNT(*) FROM channels WHERE is_active = TRUE")
        else:
            cursor = await db.conn.execute("SELECT COUNT(*) FROM scheduled_posts")
            total_posts = (await cursor.fetchone())[0]
            
            cursor = await db.conn.execute("SELECT COUNT(*) FROM scheduled_posts WHERE is_published = 1")
            published_posts = (await cursor.fetchone())[0]
            
            cursor = await db.conn.execute("SELECT COUNT(*) FROM channels WHERE is_active = 1")
            active_channels = (await cursor.fetchone())[0]
    except Exception as e:
        logger.error(f"Ошибка получения статистики: {e}")
        total_posts = published_posts = active_channels = 0
    
    # Конверсия
    conversion = (len(subscribers) / len(all_users) * 100) if all_users else 0
    
    stats_text = (
        f"📊 <b>Статистика бота</b>\n\n"
        f"<b>👥 Пользователи:</b>\n"
        f"• Всего: {len(all_users)}\n"
        f"• Подписчиков: {len(subscribers)}\n"
        f"• Конверсия: {conversion:.1f}%\n\n"
        
        f"<b>📈 Активность:</b>\n"
        f"• Всего постов: {total_posts}\n"
        f"• Опубликовано: {published_posts}\n"
        f"• Активных каналов: {active_channels}\n\n"
        
        f"<b>💰 Тариф:</b>\n"
        f"• Название: {Config.TARIFF_NAME}\n"
        f"• Цена: {Config.TARIFF_PRICE}\n"
        f"• Лимиты: {Config.TARIFF_CHANNELS_LIMIT} каналов, {Config.TARIFF_POSTS_PER_DAY} постов/день\n\n"
        
        f"<b>⚙️ Система:</b>\n"
        f"• Серверное время: {datetime.utcnow().strftime('%H:%M UTC')}\n"
        f"• Задач в планировщике: {len(scheduler.get_jobs())}"
    )
    
    await callback.message.edit_text(stats_text)
    await callback.answer()

@router.callback_query(F.data == "admin_refresh")
async def callback_admin_refresh(callback: types.CallbackQuery):
    """Обновить админ панель"""
    if callback.from_user.id != Config.ADMIN_ID:
        await callback.answer("⛔ Нет доступа!", show_alert=True)
        return
    
    await cmd_admin(callback.message)
    await callback.answer("🔄 Обновлено!")

@router.callback_query(F.data == "admin_back")
async def callback_admin_back(callback: types.CallbackQuery):
    """Вернуться в главное меню"""
    if callback.from_user.id != Config.ADMIN_ID:
        await callback.answer("⛔ Нет доступа!", show_alert=True)
        return
    
    try:
        await callback.message.delete()
    except:
        pass
    
    user = await db.get_or_create_user(callback.from_user.id)
    has_subscription = user.get('subscribed', False) if user else False
    
    await callback.message.answer(
        "🔙 <b>Возврат в главное меню</b>",
        reply_markup=get_main_keyboard(callback.from_user.id, has_subscription)
    )
    await callback.answer()

@router.callback_query(F.data == "back_to_main")
async def callback_back_to_main(callback: types.CallbackQuery):
    """Вернуться в главное меню из других разделов"""
    user = await db.get_or_create_user(callback.from_user.id)
    has_subscription = user.get('subscribed', False) if user else False
    
    try:
        await callback.message.delete()
    except:
        pass
    
    await callback.message.answer(
        "🏠 <b>Главное меню</b>",
        reply_markup=get_main_keyboard(callback.from_user.id, has_subscription)
    )
    await callback.answer()

# ========== КОМАНДА ОТМЕНЫ ==========
@router.message(Command("cancel"))
async def cmd_cancel(message: types.Message, state: FSMContext):
    """Отмена текущего действия"""
    current_state = await state.get_state()
    if current_state is None:
        await message.answer("❌ Нет активных действий для отмены.")
        return
    
    await state.clear()
    await message.answer("✅ Действие отменено.", reply_markup=ReplyKeyboardRemove())

# ========== ФУНКЦИЯ ПУБЛИКАЦИИ ПОСТОВ ==========
async def publish_scheduled_post(post_id: int):
    """Опубликовать запланированный пост"""
    try:
        # Получаем данные поста
        if not db.is_sqlite and db.pool:
            async with db.pool.acquire() as conn:
                post = await conn.fetchrow(
                    """
                    SELECT sp.*, u.telegram_id, c.channel_id as channel_ident
                    FROM scheduled_posts sp
                    JOIN users u ON sp.user_id = u.id
                    JOIN channels c ON sp.channel_id = c.channel_id AND c.user_id = u.id
                    WHERE sp.id = $1 AND sp.is_published = FALSE
                    """,
                    post_id
                )
                
                if not post:
                    logger.warning(f"Пост {post_id} не найден или уже опубликован")
                    return
                
                post = dict(post)
        else:
            cursor = await db.conn.execute(
                """
                SELECT sp.*, u.telegram_id, c.channel_id as channel_ident
                FROM scheduled_posts sp
                JOIN users u ON sp.user_id = u.id
                JOIN channels c ON sp.channel_id = c.channel_id AND c.user_id = u.id
                WHERE sp.id = ? AND sp.is_published = 0
                """,
                (post_id,)
            )
            row = await cursor.fetchone()
            if not row:
                logger.warning(f"Пост {post_id} не найден или уже опубликован")
                return
            
            columns = [desc[0] for desc in cursor.description]
            post = dict(zip(columns, row))
        
        # Публикуем пост в канале
        channel_id = post.get('channel_ident')
        message_text = post.get('message_text', '')
        photo_id = post.get('photo_id')
        
        if photo_id:
            await bot.send_photo(
                chat_id=channel_id,
                photo=photo_id,
                caption=message_text,
                parse_mode=ParseMode.HTML
            )
            logger.info(f"Опубликован пост {post_id} с фото в канале {channel_id}")
        else:
            await bot.send_message(
                chat_id=channel_id,
                text=message_text,
                parse_mode=ParseMode.HTML
            )
            logger.info(f"Опубликован пост {post_id} в канале {channel_id}")
        
        # Отмечаем как опубликованный
        await db.mark_post_published(post_id)
        
        # Уведомляем пользователя
        user_id = post.get('telegram_id')
        if user_id:
            await notify_user(
                user_id,
                f"✅ <b>Пост опубликован!</b>\n\n"
                f"Ваш запланированный пост был успешно опубликован в канале.\n\n"
                f"<b>Текст:</b>\n{message_text[:100]}..."
            )
        
    except Exception as e:
        logger.error(f"❌ Ошибка публикации поста {post_id}: {e}")

async def check_pending_posts():
    """Проверить и опубликовать отложенные посты"""
    try:
        posts = await db.get_posts_to_publish()
        
        for post in posts:
            post_id = post.get('id')
            if post_id:
                # Публикуем пост
                await publish_scheduled_post(post_id)
                await asyncio.sleep(0.5)  # Задержка между постами
                
    except Exception as e:
        logger.error(f"Ошибка в check_pending_posts: {e}")

# ========== ЗАПУСК И ВЫКЛЮЧЕНИЕ ==========
async def on_startup():
    """Действия при запуске бота"""
    logger.info("🚀 Бот запускается...")
    
    # Подключаем БД
    await db.connect()
    logger.info("✅ База данных подключена")
    
    # Запускаем планировщик
    scheduler.start()
    scheduler.add_job(
        check_pending_posts,
        'interval',
        minutes=1,
        id='check_posts',
        replace_existing=True
    )
    logger.info("✅ Планировщик запущен")
    
    # Загружаем неопубликованные посты в планировщик
    try:
        if not db.is_sqlite and db.pool:
            async with db.pool.acquire() as conn:
                posts = await conn.fetch(
                    """
                    SELECT id, scheduled_time 
                    FROM scheduled_posts 
                    WHERE is_published = FALSE 
                    AND scheduled_time > NOW()
                    """
                )
        else:
            cursor = await db.conn.execute(
                """
                SELECT id, scheduled_time 
                FROM scheduled_posts 
                WHERE is_published = 0 
                AND scheduled_time > datetime('now')
                """
            )
            posts = await cursor.fetchall()
        
        loaded_count = 0
        for post in posts:
            post_id = post[0]
            scheduled_time = post[1]
            
            if isinstance(scheduled_time, str):
                scheduled_datetime = datetime.fromisoformat(scheduled_time.replace('Z', '+00:00'))
            else:
                scheduled_datetime = scheduled_time
            
            if scheduled_datetime > datetime.utcnow():
                scheduler.add_job(
                    publish_scheduled_post,
                    'date',
                    run_date=scheduled_datetime,
                    args=[post_id],
                    id=f"post_{post_id}",
                    replace_existing=True
                )
                loaded_count += 1
        
        logger.info(f"✅ Загружено {loaded_count} запланированных постов")
        
    except Exception as e:
        logger.error(f"Ошибка загрузки запланированных постов: {e}")
    
    # Отправляем уведомление админу
    try:
        await bot.send_message(
            chat_id=Config.ADMIN_ID,
            text=f"🤖 <b>Бот запущен!</b>\n\n"
                 f"Время: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}\n"
                 f"Пользователей в БД: {len(await db.get_all_users())}\n"
                 f"Запланированных постов: {loaded_count}\n\n"
                 f"✅ Бот готов к работе!"
        )
    except Exception as e:
        logger.error(f"Не удалось отправить уведомление админу: {e}")
    
    logger.info("✅ Бот успешно запущен!")

async def on_shutdown():
    """Действия при выключении бота"""
    logger.info("🛑 Бот выключается...")
    
    # Останавливаем планировщик
    scheduler.shutdown()
    logger.info("✅ Планировщик остановлен")
    
    # Закрываем БД
    await db.close()
    logger.info("✅ База данных отключена")
    
    # Закрываем сессию бота
    await bot.session.close()
    logger.info("✅ Сессия бота закрыта")

# ========== ГЛАВНАЯ ФУНКЦИЯ ==========
async def main():
    """Основная функция запуска"""
    # Регистрируем обработчики событий
    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)
    
    # Удаляем вебхук (на всякий случай)
    await bot.delete_webhook(drop_pending_updates=True)
    
    # Запускаем поллинг
    try:
        await dp.start_polling(
            bot,
            allowed_updates=dp.resolve_used_update_types(),
            skip_updates=False
        )
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
        raise
    finally:
        await on_shutdown()

if __name__ == "__main__":
    # Проверяем обязательные переменные
    try:
        Config.validate()
    except ValueError as e:
        logger.error(f"Ошибка конфигурации: {e}")
        print(f"\n❌ Ошибка: {e}")
        print("Проверьте файл .env или переменные окружения на Railway:")
        print("1. BOT_TOKEN - токен бота от @BotFather")
        print("2. ADMIN_ID - ваш Telegram ID (узнать через @userinfobot)")
        sys.exit(1)
    
    # Запускаем бота
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Бот остановлен пользователем")
    except Exception as e:
        logger.error(f"Неожиданная ошибка: {e}")
        sys.exit(1)
