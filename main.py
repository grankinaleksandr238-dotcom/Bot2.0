import asyncio
import logging
import random
import os
import time
import string
from datetime import datetime, timedelta, date
from typing import Dict, List, Optional, Tuple, Any
from collections import defaultdict
import asyncpg
from aiohttp import web

from aiogram import Bot, Dispatcher, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils import executor
from aiogram.utils.exceptions import (
    BotBlocked, UserDeactivated, ChatNotFound, RetryAfter,
    TelegramAPIError, MessageNotModified, MessageToEditNotFound,
    TerminatedByOtherGetUpdates, ChatAdminRequired
)
from aiogram.dispatcher.middlewares import BaseMiddleware
from aiogram.dispatcher.handler import CancelHandler

# ===== НАСТРОЙКИ =====
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN не задан в переменных окружения")

SUPER_ADMINS_STR = os.getenv("SUPER_ADMINS", "")
SUPER_ADMINS = [int(x.strip()) for x in SUPER_ADMINS_STR.split(",") if x.strip()]

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL не задан. Создай PostgreSQL базу в Railway.")

# Значения по умолчанию для настроек
DEFAULT_SETTINGS = {
    "random_attack_cost": "0",
    "targeted_attack_cost": "50",
    "theft_cooldown_minutes": "30",
    "theft_success_chance": "40",
    "theft_defense_chance": "20",
    "theft_defense_penalty": "10",
    "casino_win_chance": "30",
    "min_theft_amount": "5",
    "max_theft_amount": "15",
    "dice_multiplier": "2",
    "guess_multiplier": "5",
    "guess_reputation": "1",
    "chat_notify_big_win": "1",
    "chat_notify_big_purchase": "1",
    "chat_notify_giveaway": "1",
    "gift_amount": "30",
    "gift_limit_per_day": "3",
    "referral_bonus": "50",
    "referral_reputation": "2",
    # Настройки опыта и уровней
    "exp_per_casino_win": "5",
    "exp_per_casino_lose": "1",
    "exp_per_dice_win": "3",
    "exp_per_dice_lose": "1",
    "exp_per_guess_win": "4",
    "exp_per_guess_lose": "1",
    "exp_per_theft_success": "10",
    "exp_per_theft_fail": "2",
    "exp_per_theft_defense": "5",
    "exp_per_game_win": "15",
    "exp_per_game_lose": "3",
    "level_multiplier": "100",
    "level_reward_coins": "50",
    "level_reward_reputation": "5",
    "level_reward_coins_increment": "10",
    "level_reward_reputation_increment": "1",
    "reputation_theft_bonus": "0.5",
    "reputation_defense_bonus": "0.5",
    # Настройки боссов
    "boss_spawn_chance": "20",
    "boss_min_interval": "360",
    "boss_max_per_day": "2",
    "boss_hp_multiplier": "100",
    "boss_attack_cooldown": "3",
    "boss_base_damage": "10",
    "boss_reward_coins": "500",
    "boss_reward_coins_variance": "200",
    # Настройки подгона
    "gift_global_limit_per_user": "4",
    "gift_cooldown": "60",
    # Настройки статов
    "stat_strength_per_level": "1",
    "stat_agility_per_level": "1",
    "stat_defense_per_level": "1",
}

# Константы
ITEMS_PER_PAGE = 10
BIG_WIN_THRESHOLD = 100
BIG_PURCHASE_THRESHOLD = 100
MAX_ROOMS = 20
MIN_PLAYERS = 2
MAX_PLAYERS = 5
MIN_BET = 3
DEALER_WIN_RATE = 3

# ===== ИНИЦИАЛИЗАЦИЯ =====
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s"
)

db_pool = None
settings_cache = {}
last_settings_update = 0
channels_cache = []
last_channels_update = 0
confirmed_chats_cache = {}
last_confirmed_chats_update = 0

async def before_start():
    await bot.delete_webhook(drop_pending_updates=True)
    logging.info("Webhook удалён, пропущены старые обновления")

bot = Bot(token=BOT_TOKEN, parse_mode="HTML")
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)

# ===== МИДЛВАРЬ ДЛЯ ЗАЩИТЫ ОТ ФЛУДА =====
class ThrottlingMiddleware(BaseMiddleware):
    def __init__(self, rate_limit=1.0):
        self.rate_limit = rate_limit
        self.user_last_time = defaultdict(float)
        super().__init__()

    async def on_process_message(self, message: types.Message, data: dict):
        if message.chat.type != 'private' or await is_admin(message.from_user.id):
            return
        user_id = message.from_user.id
        now = time.time()
        if now - self.user_last_time[user_id] < self.rate_limit:
            await message.reply("⏳ Слишком много запросов. Подожди секунду.")
            raise CancelHandler()
        self.user_last_time[user_id] = now

dp.middleware.setup(ThrottlingMiddleware(rate_limit=0.5))

# ===== БЕЗОПАСНАЯ ОТПРАВКА СООБЩЕНИЙ =====
async def safe_send_message(user_id: int, text: str, **kwargs):
    try:
        await bot.send_message(user_id, text, **kwargs)
    except BotBlocked:
        logging.warning(f"Bot blocked by user {user_id}")
    except UserDeactivated:
        logging.warning(f"User {user_id} deactivated")
    except ChatNotFound:
        logging.warning(f"Chat {user_id} not found")
    except RetryAfter as e:
        logging.warning(f"Flood limit exceeded. Retry after {e.timeout} seconds")
        await asyncio.sleep(e.timeout)
        try:
            await bot.send_message(user_id, text, **kwargs)
        except Exception as ex:
            logging.warning(f"Still failed after retry: {ex}")
    except TelegramAPIError as e:
        logging.warning(f"Telegram API error for user {user_id}: {e}")
    except Exception as e:
        logging.warning(f"Failed to send message to {user_id}: {e}")

def safe_send_message_task(user_id: int, text: str, **kwargs):
    asyncio.create_task(safe_send_message(user_id, text, **kwargs))

async def safe_send_chat(chat_id: int, text: str, **kwargs):
    try:
        await bot.send_message(chat_id, text, **kwargs)
    except Exception as e:
        logging.error(f"Failed to send to chat {chat_id}: {e}")

# ===== ПОДКЛЮЧЕНИЕ К POSTGRESQL =====
async def create_db_pool():
    global db_pool
    db_pool = await asyncpg.create_pool(
        DATABASE_URL,
        min_size=5,
        max_size=20,
        command_timeout=60,
        max_queries=50000,
        max_inactive_connection_lifetime=300
    )
    logging.info("Подключение к PostgreSQL установлено")

async def init_db():
    async with db_pool.acquire() as conn:
        # Таблица users
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                joined_date TEXT,
                balance INTEGER DEFAULT 0,
                reputation INTEGER DEFAULT 0,
                total_spent INTEGER DEFAULT 0,
                negative_balance INTEGER DEFAULT 0,
                last_bonus TEXT,
                last_theft_time TEXT,
                theft_attempts INTEGER DEFAULT 0,
                theft_success INTEGER DEFAULT 0,
                theft_failed INTEGER DEFAULT 0,
                theft_protected INTEGER DEFAULT 0,
                casino_wins INTEGER DEFAULT 0,
                casino_losses INTEGER DEFAULT 0,
                guess_wins INTEGER DEFAULT 0,
                guess_losses INTEGER DEFAULT 0,
                game_wins INTEGER DEFAULT 0
            )
        ''')
        # Добавляем новые поля
        await conn.execute('ALTER TABLE users ADD COLUMN IF NOT EXISTS exp INTEGER DEFAULT 0')
        await conn.execute('ALTER TABLE users ADD COLUMN IF NOT EXISTS level INTEGER DEFAULT 1')
        await conn.execute('ALTER TABLE users ADD COLUMN IF NOT EXISTS strength INTEGER DEFAULT 1')
        await conn.execute('ALTER TABLE users ADD COLUMN IF NOT EXISTS agility INTEGER DEFAULT 1')
        await conn.execute('ALTER TABLE users ADD COLUMN IF NOT EXISTS defense INTEGER DEFAULT 1')
        await conn.execute('ALTER TABLE users ADD COLUMN IF NOT EXISTS last_gift_time TEXT')
        await conn.execute('ALTER TABLE users ADD COLUMN IF NOT EXISTS gift_count_today INTEGER DEFAULT 0')

        # Таблица подтверждённых чатов
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS confirmed_chats (
                chat_id BIGINT PRIMARY KEY,
                title TEXT,
                type TEXT,
                joined_date TEXT,
                confirmed_by BIGINT,
                confirmed_date TEXT,
                notify_enabled BOOLEAN DEFAULT TRUE,
                last_gift_date DATE,
                gift_count_today INTEGER DEFAULT 0,
                boss_last_spawn TEXT,
                boss_spawn_count INTEGER DEFAULT 0
            )
        ''')

        # Таблица запросов на подтверждение чатов
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS chat_confirmation_requests (
                chat_id BIGINT PRIMARY KEY,
                title TEXT,
                type TEXT,
                requested_by BIGINT,
                request_date TEXT,
                status TEXT DEFAULT 'pending'
            )
        ''')

        # Таблица боссов (participants теперь BIGINT[])
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS bosses (
                id SERIAL PRIMARY KEY,
                chat_id BIGINT,
                name TEXT,
                level INTEGER,
                hp INTEGER,
                max_hp INTEGER,
                spawned_at TEXT,
                expires_at TEXT,
                reward_coins INTEGER,
                participants BIGINT[] DEFAULT '{}',
                status TEXT DEFAULT 'active'
            )
        ''')

        # Таблица атак на босса
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS boss_attacks (
                boss_id INTEGER,
                user_id BIGINT,
                damage INTEGER,
                attack_time TEXT,
                PRIMARY KEY (boss_id, user_id)
            )
        ''')

        # Остальные таблицы
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS channels (
                id SERIAL PRIMARY KEY,
                chat_id TEXT UNIQUE,
                title TEXT,
                invite_link TEXT
            )
        ''')
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS referrals (
                id SERIAL PRIMARY KEY,
                referrer_id BIGINT,
                referred_id BIGINT UNIQUE,
                referred_date TEXT,
                reward_given BOOLEAN DEFAULT FALSE
            )
        ''')
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS shop_items (
                id SERIAL PRIMARY KEY,
                name TEXT,
                description TEXT,
                price INTEGER,
                stock INTEGER DEFAULT -1
            )
        ''')
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS purchases (
                id SERIAL PRIMARY KEY,
                user_id BIGINT,
                item_id INTEGER,
                purchase_date TEXT,
                status TEXT DEFAULT 'pending',
                admin_comment TEXT
            )
        ''')
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS promocodes (
                code TEXT PRIMARY KEY,
                reward INTEGER,
                max_uses INTEGER,
                used_count INTEGER DEFAULT 0,
                created_at TEXT
            )
        ''')
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS promo_activations (
                user_id BIGINT,
                promo_code TEXT,
                activated_at TEXT,
                PRIMARY KEY (user_id, promo_code)
            )
        ''')
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS giveaways (
                id SERIAL PRIMARY KEY,
                prize TEXT,
                description TEXT,
                end_date TEXT,
                media_file_id TEXT,
                media_type TEXT,
                status TEXT DEFAULT 'active',
                winner_id BIGINT,
                winners_count INTEGER DEFAULT 1,
                notified BOOLEAN DEFAULT FALSE
            )
        ''')
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS participants (
                user_id BIGINT,
                giveaway_id INTEGER,
                PRIMARY KEY (user_id, giveaway_id)
            )
        ''')
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS admins (
                user_id BIGINT PRIMARY KEY,
                added_by BIGINT,
                added_date TEXT
            )
        ''')
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS banned_users (
                user_id BIGINT PRIMARY KEY,
                banned_by BIGINT,
                banned_date TEXT,
                reason TEXT
            )
        ''')
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        ''')
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS tasks (
                id SERIAL PRIMARY KEY,
                name TEXT,
                description TEXT,
                task_type TEXT,
                target_id TEXT,
                reward_coins INTEGER DEFAULT 0,
                reward_reputation INTEGER DEFAULT 0,
                required_days INTEGER DEFAULT 0,
                penalty_days INTEGER DEFAULT 0,
                created_by BIGINT,
                created_at TEXT,
                active BOOLEAN DEFAULT TRUE
            )
        ''')
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS user_tasks (
                user_id BIGINT,
                task_id INTEGER,
                completed_at TEXT,
                expires_at TEXT,
                status TEXT DEFAULT 'completed',
                PRIMARY KEY (user_id, task_id)
            )
        ''')
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS multiplayer_games (
                game_id TEXT PRIMARY KEY,
                host_id BIGINT,
                max_players INTEGER,
                bet_amount INTEGER,
                status TEXT DEFAULT 'waiting',
                deck TEXT,
                created_at TEXT
            )
        ''')
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS game_players (
                game_id TEXT,
                user_id BIGINT,
                username TEXT,
                cards TEXT,
                value INTEGER DEFAULT 0,
                stopped BOOLEAN DEFAULT FALSE,
                joined_at TEXT,
                doubled BOOLEAN DEFAULT FALSE,
                PRIMARY KEY (game_id, user_id)
            )
        ''')
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS level_rewards (
                level INTEGER PRIMARY KEY,
                coins INTEGER,
                reputation INTEGER
            )
        ''')

        # Индексы
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_users_balance ON users(balance DESC)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_users_reputation ON users(reputation DESC)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_users_total_spent ON users(total_spent DESC)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_users_username ON users(username)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_purchases_user_id ON purchases(user_id)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_purchases_status ON purchases(status)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_giveaways_status ON giveaways(status)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_promo_activations_user ON promo_activations(user_id)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_user_tasks_expires ON user_tasks(expires_at)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_tasks_active ON tasks(active)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_referrals_referrer ON referrals(referrer_id)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_multiplayer_games_status ON multiplayer_games(status)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_users_level ON users(level)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_users_exp ON users(exp)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_bosses_chat_status ON bosses(chat_id, status)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_boss_attacks_boss ON boss_attacks(boss_id)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_confirmed_chats_chat ON confirmed_chats(chat_id)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_chat_requests_status ON chat_confirmation_requests(status)")

    # Заполняем настройки
    await init_settings()
    # Заполняем level_rewards
    async with db_pool.acquire() as conn:
        for lvl in range(1, 101):
            exists = await conn.fetchval("SELECT level FROM level_rewards WHERE level=$1", lvl)
            if not exists:
                coins = int(DEFAULT_SETTINGS["level_reward_coins"]) + (lvl-1) * int(DEFAULT_SETTINGS["level_reward_coins_increment"])
                rep = int(DEFAULT_SETTINGS["level_reward_reputation"]) + (lvl-1) * int(DEFAULT_SETTINGS["level_reward_reputation_increment"])
                await conn.execute(
                    "INSERT INTO level_rewards (level, coins, reputation) VALUES ($1, $2, $3)",
                    lvl, coins, rep
                )
    logging.info("Таблицы в PostgreSQL проверены/обновлены")

async def init_settings():
    async with db_pool.acquire() as conn:
        for key, value in DEFAULT_SETTINGS.items():
            await conn.execute(
                "INSERT INTO settings (key, value) VALUES ($1, $2) ON CONFLICT (key) DO NOTHING",
                key, value
            )

async def get_setting(key: str) -> str:
    global settings_cache, last_settings_update
    now = time.time()
    if now - last_settings_update > 60 or not settings_cache:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch("SELECT key, value FROM settings")
            settings_cache = {row['key']: row['value'] for row in rows}
        last_settings_update = now
    return settings_cache.get(key, DEFAULT_SETTINGS[key])

async def set_setting(key: str, value: str):
    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE settings SET value=$1 WHERE key=$2", value, key)
    settings_cache[key] = value

# ===== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ =====
async def is_super_admin(user_id: int) -> bool:
    return user_id in SUPER_ADMINS

async def is_junior_admin(user_id: int) -> bool:
    async with db_pool.acquire() as conn:
        row = await conn.fetchval("SELECT user_id FROM admins WHERE user_id=$1", user_id)
    return row is not None

async def is_admin(user_id: int) -> bool:
    return await is_super_admin(user_id) or await is_junior_admin(user_id)

async def is_banned(user_id: int) -> bool:
    async with db_pool.acquire() as conn:
        row = await conn.fetchval("SELECT user_id FROM banned_users WHERE user_id=$1", user_id)
    return row is not None

async def get_channels():
    global channels_cache, last_channels_update
    now = time.time()
    if now - last_channels_update > 300 or not channels_cache:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch("SELECT chat_id, title, invite_link FROM channels")
            channels_cache = [(r['chat_id'], r['title'], r['invite_link']) for r in rows]
        last_channels_update = now
    return channels_cache

async def get_confirmed_chats(force_update=False) -> Dict[int, dict]:
    global confirmed_chats_cache, last_confirmed_chats_update
    now = time.time()
    if force_update or now - last_confirmed_chats_update > 300 or not confirmed_chats_cache:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM confirmed_chats")
            confirmed_chats_cache = {row['chat_id']: dict(row) for row in rows}
        last_confirmed_chats_update = now
    return confirmed_chats_cache

async def is_chat_confirmed(chat_id: int) -> bool:
    confirmed = await get_confirmed_chats()
    return chat_id in confirmed

async def add_confirmed_chat(chat_id: int, title: str, chat_type: str, confirmed_by: int):
    async with db_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO confirmed_chats (chat_id, title, type, joined_date, confirmed_by, confirmed_date) VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (chat_id) DO UPDATE SET confirmed_by=$5, confirmed_date=$6",
            chat_id, title, chat_type, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), confirmed_by, datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        )
    await get_confirmed_chats(force_update=True)

async def remove_confirmed_chat(chat_id: int):
    async with db_pool.acquire() as conn:
        await conn.execute("DELETE FROM confirmed_chats WHERE chat_id=$1", chat_id)
    await get_confirmed_chats(force_update=True)

async def create_chat_confirmation_request(chat_id: int, title: str, chat_type: str, requested_by: int):
    async with db_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO chat_confirmation_requests (chat_id, title, type, requested_by, request_date, status) VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (chat_id) DO UPDATE SET status='pending', requested_by=$4, request_date=$5",
            chat_id, title, chat_type, requested_by, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 'pending'
        )

async def get_pending_chat_requests() -> List[dict]:
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM chat_confirmation_requests WHERE status='pending' ORDER BY request_date")
        return [dict(r) for r in rows]

async def update_chat_request_status(chat_id: int, status: str):
    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE chat_confirmation_requests SET status=$1 WHERE chat_id=$2", status, chat_id)

# Работа с пользователями
async def get_user_balance(user_id: int) -> int:
    async with db_pool.acquire() as conn:
        balance = await conn.fetchval("SELECT balance FROM users WHERE user_id=$1", user_id)
        return balance if balance is not None else 0

async def update_user_balance(user_id: int, delta: int):
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT balance, negative_balance FROM users WHERE user_id=$1", user_id)
        if not row:
            return
        balance, negative = row['balance'], row['negative_balance']
        new_balance = balance + delta
        if new_balance < 0:
            negative += abs(new_balance)
            new_balance = 0
        await conn.execute(
            "UPDATE users SET balance=$1, negative_balance=$2 WHERE user_id=$3",
            new_balance, negative, user_id
        )

async def get_user_reputation(user_id: int) -> int:
    async with db_pool.acquire() as conn:
        rep = await conn.fetchval("SELECT reputation FROM users WHERE user_id=$1", user_id)
        return rep if rep is not None else 0

async def update_user_reputation(user_id: int, delta: int):
    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE users SET reputation = reputation + $1 WHERE user_id=$2", delta, user_id)

async def get_user_stats(user_id: int) -> dict:
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT level, strength, agility, defense FROM users WHERE user_id=$1", user_id)
        if row:
            return dict(row)
        return {'level': 1, 'strength': 1, 'agility': 1, 'defense': 1}

async def update_user_stats(user_id: int, strength_delta=0, agility_delta=0, defense_delta=0):
    async with db_pool.acquire() as conn:
        await conn.execute(
            "UPDATE users SET strength = strength + $1, agility = agility + $2, defense = defense + $3 WHERE user_id=$4",
            strength_delta, agility_delta, defense_delta, user_id
        )

async def add_exp(user_id: int, exp: int, conn=None):
    async def _add(conn):
        user = await conn.fetchrow("SELECT exp, level FROM users WHERE user_id=$1", user_id)
        if not user:
            return
        new_exp = user['exp'] + exp
        level = user['level']
        level_mult = int(await get_setting("level_multiplier"))
        levels_gained = 0
        while new_exp >= level * level_mult:
            new_exp -= level * level_mult
            level += 1
            levels_gained += 1
        await conn.execute(
            "UPDATE users SET exp=$1, level=$2 WHERE user_id=$3",
            new_exp, level, user_id
        )
        if levels_gained > 0:
            str_inc = int(await get_setting("stat_strength_per_level")) * levels_gained
            agi_inc = int(await get_setting("stat_agility_per_level")) * levels_gained
            def_inc = int(await get_setting("stat_defense_per_level")) * levels_gained
            await update_user_stats(user_id, str_inc, agi_inc, def_inc)
            for lvl in range(level - levels_gained + 1, level + 1):
                await reward_level_up(user_id, lvl, conn)
    if conn:
        await _add(conn)
    else:
        async with db_pool.acquire() as conn2:
            await _add(conn2)

async def reward_level_up(user_id: int, new_level: int, conn=None):
    async def _reward(conn):
        reward = await conn.fetchrow(
            "SELECT coins, reputation FROM level_rewards WHERE level=$1",
            new_level
        )
        if reward:
            await conn.execute(
                "UPDATE users SET balance = balance + $1, reputation = reputation + $2 WHERE user_id=$3",
                reward['coins'], reward['reputation'], user_id
            )
            await safe_send_message(
                user_id,
                f"🎉 Поздравляем! Ты достиг {new_level} уровня!\n"
                f"Награда: +{reward['coins']} монет, +{reward['reputation']} репутации!\n"
                f"Твои статы увеличены: сила +{int(await get_setting('stat_strength_per_level'))}, ловкость +{int(await get_setting('stat_agility_per_level'))}, защита +{int(await get_setting('stat_defense_per_level'))}."
            )
    if conn:
        await _reward(conn)
    else:
        async with db_pool.acquire() as conn2:
            await _reward(conn2)

async def get_user_level(user_id: int) -> int:
    async with db_pool.acquire() as conn:
        level = await conn.fetchval("SELECT level FROM users WHERE user_id=$1", user_id)
        return level if level is not None else 1

async def get_user_exp(user_id: int) -> int:
    async with db_pool.acquire() as conn:
        exp = await conn.fetchval("SELECT exp FROM users WHERE user_id=$1", user_id)
        return exp if exp is not None else 0

async def update_user_total_spent(user_id: int, amount: int):
    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE users SET total_spent = total_spent + $1 WHERE user_id=$2", amount, user_id)

async def get_random_user(exclude_id: int):
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT user_id FROM users 
            WHERE user_id != $1 AND user_id NOT IN (SELECT user_id FROM banned_users)
            ORDER BY RANDOM() LIMIT 1
        """, exclude_id)
        return row['user_id'] if row else None

async def find_user_by_input(input_str: str) -> Optional[Dict]:
    input_str = input_str.strip()
    try:
        uid = int(input_str)
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM users WHERE user_id=$1", uid)
            return dict(row) if row else None
    except ValueError:
        username = input_str.lower()
        if username.startswith('@'):
            username = username[1:]
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM users WHERE LOWER(username)=$1", username)
            return dict(row) if row else None

async def notify_chats(message_text: str, importance: str = 'info'):
    confirmed = await get_confirmed_chats()
    for chat_id, data in confirmed.items():
        if not data.get('notify_enabled', True):
            continue
        await safe_send_chat(chat_id, message_text)

# Функции для мультиплеера
def generate_game_id():
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))

def calculate_hand_value(cards):
    value = 0
    aces = 0
    for card in cards:
        rank = card[:-1]
        if rank in ['J', 'Q', 'K']:
            value += 10
        elif rank == 'A':
            aces += 1
            value += 11
        else:
            value += int(rank)
    while value > 21 and aces:
        value -= 10
        aces -= 1
    return value

def create_deck():
    suits = ['♠', '♥', '♦', '♣']
    ranks = ['2', '3', '4', '5', '6', '7', '8', '9', '10', 'J', 'Q', 'K', 'A']
    deck = [f"{rank}{suit}" for suit in suits for rank in ranks]
    random.shuffle(deck)
    return deck

# ===== СОСТОЯНИЯ FSM =====
class CreateGiveaway(StatesGroup):
    prize = State()
    description = State()
    end_date = State()
    media = State()

class AddChannel(StatesGroup):
    chat_id = State()
    title = State()
    invite_link = State()

class RemoveChannel(StatesGroup):
    chat_id = State()

class AddShopItem(StatesGroup):
    name = State()
    description = State()
    price = State()
    stock = State()

class RemoveShopItem(StatesGroup):
    item_id = State()

class EditShopItem(StatesGroup):
    item_id = State()
    field = State()
    value = State()

class CreatePromocode(StatesGroup):
    code = State()
    reward = State()
    max_uses = State()

class Broadcast(StatesGroup):
    media = State()

class AddBalance(StatesGroup):
    user_id = State()
    amount = State()

class RemoveBalance(StatesGroup):
    user_id = State()
    amount = State()

class AddReputation(StatesGroup):
    user_id = State()
    amount = State()

class RemoveReputation(StatesGroup):
    user_id = State()
    amount = State()

class AddExp(StatesGroup):
    user_id = State()
    amount = State()

class SetLevel(StatesGroup):
    user_id = State()
    level = State()

class CasinoBet(StatesGroup):
    amount = State()

class DiceBet(StatesGroup):
    amount = State()

class GuessBet(StatesGroup):
    amount = State()
    number = State()

class PromoActivate(StatesGroup):
    code = State()

class TheftTarget(StatesGroup):
    target = State()

class FindUser(StatesGroup):
    query = State()

class AddJuniorAdmin(StatesGroup):
    user_id = State()

class RemoveJuniorAdmin(StatesGroup):
    user_id = State()

class CompleteGiveaway(StatesGroup):
    giveaway_id = State()
    winners_count = State()

class BlockUser(StatesGroup):
    user_id = State()
    reason = State()

class UnblockUser(StatesGroup):
    user_id = State()

class EditSettings(StatesGroup):
    key = State()
    value = State()

class CreateTask(StatesGroup):
    name = State()
    description = State()
    task_type = State()
    target_id = State()
    reward_coins = State()
    reward_reputation = State()
    required_days = State()
    penalty_days = State()

class TakeTask(StatesGroup):
    task_id = State()

class DeleteTask(StatesGroup):
    task_id = State()

class MultiplayerGame(StatesGroup):
    create_max_players = State()
    create_bet = State()
    join_code = State()

class RoomChat(StatesGroup):
    message = State()

class ManageChats(StatesGroup):
    action = State()
    chat_id = State()

class BossSpawn(StatesGroup):
    chat_id = State()
    level = State()

# ===== КЛАВИАТУРЫ =====
def subscription_inline(not_subscribed):
    kb = []
    for title, link in not_subscribed:
        if link:
            kb.append([InlineKeyboardButton(text=f"📢 {title}", url=link)])
        else:
            kb.append([InlineKeyboardButton(text=f"📢 {title}", callback_data="no_link")])
    kb.append([InlineKeyboardButton(text="✅ Я подписался", callback_data="check_sub")])
    return InlineKeyboardMarkup(row_width=1, inline_keyboard=kb)

def user_main_keyboard(is_admin_user=False):
    buttons = [
        [KeyboardButton(text="👤 Профиль"), KeyboardButton(text="🎁 Бонус")],
        [KeyboardButton(text="🛒 Магазин подарков"), KeyboardButton(text="🎰 Казино")],
        [KeyboardButton(text="🎟 Промокод"), KeyboardButton(text="🏆 Топ игроков")],
        [KeyboardButton(text="💰 Мои покупки"), KeyboardButton(text="🔫 Ограбить")],
        [KeyboardButton(text="🎲 Игры"), KeyboardButton(text="⭐️ Репутация")],
        [KeyboardButton(text="📋 Задания"), KeyboardButton(text="🔗 Рефералка")],
        [KeyboardButton(text="🎲 Розыгрыши"), KeyboardButton(text="📊 Уровень")],
    ]
    if is_admin_user:
        buttons.append([KeyboardButton(text="⚙️ Админ панель")])
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)

def theft_choice_keyboard():
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="🎲 Случайная цель")],
        [KeyboardButton(text="👤 Выбрать пользователя")],
        [KeyboardButton(text="◀️ Назад")]
    ], resize_keyboard=True)

def games_keyboard():
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="🎲 Кости"), KeyboardButton(text="🔢 Угадай число")],
        [KeyboardButton(text="👥 Комнатная игра 21")],
        [KeyboardButton(text="◀️ Назад")]
    ], resize_keyboard=True)

def room_menu_keyboard():
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="📋 Список комнат")],
        [KeyboardButton(text="🎮 Создать комнату")],
        [KeyboardButton(text="ℹ️ Правила игры")],
        [KeyboardButton(text="🏆 Топ игроков")],
        [KeyboardButton(text="◀️ Назад в игры")]
    ], resize_keyboard=True)

def room_control_keyboard(game_id):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🚀 Начать игру", callback_data=f"start_game_{game_id}")],
        [InlineKeyboardButton(text="❌ Закрыть комнату", callback_data=f"close_room_{game_id}")]
    ])

def room_action_keyboard(game_id):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎯 Ещё", callback_data="room_hit"),
         InlineKeyboardButton(text="🛑 Хватит", callback_data="room_stand")],
        [InlineKeyboardButton(text="💰 Удвоить", callback_data="room_double")],
        [InlineKeyboardButton(text="🏳️ Сдаться", callback_data="room_surrender")],
        [InlineKeyboardButton(text="💬 Написать в чат", callback_data="room_chat")]
    ])

def leave_room_keyboard(game_id):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🚪 Выйти из комнаты", callback_data=f"leave_room_{game_id}")]
    ])

def admin_main_keyboard(is_super):
    buttons = [
        [KeyboardButton(text="👥 Управление пользователями")],
        [KeyboardButton(text="🛒 Управление магазином")],
        [KeyboardButton(text="🎁 Управление розыгрышами")],
        [KeyboardButton(text="📢 Управление каналами")],
        [KeyboardButton(text="🎫 Управление промокодами")],
        [KeyboardButton(text="📋 Управление заданиями")],
        [KeyboardButton(text="🤖 Управление чатами")],
        [KeyboardButton(text="👾 Управление боссами")],
        [KeyboardButton(text="⚙️ Настройки игры")],
        [KeyboardButton(text="📊 Статистика")],
        [KeyboardButton(text="🔨 Блокировки")],
        [KeyboardButton(text="📢 Рассылка")],
        [KeyboardButton(text="🧹 Очистка старых записей")],
    ]
    if is_super:
        buttons.append([KeyboardButton(text="➕ Управление админами")])
    buttons.append([KeyboardButton(text="◀️ Назад в главное меню")])
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)

def admin_users_keyboard():
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="💰 Начислить монеты"), KeyboardButton(text="💸 Списать монеты")],
        [KeyboardButton(text="⭐️ Начислить репутацию"), KeyboardButton(text="🔻 Снять репутацию")],
        [KeyboardButton(text="📈 Начислить опыт"), KeyboardButton(text="🔝 Установить уровень")],
        [KeyboardButton(text="👥 Найти пользователя")],
        [KeyboardButton(text="◀️ Назад в админку")]
    ], resize_keyboard=True)

def admin_shop_keyboard():
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="➕ Добавить товар")],
        [KeyboardButton(text="➖ Удалить товар")],
        [KeyboardButton(text="✏️ Редактировать товар")],
        [KeyboardButton(text="📋 Список товаров")],
        [KeyboardButton(text="🛍️ Список покупок")],
        [KeyboardButton(text="◀️ Назад в админку")]
    ], resize_keyboard=True)

def admin_giveaway_keyboard():
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="➕ Создать розыгрыш")],
        [KeyboardButton(text="📋 Активные розыгрыши")],
        [KeyboardButton(text="✅ Завершить розыгрыш")],
        [KeyboardButton(text="◀️ Назад в админку")]
    ], resize_keyboard=True)

def admin_channel_keyboard():
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="➕ Добавить канал")],
        [KeyboardButton(text="➖ Удалить канал")],
        [KeyboardButton(text="📋 Список каналов")],
        [KeyboardButton(text="◀️ Назад в админку")]
    ], resize_keyboard=True)

def admin_promo_keyboard():
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="➕ Создать промокод")],
        [KeyboardButton(text="📋 Список промокодов")],
        [KeyboardButton(text="◀️ Назад в админку")]
    ], resize_keyboard=True)

def admin_tasks_keyboard():
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="➕ Создать задание")],
        [KeyboardButton(text="📋 Список заданий")],
        [KeyboardButton(text="❌ Удалить задание")],
        [KeyboardButton(text="◀️ Назад в админку")]
    ], resize_keyboard=True)

def admin_ban_keyboard():
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="🔨 Заблокировать пользователя")],
        [KeyboardButton(text="🔓 Разблокировать пользователя")],
        [KeyboardButton(text="📋 Список заблокированных")],
        [KeyboardButton(text="◀️ Назад в админку")]
    ], resize_keyboard=True)

def admin_admins_keyboard():
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="➕ Добавить админа")],
        [KeyboardButton(text="➖ Удалить админа")],
        [KeyboardButton(text="📋 Список админов")],
        [KeyboardButton(text="◀️ Назад в админку")]
    ], resize_keyboard=True)

def admin_chats_keyboard():
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="📋 Список запросов на подтверждение")],
        [KeyboardButton(text="✅ Подтвердить чат")],
        [KeyboardButton(text="❌ Отклонить запрос")],
        [KeyboardButton(text="📋 Список подтверждённых чатов")],
        [KeyboardButton(text="🗑 Удалить чат из подтверждённых")],
        [KeyboardButton(text="◀️ Назад в админку")]
    ], resize_keyboard=True)

def admin_boss_keyboard():
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="📋 Активные боссы")],
        [KeyboardButton(text="⚔️ Создать босса вручную")],
        [KeyboardButton(text="◀️ Назад в админку")]
    ], resize_keyboard=True)

def settings_reply_keyboard():
    buttons = [
        [KeyboardButton(text="💰 Стоимость случайной кражи")],
        [KeyboardButton(text="👤 Стоимость кражи по username")],
        [KeyboardButton(text="⏱ Кулдаун (минут)")],
        [KeyboardButton(text="🎲 Шанс успеха %")],
        [KeyboardButton(text="🛡 Шанс защиты %")],
        [KeyboardButton(text="💥 Штраф при защите")],
        [KeyboardButton(text="🎰 Шанс казино %")],
        [KeyboardButton(text="💰 Мин. сумма кражи")],
        [KeyboardButton(text="💰 Макс. сумма кражи")],
        [KeyboardButton(text="🎲 Множитель костей")],
        [KeyboardButton(text="🔢 Множитель угадайки")],
        [KeyboardButton(text="⭐️ Репутация за угадайку")],
        [KeyboardButton(text="📢 Уведомления в чатах")],
        [KeyboardButton(text="💰 Сумма подарка в чате")],
        [KeyboardButton(text="📊 Лимит подарков в день")],
        [KeyboardButton(text="👥 Реферальный бонус (монеты)")],
        [KeyboardButton(text="⭐️ Реферальный бонус (репутация)")],
        # Настройки опыта и уровней
        [KeyboardButton(text="📈 Опыт за казино (победа)")],
        [KeyboardButton(text="📉 Опыт за казино (поражение)")],
        [KeyboardButton(text="🎲 Опыт за кости (победа)")],
        [KeyboardButton(text="🎲 Опыт за кости (поражение)")],
        [KeyboardButton(text="🔢 Опыт за угадайку (победа)")],
        [KeyboardButton(text="🔢 Опыт за угадайку (поражение)")],
        [KeyboardButton(text="🔫 Опыт за успешный грабёж")],
        [KeyboardButton(text="🔫 Опыт за провал грабежа")],
        [KeyboardButton(text="🛡 Опыт за защиту")],
        [KeyboardButton(text="👥 Опыт за победу в 21")],
        [KeyboardButton(text="👥 Опыт за поражение в 21")],
        [KeyboardButton(text="📈 Множитель опыта для уровня")],
        [KeyboardButton(text="💰 Базовая награда за уровень (монеты)")],
        [KeyboardButton(text="⭐️ Базовая награда за уровень (репутация)")],
        [KeyboardButton(text="📈 Инкремент награды (монеты)")],
        [KeyboardButton(text="⭐️ Инкремент награды (репутация)")],
        [KeyboardButton(text="🎯 Бонус репутации к грабежу (%)")],
        [KeyboardButton(text="🛡 Бонус репутации к защите (%)")],
        # Настройки боссов
        [KeyboardButton(text="👾 Шанс появления босса (%)")],
        [KeyboardButton(text="⏱ Мин. интервал между боссами (мин)")],
        [KeyboardButton(text="📊 Макс. боссов в день")],
        [KeyboardButton(text="❤️ Множитель HP босса")],
        [KeyboardButton(text="⚔️ Кулдаун атаки (мин)")],
        [KeyboardButton(text="💥 Базовый урон игрока")],
        [KeyboardButton(text="💰 Базовая награда за босса")],
        [KeyboardButton(text="💰 Вариация награды")],
        # Настройки подгона
        [KeyboardButton(text="🎁 Глобальный лимит подгона в день")],
        [KeyboardButton(text="⏱ Кулдаун подгона (мин)")],
        # Настройки статов
        [KeyboardButton(text="💪 Силы за уровень")],
        [KeyboardButton(text="🏃 Ловкости за уровень")],
        [KeyboardButton(text="🛡 Защиты за уровень")],
        [KeyboardButton(text="◀️ Назад в админку")]
    ]
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)

def back_keyboard():
    return ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="◀️ Назад")]], resize_keyboard=True)

def purchase_action_keyboard(purchase_id):
    return InlineKeyboardMarkup(row_width=2, inline_keyboard=[
        [InlineKeyboardButton(text="✅ Выполнено", callback_data=f"purchase_done_{purchase_id}"),
         InlineKeyboardButton(text="❌ Отказ", callback_data=f"purchase_reject_{purchase_id}")]
    ])

def confirm_chat_inline(chat_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"confirm_chat_{chat_id}"),
         InlineKeyboardButton(text="❌ Отклонить", callback_data=f"reject_chat_{chat_id}")]
    ])

def boss_attack_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⚔️ Атаковать босса", callback_data="boss_attack")]
    ])

# ===== ТЕКСТОВЫЕ ФРАЗЫ =====
BONUS_PHRASES = [
    "🎉 Красава, лови +{bonus} монет!",
    "💰 Зашкварно богатенький стал! +{bonus}",
    "🌟 Хайпанули? +{bonus} монет в карман!",
    "🍀 Удача крашеная, держи +{bonus}",
    "🎁 Ты в тренде, +{bonus} монет!"
]

CASINO_WIN_PHRASES = [
    "🎰 Краш! Ты выиграл {win} монет (чистыми {profit})!",
    "🍒 Хайповая комбинация! +{profit} монет!",
    "💫 Фортуна крашеная, твой выигрыш: {win} монет!",
    "🎲 Изи-катка, {profit} монет твои!",
    "✨ Ты красавчик, обыграл казино! +{profit} монет!"
]

CASINO_LOSE_PHRASES = [
    "😢 Обидно, потерял {loss} монет.",
    "💔 Зашкварно, минус {loss}.",
    "📉 Не фортануло, -{loss} монет.",
    "🍂 В следующий раз краш будет твоим, а пока -{loss}.",
    "⚡️ Лузернулся на {loss} монет."
]

PURCHASE_PHRASES = [
    "✅ Купил! Админ скоро в личку прилетит.",
    "🛒 Товар твой! Жди админа, бро.",
    "🎁 Крутая покупка! Админ уже в курсе.",
    "💎 Ты краш! Админ свяжется."
]

THEFT_CHOICE_PHRASES = [
    "🔫 Выбери, как хочешь напасть:",
    "💢 Кого будем грабить?",
    "😈 Куда направим бандитские лапы?"
]

THEFT_COOLDOWN_PHRASES = [
    "⏳ Ты ещё не остыл после прошлого налёта. Подожди {minutes} мин.",
    "🕐 Полегче, ковбой! Отдохни {minutes} минут.",
    "😴 Грабить так часто – плохая примета. Возвращайся через {minutes} мин."
]

THEFT_NO_MONEY_PHRASES = [
    "😕 У тебя нет монет даже на подготовку к краже!",
    "💸 Сначала заработай, потом грабить будешь.",
    "💰 Пустой карман – не до криминала."
]

THEFT_SUCCESS_PHRASES = [
    "🔫 Красава! Ты украл {amount} монет у {target}!",
    "💰 Хайпанул, {amount} монет у {target} теперь твои!",
    "🦹‍♂️ Удачная кража! +{amount} от {target}",
    "😈 Ты краш, {target} даже не понял! +{amount}"
]

THEFT_FAIL_PHRASES = [
    "😢 Облом, тебя спалили! Ничего не украл.",
    "🚨 Треск, {target} оказался слишком бдительным!",
    "👮‍♂️ Пришлось сваливать, 0 монет.",
    "💔 Не фортануло, {target} слишком крутой."
]

THEFT_DEFENSE_PHRASES = [
    "🛡️ {target} отразил атаку! Ты потерял {penalty} монет.",
    "💥 Бабах! {target} выставил защиту, и ты лишился {penalty} монет.",
    "😱 Засада! Ты напоролся на защиту и потерял {penalty} монет."
]

THEFT_VICTIM_DEFENSE_PHRASES = [
    "🛡️ Твоя защита сработала! {attacker} ничего не украл и потерял {penalty} монет.",
    "💪 Ты краш! Отбил атаку {attacker} и получил {penalty} монет.",
    "😎 Ха! {attacker} думал поживиться, а сам потерял {penalty} монет."
]

DICE_WIN_PHRASES = [
    "🎲 {dice1} + {dice2} = {total} — Победа! +{profit} монет!",
    "🎲 Круто! {dice1}+{dice2}={total}, ты выиграл {profit}!",
    "🎲 Хайп! {total} очков, твой выигрыш: {profit}!"
]

DICE_LOSE_PHRASES = [
    "🎲 {dice1} + {dice2} = {total} — Проигрыш. -{loss} монет.",
    "🎲 Эх, {total} очков, не повезло. -{loss}.",
    "🎲 В этот раз не зашло, -{loss} монет."
]

GUESS_WIN_PHRASES = [
    "🔢 Ты угадал! Было {secret}. Выигрыш: +{profit} монет и +{rep} репутации!",
    "🔢 Красава! Число {secret}, твой выигрыш {profit} монет!",
    "🔢 Хайпанул! +{profit} монет, репутация +{rep}!"
]

GUESS_LOSE_PHRASES = [
    "🔢 Не угадал. Было {secret}. -{loss} монет.",
    "🔢 Увы, загадано {secret}. Теряешь {loss} монет.",
    "🔢 Не фортануло, правильный ответ {secret}. -{loss}."
]

CHAT_WIN_PHRASES = [
    "🔥 {name} только что выиграл {amount} монет в казино!",
    "💰 Удача на стороне {name}: +{amount} монет!",
    "🎰 {name} сорвал куш — {amount} монет!"
]

CHAT_PURCHASE_PHRASES = [
    "🛒 {name} купил {item} за {price} монет!",
    "🎁 {name} приобрёл {item}! Админ уже в пути.",
    "💎 {name} потратил {price} монет на {item}!"
]

CHAT_GIVEAWAY_PHRASES = [
    "🎁 Не пропусти розыгрыш! Осталось {time}",
    "⏰ Напоминание: розыгрыш {prize} заканчивается через {time}",
    "🔥 Участвуй в розыгрыше {prize}! Осталось {time}"
]

BOSS_SPAWN_PHRASES = [
    "⚠️ ВНИМАНИЕ! В чате появился {name} (Уровень {level})! Здоровье: {hp}",
    "👾 Босс {name} пришёл навестить нас! Уровень {level}, HP: {hp}",
    "🔥 Легендарный {name} пробудился! Уровень {level}, здоровье: {hp}",
]

BOSS_HIT_PHRASES = [
    "💥 Ты нанёс {damage} урона!",
    "⚡️ Удар! -{damage} HP",
    "🔥 Критическое попадание! {damage} урона",
]

BOSS_MISS_PHRASES = [
    "💨 Промах! Босс уклонился",
    "😵 Твоя атака не достигла цели",
    "🛡 Босс отразил удар",
]

BOSS_DEATH_PHRASES = [
    "🏆 Босс {name} повержен! Все участники получают награду!",
    "🎉 Победа! {name} пал! Награда разделена между участниками",
    "💀 Босс уничтожен! Спасибо за участие!",
]

BOSS_STATUS_PHRASES = [
    "👾 {name} | Уровень {level} | HP: {current_hp}/{max_hp}",
]
# ===== КОНЕЦ ПЕРВОЙ ЧАСТИ =====
# ===== ВТОРАЯ ЧАСТЬ (ПОЛЬЗОВАТЕЛЬСКИЕ ХЕНДЛЕРЫ) =====

# ===== КОМАНДА HELP =====
@dp.message_handler(commands=['help'])
async def cmd_help(message: types.Message):
    if message.chat.type != 'private':
        return
    user_id = message.from_user.id
    if await is_banned(user_id) and not await is_admin(user_id):
        return
    ok, not_subscribed = await check_subscription(user_id)
    if not ok:
        await message.answer("❗️ Сначала подпишись на каналы.", reply_markup=subscription_inline(not_subscribed))
        return
    text = (
        "🤖 <b>Malboro GAME</b> – помощь:\n\n"
        "• 👤 Профиль – баланс, уровень, репутация, статы\n"
        "• 🎁 Бонус – ежедневная награда\n"
        "• 🛒 Магазин подарков – покупка подарков\n"
        "• 🎰 Казино – испытай удачу\n"
        "• 🎟 Промокод – активация промокодов\n"
        "• 🏆 Топ игроков – лучшие по балансу, уровню, статам\n"
        "• 💰 Мои покупки – история заказов\n"
        "• 🔫 Ограбить – укради монеты у другого (шанс зависит от репутации)\n"
        "• 🎲 Игры – кости, угадай число, мультиплеер 21\n"
        "• ⭐️ Репутация – твой авторитет (влияет на грабёж)\n"
        "• 📋 Задания – выполняй и получай награды\n"
        "• 🔗 Рефералка – приглашай друзей и получай бонусы\n"
        "• 👥 Комнатная игра 21 – мультиплеер\n"
        "• 📊 Уровень – твой прогресс и достижения\n"
        "• 👾 Боссы – сражения в групповых чатах\n"
        "• 🎁 Подгон – случайный подарок в чате (доступен в активированных чатах)\n\n"
        "Администраторы имеют дополнительные функции в панели."
    )
    await message.answer(text, reply_markup=user_main_keyboard(await is_admin(user_id)))

# ===== СТАРТ =====
@dp.message_handler(commands=['start'])
async def cmd_start(message: types.Message):
    if message.chat.type != 'private':
        return

    user_id = message.from_user.id
    if await is_banned(user_id) and not await is_admin(user_id):
        await message.answer("⛔ Вы заблокированы.")
        return

    args = message.get_args()
    if args and args.startswith('ref'):
        try:
            referrer_id = int(args[3:])
            if referrer_id != user_id and not await is_banned(referrer_id):
                async with db_pool.acquire() as conn:
                    existing = await conn.fetchval("SELECT 1 FROM referrals WHERE referred_id=$1", user_id)
                    if not existing:
                        await conn.execute(
                            "INSERT INTO referrals (referrer_id, referred_id, referred_date, reward_given) VALUES ($1, $2, $3, $4)",
                            referrer_id, user_id, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), False
                        )
                        await safe_send_message(referrer_id, f"🔗 Новый пользователь {message.from_user.first_name} зарегистрировался по вашей ссылке! Награда будет выдана после того, как он совершит 15 успешных ограблений.")
        except:
            pass

    username = message.from_user.username
    first_name = message.from_user.first_name
    try:
        async with db_pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO users (user_id, username, first_name, joined_date, balance, reputation, total_spent, negative_balance, game_wins, exp, level, strength, agility, defense) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14) ON CONFLICT (user_id) DO NOTHING",
                user_id, username, first_name, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 0, 0, 0, 0, 0, 0, 1, 1, 1, 1
            )
    except Exception as e:
        logging.error(f"DB error in start: {e}")
        await message.answer("❌ Ошибка базы данных. Попробуй позже.")
        return

    ok, not_subscribed = await check_subscription(user_id)
    if not ok:
        await message.answer(
            "❗️ Для доступа к боту нужно подписаться на наши каналы.\nПосле подписки нажми кнопку ниже.",
            reply_markup=subscription_inline(not_subscribed)
        )
        return
    admin_flag = await is_admin(user_id)
    await message.answer(
        f"Привет, {first_name}!\n"
        f"Добро пожаловать в <b>Malboro GAME</b>! 🚬\n"
        f"Тут ты найдёшь: казино, розыгрыши, магазин с подарками.\n"
        f"А ещё можешь грабить других (раз в 30 мин) – случайно или по username!\n"
        f"У тебя 1 уровень. Зарабатывай опыт и повышай уровень, улучшая силу, ловкость и защиту!\n\n"
        f"Канал: @lllMALBOROlll (подпишись, чтобы быть в теме)",
        reply_markup=user_main_keyboard(admin_flag)
    )

# ===== ПРОВЕРКА ПОДПИСКИ =====
@dp.callback_query_handler(lambda c: c.data == "check_sub")
async def check_sub_callback(callback: types.CallbackQuery):
    if callback.message.chat.type != 'private':
        await callback.answer("Эта функция работает только в личке", show_alert=True)
        return
    if await is_banned(callback.from_user.id) and not await is_admin(callback.from_user.id):
        await callback.answer("⛔ Вы заблокированы.", show_alert=True)
        return
    ok, not_subscribed = await check_subscription(callback.from_user.id)
    if ok:
        admin_flag = await is_admin(callback.from_user.id)
        await callback.message.edit_text("✅ Подписка подтверждена! Добро пожаловать.")
        await callback.message.answer("Главное меню:", reply_markup=user_main_keyboard(admin_flag))
    else:
        await callback.answer("❌ Ты ещё не подписался на все каналы!", show_alert=True)
        await callback.message.edit_reply_markup(reply_markup=subscription_inline(not_subscribed))

@dp.callback_query_handler(lambda c: c.data == "no_link")
async def no_link(callback: types.CallbackQuery):
    await callback.answer("Ссылка временно недоступна, найди канал вручную", show_alert=True)

# ===== ПРОФИЛЬ =====
@dp.message_handler(lambda message: message.text == "👤 Профиль")
async def profile_handler(message: types.Message):
    if message.chat.type != 'private':
        return
    user_id = message.from_user.id
    if await is_banned(user_id) and not await is_admin(user_id):
        return
    ok, not_subscribed = await check_subscription(user_id)
    if not ok:
        await message.answer("❗️ Сначала подпишись на каналы.", reply_markup=subscription_inline(not_subscribed))
        return
    try:
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT balance, reputation, total_spent, negative_balance, joined_date, theft_attempts, theft_success, theft_failed, theft_protected, casino_wins, casino_losses, guess_wins, guess_losses, game_wins, exp, level, strength, agility, defense FROM users WHERE user_id=$1",
                user_id
            )
        if row:
            balance, rep, spent, neg, joined, attempts, success, failed, protected, cw, cl, gw, gl, gwins, exp, level, strength, agility, defense = (
                row['balance'], row['reputation'], row['total_spent'], row['negative_balance'], row['joined_date'],
                row['theft_attempts'], row['theft_success'], row['theft_failed'], row['theft_protected'],
                row['casino_wins'], row['casino_losses'], row['guess_wins'], row['guess_losses'],
                row['game_wins'], row['exp'], row['level'], row['strength'], row['agility'], row['defense']
            )
            neg_text = f" (долг: {neg})" if neg > 0 else ""
            level_mult = int(await get_setting("level_multiplier"))
            exp_needed = level * level_mult
            progress = exp / exp_needed if exp_needed > 0 else 0
            bar_length = 10
            filled = int(progress * bar_length)
            bar = "🟩" * filled + "⬜" * (bar_length - filled)
            text = (
                f"👤 <b>Твой профиль</b>\n"
                f"📊 <b>Уровень:</b> {level}\n"
                f"📈 <b>Опыт:</b> {exp}/{exp_needed}\n{bar}\n"
                f"💪 Сила: {strength} | 🏃 Ловкость: {agility} | 🛡 Защита: {defense}\n"
                f"💰 Баланс: {balance} монет{neg_text}\n"
                f"⭐️ Репутация: {rep}\n"
                f"💸 Всего потрачено: {spent} монет\n"
                f"📅 Зарегистрирован: {joined}\n"
                f"🔫 Ограблений: {attempts} (успешно: {success}, провал: {failed})\n"
                f"⚔️ Отбито атак: {protected}\n"
                f"🎰 Казино: побед {cw}, поражений {cl}\n"
                f"🔢 Угадайка: побед {gw}, поражений {gl}\n"
                f"👥 Побед в мультиплеере: {gwins}"
            )
        else:
            text = "Профиль не найден"
    except Exception as e:
        logging.error(f"Profile error: {e}")
        text = "❌ Ошибка загрузки профиля."
    await message.answer(text, reply_markup=user_main_keyboard(await is_admin(user_id)))

# ===== УРОВЕНЬ =====
@dp.message_handler(lambda message: message.text == "📊 Уровень")
async def level_handler(message: types.Message):
    if message.chat.type != 'private':
        return
    user_id = message.from_user.id
    if await is_banned(user_id) and not await is_admin(user_id):
        return
    ok, not_subscribed = await check_subscription(user_id)
    if not ok:
        await message.answer("❗️ Сначала подпишись на каналы.", reply_markup=subscription_inline(not_subscribed))
        return
    level = await get_user_level(user_id)
    exp = await get_user_exp(user_id)
    level_mult = int(await get_setting("level_multiplier"))
    exp_needed = level * level_mult
    progress = exp / exp_needed if exp_needed > 0 else 0
    bar_length = 15
    filled = int(progress * bar_length)
    bar = "🟩" * filled + "⬜" * (bar_length - filled)
    level_names = {
        1: "🔰 Новичок",
        2: "⛏️ Искатель",
        3: "⚔️ Воин",
        4: "🛡️ Защитник",
        5: "🌟 Звезда",
        6: "🔥 Ветеран",
        7: "💫 Мастер",
        8: "👑 Легенда",
        9: "💎 Алмазный",
        10: "👁‍🗨 Патриарх",
    }
    level_name = level_names.get(level, f"Уровень {level}")
    text = (
        f"📊 <b>{level_name}</b>\n\n"
        f"Уровень: {level}\n"
        f"Опыт: {exp} / {exp_needed}\n"
        f"{bar}\n\n"
        f"За повышение уровня ты получаешь монеты, репутацию и очки статов!\n"
        f"Следующая награда: +{await get_level_reward_coins(level+1)} монет, +{await get_level_reward_rep(level+1)} репутации."
    )
    await message.answer(text, reply_markup=user_main_keyboard(await is_admin(user_id)))

async def get_level_reward_coins(level: int) -> int:
    async with db_pool.acquire() as conn:
        val = await conn.fetchval("SELECT coins FROM level_rewards WHERE level=$1", level)
        return val if val else 0

async def get_level_reward_rep(level: int) -> int:
    async with db_pool.acquire() as conn:
        val = await conn.fetchval("SELECT reputation FROM level_rewards WHERE level=$1", level)
        return val if val else 0

# ===== РЕПУТАЦИЯ =====
@dp.message_handler(lambda message: message.text == "⭐️ Репутация")
async def reputation_handler(message: types.Message):
    if message.chat.type != 'private':
        return
    user_id = message.from_user.id
    if await is_banned(user_id) and not await is_admin(user_id):
        return
    ok, not_subscribed = await check_subscription(user_id)
    if not ok:
        await message.answer("❗️ Сначала подпишись на каналы.", reply_markup=subscription_inline(not_subscribed))
        return
    rep = await get_user_reputation(user_id)
    theft_bonus = float(await get_setting("reputation_theft_bonus")) * rep
    defense_bonus = float(await get_setting("reputation_defense_bonus")) * rep
    await message.answer(
        f"⭐️ Твоя репутация: {rep}\n\n"
        f"Репутация увеличивает шансы:\n"
        f"🔫 Бонус к грабежу: +{theft_bonus:.1f}%\n"
        f"🛡 Бонус к защите: +{defense_bonus:.1f}%\n\n"
        f"Зарабатывай репутацию в играх и за выполнение заданий!",
        reply_markup=user_main_keyboard(await is_admin(user_id))
    )

# ===== БОНУС =====
@dp.message_handler(lambda message: message.text == "🎁 Бонус")
async def bonus_handler(message: types.Message):
    if message.chat.type != 'private':
        return
    user_id = message.from_user.id
    if await is_banned(user_id) and not await is_admin(user_id):
        return
    ok, not_subscribed = await check_subscription(user_id)
    if not ok:
        await message.answer("❗️ Сначала подпишись на каналы.", reply_markup=subscription_inline(not_subscribed))
        return
    try:
        async with db_pool.acquire() as conn:
            last_bonus_str = await conn.fetchval("SELECT last_bonus FROM users WHERE user_id=$1", user_id)

        now = datetime.now()
        if last_bonus_str:
            last_bonus = datetime.strptime(last_bonus_str, "%Y-%m-%d %H:%M:%S")
            if now - last_bonus < timedelta(days=1):
                remaining = timedelta(days=1) - (now - last_bonus)
                hours = remaining.seconds // 3600
                minutes = (remaining.seconds // 60) % 60
                await message.answer(f"⏳ Бонус можно будет получить через {hours} ч {minutes} мин")
                return

        bonus = random.randint(5, 15)
        phrase = random.choice(BONUS_PHRASES).format(bonus=bonus)

        async with db_pool.acquire() as conn:
            await conn.execute(
                "UPDATE users SET balance = balance + $1, last_bonus = $2 WHERE user_id=$3",
                bonus, now.strftime("%Y-%m-%d %H:%M:%S"), user_id
            )
        await message.answer(phrase, reply_markup=user_main_keyboard(await is_admin(user_id)))
    except Exception as e:
        logging.error(f"Bonus error: {e}")
        await message.answer("❌ Ошибка при получении бонуса.")

# ===== ТОП ИГРОКОВ =====
@dp.message_handler(lambda message: message.text == "🏆 Топ игроков")
async def leaderboard_menu(message: types.Message):
    if message.chat.type != 'private':
        return
    user_id = message.from_user.id
    if await is_banned(user_id) and not await is_admin(user_id):
        return
    ok, not_subscribed = await check_subscription(user_id)
    if not ok:
        await message.answer("❗️ Сначала подпишись на каналы.", reply_markup=subscription_inline(not_subscribed))
        return
    kb = ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="💰 Самые богатые")],
        [KeyboardButton(text="💸 Транжиры")],
        [KeyboardButton(text="🔫 Крадуны")],
        [KeyboardButton(text="⭐️ По репутации")],
        [KeyboardButton(text="👥 Победы в мультиплеере")],
        [KeyboardButton(text="📈 По уровню")],
        [KeyboardButton(text="💪 По силе")],
        [KeyboardButton(text="🏃 По ловкости")],
        [KeyboardButton(text="🛡 По защите")],
        [KeyboardButton(text="◀️ Назад")]
    ], resize_keyboard=True)
    await message.answer("Выбери категорию топа:", reply_markup=kb)

@dp.message_handler(lambda message: message.text == "💰 Самые богатые")
async def top_rich_handler(message: types.Message):
    if message.chat.type != 'private':
        return
    await show_top(message, "balance", "💰 Самые богатые")

@dp.message_handler(lambda message: message.text == "💸 Транжиры")
async def top_spenders_handler(message: types.Message):
    if message.chat.type != 'private':
        return
    await show_top(message, "total_spent", "💸 Транжиры (по потраченным монетам)")

@dp.message_handler(lambda message: message.text == "🔫 Крадуны")
async def top_thieves_handler(message: types.Message):
    if message.chat.type != 'private':
        return
    await show_top(message, "theft_success", "🔫 Крадуны (успешные ограбления)")

@dp.message_handler(lambda message: message.text == "⭐️ По репутации")
async def top_reputation_handler(message: types.Message):
    if message.chat.type != 'private':
        return
    await show_top(message, "reputation", "⭐️ По репутации")

@dp.message_handler(lambda message: message.text == "👥 Победы в мультиплеере")
async def top_multiplayer_handler(message: types.Message):
    if message.chat.type != 'private':
        return
    await show_top(message, "game_wins", "👥 Победы в мультиплеере")

@dp.message_handler(lambda message: message.text == "📈 По уровню")
async def top_level_handler(message: types.Message):
    if message.chat.type != 'private':
        return
    await show_top(message, "level", "📈 По уровню")

@dp.message_handler(lambda message: message.text == "💪 По силе")
async def top_strength_handler(message: types.Message):
    if message.chat.type != 'private':
        return
    await show_top(message, "strength", "💪 По силе")

@dp.message_handler(lambda message: message.text == "🏃 По ловкости")
async def top_agility_handler(message: types.Message):
    if message.chat.type != 'private':
        return
    await show_top(message, "agility", "🏃 По ловкости")

@dp.message_handler(lambda message: message.text == "🛡 По защите")
async def top_defense_handler(message: types.Message):
    if message.chat.type != 'private':
        return
    await show_top(message, "defense", "🛡 По защите")

async def show_top(message: types.Message, order_field: str, title: str):
    page = 1
    try:
        parts = message.text.split()
        if len(parts) > 1:
            page = int(parts[1])
    except:
        pass
    offset = (page - 1) * ITEMS_PER_PAGE
    try:
        async with db_pool.acquire() as conn:
            total = await conn.fetchval(f"SELECT COUNT(*) FROM users")
            rows = await conn.fetch(
                f"SELECT first_name, {order_field} FROM users ORDER BY {order_field} DESC LIMIT $1 OFFSET $2",
                ITEMS_PER_PAGE, offset
            )
        if not rows:
            await message.answer("Нет данных.")
            return
        text = f"{title} (страница {page}):\n\n"
        for idx, row in enumerate(rows, start=offset+1):
            text += f"{idx}. {row['first_name']} – {row[order_field]}\n"
        kb = []
        nav_buttons = []
        if page > 1:
            nav_buttons.append(InlineKeyboardButton(text="⬅️", callback_data=f"top:{order_field}:{page-1}"))
        if offset + ITEMS_PER_PAGE < total:
            nav_buttons.append(InlineKeyboardButton(text="➡️", callback_data=f"top:{order_field}:{page+1}"))
        if nav_buttons:
            kb.append(nav_buttons)
        if kb:
            await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))
        else:
            await message.answer(text)
    except Exception as e:
        logging.error(f"Top error: {e}")
        await message.answer("❌ Ошибка загрузки топа.")

@dp.callback_query_handler(lambda c: c.data.startswith("top:"))
async def top_page_callback(callback: types.CallbackQuery):
    parts = callback.data.split(":")
    field = parts[1]
    page = int(parts[2])
    titles = {
        "balance": "💰 Самые богатые",
        "total_spent": "💸 Транжиры",
        "theft_success": "🔫 Крадуны",
        "reputation": "⭐️ По репутации",
        "game_wins": "👥 Победы в мультиплеере",
        "level": "📈 По уровню",
        "strength": "💪 По силе",
        "agility": "🏃 По ловкости",
        "defense": "🛡 По защите"
    }
    title = titles.get(field, "Топ")
    await show_top(callback.message, field, title)
    await callback.answer()

# ===== МАГАЗИН ПОДАРКОВ =====
@dp.message_handler(lambda message: message.text == "🛒 Магазин подарков")
async def shop_handler(message: types.Message):
    if message.chat.type != 'private':
        return
    user_id = message.from_user.id
    if await is_banned(user_id) and not await is_admin(user_id):
        return
    ok, not_subscribed = await check_subscription(user_id)
    if not ok:
        await message.answer("❗️ Сначала подпишись на каналы.", reply_markup=subscription_inline(not_subscribed))
        return
    page = 1
    try:
        parts = message.text.split()
        if len(parts) > 1:
            page = int(parts[1])
    except:
        pass
    offset = (page - 1) * ITEMS_PER_PAGE
    try:
        async with db_pool.acquire() as conn:
            total = await conn.fetchval("SELECT COUNT(*) FROM shop_items")
            rows = await conn.fetch(
                "SELECT id, name, description, price, stock FROM shop_items ORDER BY id LIMIT $1 OFFSET $2",
                ITEMS_PER_PAGE, offset
            )
        if not rows:
            await message.answer("🎁 В магазине пока нет подарков.")
            return
        text = f"🎁 Подарки (страница {page}):\n\n"
        kb = []
        for row in rows:
            item_id = row['id']
            name = row['name']
            desc = row['description']
            price = row['price']
            stock = row['stock']
            stock_info = f" (в наличии: {stock})" if stock != -1 else ""
            text += f"🔹 {name}\n{desc}\n💰 {price} монет{stock_info}\n\n"
            kb.append([InlineKeyboardButton(text=f"Купить {name}", callback_data=f"buy_{item_id}")])
        nav_buttons = []
        if page > 1:
            nav_buttons.append(InlineKeyboardButton(text="⬅️", callback_data=f"shop_page_{page-1}"))
        if offset + ITEMS_PER_PAGE < total:
            nav_buttons.append(InlineKeyboardButton(text="➡️", callback_data=f"shop_page_{page+1}"))
        if nav_buttons:
            kb.append(nav_buttons)
        await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))
    except Exception as e:
        logging.error(f"Shop error: {e}")
        await message.answer("❌ Ошибка загрузки магазина.")

@dp.callback_query_handler(lambda c: c.data.startswith("shop_page_"))
async def shop_page_callback(callback: types.CallbackQuery):
    page = int(callback.data.split("_")[2])
    callback.message.text = f"🛒 Магазин подарков {page}"
    await shop_handler(callback.message)
    await callback.answer()

@dp.callback_query_handler(lambda c: c.data.startswith("buy_"))
async def buy_callback(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    if await is_banned(user_id) and not await is_admin(user_id):
        await callback.answer("⛔ Вы заблокированы.", show_alert=True)
        return
    ok, not_subscribed = await check_subscription(user_id)
    if not ok:
        await callback.message.edit_text("❗️ Сначала подпишись на каналы.", reply_markup=subscription_inline(not_subscribed))
        return
    item_id = int(callback.data.split("_")[1])
    try:
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow("SELECT name, price, stock FROM shop_items WHERE id=$1", item_id)
            if not row:
                await callback.answer("Товар не найден", show_alert=True)
                return
            name, price, stock = row['name'], row['price'], row['stock']
            if stock != -1 and stock <= 0:
                await callback.answer("Товара нет в наличии!", show_alert=True)
                return
            balance = await conn.fetchval("SELECT balance FROM users WHERE user_id=$1", user_id)
            if balance is None:
                await conn.execute(
                    "INSERT INTO users (user_id, username, first_name, joined_date) VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING",
                    user_id, callback.from_user.username, callback.from_user.first_name, datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                )
                balance = 0
            if balance < price:
                await callback.answer("Не хватает монет!", show_alert=True)
                return
            async with conn.transaction():
                await conn.execute("UPDATE users SET balance = balance - $1 WHERE user_id=$2", price, user_id)
                await conn.execute("UPDATE users SET total_spent = total_spent + $1 WHERE user_id=$2", price, user_id)
                await conn.execute(
                    "INSERT INTO purchases (user_id, item_id, purchase_date) VALUES ($1, $2, $3)",
                    user_id, item_id, datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                )
                if stock != -1:
                    await conn.execute("UPDATE shop_items SET stock = stock - 1 WHERE id=$1", item_id)

        phrase = random.choice(PURCHASE_PHRASES)
        await callback.answer(f"✅ Ты купил {name}! {phrase}", show_alert=True)

        if await get_setting("chat_notify_big_purchase") == "1" and price >= BIG_PURCHASE_THRESHOLD:
            user = callback.from_user
            chat_phrase = random.choice(CHAT_PURCHASE_PHRASES).format(name=user.first_name, item=name, price=price)
            await notify_chats(chat_phrase, 'purchase')

        asyncio.create_task(notify_admins_about_purchase(callback.from_user, name, price))
        try:
            await callback.message.edit_text(f"✅ Покупка совершена!")
        except (MessageNotModified, MessageToEditNotFound):
            pass
        await callback.message.answer("Главное меню:", reply_markup=user_main_keyboard(await is_admin(user_id)))
    except Exception as e:
        logging.error(f"Purchase error: {e}")
        await callback.answer("❌ Ошибка при покупке. Попробуй позже.", show_alert=True)

async def notify_admins_about_purchase(user: types.User, item_name: str, price: int):
    admins = SUPER_ADMINS.copy()
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT user_id FROM admins")
        for row in rows:
            admins.append(row['user_id'])
    for admin_id in admins:
        await safe_send_message(admin_id,
            f"🛒 Покупка: пользователь {user.full_name} (@{user.username})\n"
            f"<a href=\"tg://user?id={user.id}\">Ссылка</a> купил {item_name} за {price} монет."
        )

# ===== МОИ ПОКУПКИ =====
@dp.message_handler(lambda message: message.text == "💰 Мои покупки")
async def my_purchases(message: types.Message):
    if message.chat.type != 'private':
        return
    user_id = message.from_user.id
    if await is_banned(user_id) and not await is_admin(user_id):
        return
    ok, not_subscribed = await check_subscription(user_id)
    if not ok:
        await message.answer("❗️ Сначала подпишись на каналы.", reply_markup=subscription_inline(not_subscribed))
        return
    page = 1
    try:
        parts = message.text.split()
        if len(parts) > 1:
            page = int(parts[1])
    except:
        pass
    offset = (page - 1) * ITEMS_PER_PAGE
    try:
        async with db_pool.acquire() as conn:
            total = await conn.fetchval("SELECT COUNT(*) FROM purchases WHERE user_id=$1", user_id)
            rows = await conn.fetch(
                "SELECT p.id, s.name, p.purchase_date, p.status, p.admin_comment FROM purchases p "
                "JOIN shop_items s ON p.item_id = s.id WHERE p.user_id=$1 ORDER BY p.purchase_date DESC LIMIT $2 OFFSET $3",
                user_id, ITEMS_PER_PAGE, offset
            )
        if not rows:
            await message.answer("У тебя пока нет покупок.", reply_markup=user_main_keyboard(await is_admin(user_id)))
            return
        text = f"📦 Твои покупки (страница {page}):\n"
        for row in rows:
            pid, name, date, status, comment = row['id'], row['name'], row['purchase_date'], row['status'], row['admin_comment']
            status_emoji = "⏳" if status == 'pending' else "✅" if status == 'completed' else "❌"
            text += f"{status_emoji} {name} от {date}\n"
            if comment:
                text += f"   Комментарий: {comment}\n"
        kb = []
        nav_buttons = []
        if page > 1:
            nav_buttons.append(InlineKeyboardButton(text="⬅️", callback_data=f"mypurchases_page_{page-1}"))
        if offset + ITEMS_PER_PAGE < total:
            nav_buttons.append(InlineKeyboardButton(text="➡️", callback_data=f"mypurchases_page_{page+1}"))
        if nav_buttons:
            kb.append(nav_buttons)
        if kb:
            await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))
        else:
            await message.answer(text, reply_markup=user_main_keyboard(await is_admin(user_id)))
    except Exception as e:
        logging.error(f"My purchases error: {e}")
        await message.answer("❌ Ошибка загрузки покупок.")

@dp.callback_query_handler(lambda c: c.data.startswith("mypurchases_page_"))
async def mypurchases_page_callback(callback: types.CallbackQuery):
    page = int(callback.data.split("_")[2])
    callback.message.text = f"💰 Мои покупки {page}"
    await my_purchases(callback.message)
    await callback.answer()

# ===== КАЗИНО =====
@dp.message_handler(lambda message: message.text == "🎰 Казино")
async def casino_handler(message: types.Message):
    if message.chat.type != 'private':
        return
    user_id = message.from_user.id
    if await is_banned(user_id) and not await is_admin(user_id):
        return
    ok, not_subscribed = await check_subscription(user_id)
    if not ok:
        await message.answer("❗️ Сначала подпишись на каналы.", reply_markup=subscription_inline(not_subscribed))
        return
    await message.answer("🎰 Введи сумму ставки (целое число):", reply_markup=back_keyboard())
    await CasinoBet.amount.set()

@dp.message_handler(state=CasinoBet.amount)
async def casino_bet_amount(message: types.Message, state: FSMContext):
    if message.chat.type != 'private':
        await state.finish()
        return
    if message.text == "◀️ Назад":
        await state.finish()
        await message.answer("Главное меню:", reply_markup=user_main_keyboard(await is_admin(message.from_user.id)))
        return
    try:
        amount = int(message.text)
    except ValueError:
        await message.answer("❌ Введите целое число.")
        return
    if amount <= 0:
        await message.answer("❌ Ставка должна быть положительной.")
        return
    user_id = message.from_user.id
    ok, not_subscribed = await check_subscription(user_id)
    if not ok:
        await message.answer("❗️ Сначала подпишись на каналы.", reply_markup=subscription_inline(not_subscribed))
        await state.finish()
        return
    try:
        win_chance = int(await get_setting("casino_win_chance")) / 100
        async with db_pool.acquire() as conn:
            balance = await conn.fetchval("SELECT balance FROM users WHERE user_id=$1", user_id)
            if balance is None:
                await conn.execute(
                    "INSERT INTO users (user_id, username, first_name, joined_date) VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING",
                    user_id, message.from_user.username, message.from_user.first_name, datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                )
                balance = 0
            if amount > balance:
                await message.answer("❌ Недостаточно монет.")
                await state.finish()
                return
            win = random.random() < win_chance
            if win:
                await conn.execute("UPDATE users SET balance = balance + $1, casino_wins = casino_wins + 1 WHERE user_id=$2", amount, user_id)
                profit = amount
                win_amount = amount * 2
                phrase = random.choice(CASINO_WIN_PHRASES).format(win=win_amount, profit=profit)
                exp = int(await get_setting("exp_per_casino_win"))
                await add_exp(user_id, exp)
                exp_text = f" +{exp} опыта"
                if await get_setting("chat_notify_big_win") == "1" and win_amount >= BIG_WIN_THRESHOLD:
                    user = message.from_user
                    chat_phrase = random.choice(CHAT_WIN_PHRASES).format(name=user.first_name, amount=win_amount)
                    await notify_chats(chat_phrase, 'win')
            else:
                await conn.execute("UPDATE users SET balance = balance - $1, casino_losses = casino_losses + 1 WHERE user_id=$2", amount, user_id)
                phrase = random.choice(CASINO_LOSE_PHRASES).format(loss=amount)
                exp = int(await get_setting("exp_per_casino_lose"))
                await add_exp(user_id, exp)
                exp_text = f" +{exp} опыта"
            new_balance = await conn.fetchval("SELECT balance FROM users WHERE user_id=$1", user_id)
        await message.answer(
            f"{phrase}\n💰 Текущий баланс: {new_balance}{exp_text}",
            reply_markup=user_main_keyboard(await is_admin(user_id))
        )
    except Exception as e:
        logging.error(f"Casino error: {e}")
        await message.answer("❌ Ошибка в казино.")
    await state.finish()

# ===== ИГРЫ =====
@dp.message_handler(lambda message: message.text == "🎲 Игры")
async def games_menu(message: types.Message):
    if message.chat.type != 'private':
        return
    user_id = message.from_user.id
    if await is_banned(user_id) and not await is_admin(user_id):
        return
    ok, not_subscribed = await check_subscription(user_id)
    if not ok:
        await message.answer("❗️ Сначала подпишись на каналы.", reply_markup=subscription_inline(not_subscribed))
        return
    await message.answer("Выбери игру:", reply_markup=games_keyboard())

@dp.message_handler(lambda message: message.text == "🎲 Кости")
async def dice_game(message: types.Message):
    if message.chat.type != 'private':
        return
    user_id = message.from_user.id
    if await is_banned(user_id) and not await is_admin(user_id):
        return
    ok, not_subscribed = await check_subscription(user_id)
    if not ok:
        await message.answer("❗️ Сначала подпишись на каналы.", reply_markup=subscription_inline(not_subscribed))
        return
    await message.answer("🎲 Введи сумму ставки (целое число):", reply_markup=back_keyboard())
    await DiceBet.amount.set()

@dp.message_handler(state=DiceBet.amount)
async def dice_bet_amount(message: types.Message, state: FSMContext):
    if message.chat.type != 'private':
        await state.finish()
        return
    if message.text == "◀️ Назад":
        await state.finish()
        await games_menu(message)
        return
    try:
        amount = int(message.text)
    except ValueError:
        await message.answer("❌ Введите целое число.")
        return
    if amount <= 0:
        await message.answer("❌ Ставка должна быть положительной.")
        return
    user_id = message.from_user.id
    balance = await get_user_balance(user_id)
    if amount > balance:
        await message.answer("❌ Недостаточно монет.")
        await state.finish()
        return

    dice1 = random.randint(1, 6)
    dice2 = random.randint(1, 6)
    total = dice1 + dice2
    multiplier = int(await get_setting("dice_multiplier"))

    if total > 7:
        profit = amount * multiplier
        await update_user_balance(user_id, profit)
        phrase = random.choice(DICE_WIN_PHRASES).format(dice1=dice1, dice2=dice2, total=total, profit=profit)
        exp = int(await get_setting("exp_per_dice_win"))
        await add_exp(user_id, exp)
        exp_text = f" +{exp} опыта"
    else:
        await update_user_balance(user_id, -amount)
        phrase = random.choice(DICE_LOSE_PHRASES).format(dice1=dice1, dice2=dice2, total=total, loss=amount)
        exp = int(await get_setting("exp_per_dice_lose"))
        await add_exp(user_id, exp)
        exp_text = f" +{exp} опыта"

    new_balance = await get_user_balance(user_id)
    await message.answer(f"{phrase}\n💰 Баланс: {new_balance}{exp_text}")
    await state.finish()

@dp.message_handler(lambda message: message.text == "🔢 Угадай число")
async def guess_game(message: types.Message):
    if message.chat.type != 'private':
        return
    user_id = message.from_user.id
    if await is_banned(user_id) and not await is_admin(user_id):
        return
    ok, not_subscribed = await check_subscription(user_id)
    if not ok:
        await message.answer("❗️ Сначала подпишись на каналы.", reply_markup=subscription_inline(not_subscribed))
        return
    await message.answer("🔢 Введи сумму ставки (целое число):", reply_markup=back_keyboard())
    await GuessBet.amount.set()

@dp.message_handler(state=GuessBet.amount)
async def guess_bet_amount(message: types.Message, state: FSMContext):
    if message.chat.type != 'private':
        await state.finish()
        return
    if message.text == "◀️ Назад":
        await state.finish()
        await games_menu(message)
        return
    try:
        amount = int(message.text)
    except ValueError:
        await message.answer("❌ Введите целое число.")
        return
    if amount <= 0:
        await message.answer("❌ Ставка должна быть положительной.")
        return
    user_id = message.from_user.id
    balance = await get_user_balance(user_id)
    if amount > balance:
        await message.answer("❌ Недостаточно монет.")
        await state.finish()
        return
    await state.update_data(amount=amount)
    await message.answer("🔢 Загадай число от 1 до 5:", reply_markup=back_keyboard())
    await GuessBet.number.set()

@dp.message_handler(state=GuessBet.number)
async def guess_bet_number(message: types.Message, state: FSMContext):
    if message.chat.type != 'private':
        await state.finish()
        return
    if message.text == "◀️ Назад":
        await state.finish()
        await games_menu(message)
        return
    try:
        guess = int(message.text)
        if guess < 1 or guess > 5:
            raise ValueError
    except ValueError:
        await message.answer("❌ Введите число от 1 до 5.")
        return
    data = await state.get_data()
    amount = data['amount']
    user_id = message.from_user.id

    secret = random.randint(1, 5)
    multiplier = int(await get_setting("guess_multiplier"))
    rep_reward = int(await get_setting("guess_reputation"))

    if guess == secret:
        profit = amount * multiplier
        await update_user_balance(user_id, profit)
        await update_user_reputation(user_id, rep_reward)
        async with db_pool.acquire() as conn:
            await conn.execute("UPDATE users SET guess_wins = guess_wins + 1 WHERE user_id=$1", user_id)
        phrase = random.choice(GUESS_WIN_PHRASES).format(secret=secret, profit=profit, rep=rep_reward)
        exp = int(await get_setting("exp_per_guess_win"))
        await add_exp(user_id, exp)
        exp_text = f" +{exp} опыта"
    else:
        await update_user_balance(user_id, -amount)
        async with db_pool.acquire() as conn:
            await conn.execute("UPDATE users SET guess_losses = guess_losses + 1 WHERE user_id=$1", user_id)
        phrase = random.choice(GUESS_LOSE_PHRASES).format(secret=secret, loss=amount)
        exp = int(await get_setting("exp_per_guess_lose"))
        await add_exp(user_id, exp)
        exp_text = f" +{exp} опыта"

    new_balance = await get_user_balance(user_id)
    new_rep = await get_user_reputation(user_id)
    await message.answer(f"{phrase}\n💰 Баланс: {new_balance}\n⭐️ Репутация: {new_rep}{exp_text}")
    await state.finish()

# ===== ПРОМОКОД =====
@dp.message_handler(lambda message: message.text == "🎟 Промокод")
async def promo_handler(message: types.Message):
    if message.chat.type != 'private':
        return
    user_id = message.from_user.id
    if await is_banned(user_id) and not await is_admin(user_id):
        return
    ok, not_subscribed = await check_subscription(user_id)
    if not ok:
        await message.answer("❗️ Сначала подпишись на каналы.", reply_markup=subscription_inline(not_subscribed))
        return
    await message.answer("Введи промокод:", reply_markup=back_keyboard())
    await PromoActivate.code.set()

@dp.message_handler(state=PromoActivate.code)
async def promo_activate(message: types.Message, state: FSMContext):
    if message.chat.type != 'private':
        await state.finish()
        return
    if message.text == "◀️ Назад":
        await state.finish()
        await message.answer("Главное меню:", reply_markup=user_main_keyboard(await is_admin(message.from_user.id)))
        return
    code = message.text.strip().upper()
    user_id = message.from_user.id
    ok, not_subscribed = await check_subscription(user_id)
    if not ok:
        await message.answer("❗️ Сначала подпишись на каналы.", reply_markup=subscription_inline(not_subscribed))
        await state.finish()
        return
    try:
        async with db_pool.acquire() as conn:
            already_used = await conn.fetchval(
                "SELECT 1 FROM promo_activations WHERE user_id=$1 AND promo_code=$2",
                user_id, code
            )
            if already_used:
                await message.answer("❌ Ты уже активировал этот промокод.")
                await state.finish()
                return
            row = await conn.fetchrow("SELECT reward, max_uses, used_count FROM promocodes WHERE code=$1", code)
            if not row:
                await message.answer("❌ Промокод не найден.")
                await state.finish()
                return
            reward, max_uses, used = row['reward'], row['max_uses'], row['used_count']
            if used >= max_uses:
                await message.answer("❌ Промокод уже использован максимальное количество раз.")
                await state.finish()
                return
            async with conn.transaction():
                await conn.execute("UPDATE users SET balance = balance + $1 WHERE user_id=$2", reward, user_id)
                await conn.execute("UPDATE promocodes SET used_count = used_count + 1 WHERE code=$1", code)
                await conn.execute(
                    "INSERT INTO promo_activations (user_id, promo_code, activated_at) VALUES ($1, $2, $3)",
                    user_id, code, datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                )
        await message.answer(
            f"✅ Промокод активирован! Ты получил {reward} монет.",
            reply_markup=user_main_keyboard(await is_admin(user_id))
        )
    except Exception as e:
        logging.error(f"Promo error: {e}")
        await message.answer("❌ Ошибка активации промокода.")
    await state.finish()

# ===== РОЗЫГРЫШИ =====
@dp.message_handler(lambda message: message.text == "🎲 Розыгрыши")
async def giveaways_handler(message: types.Message):
    if message.chat.type != 'private':
        return
    user_id = message.from_user.id
    if await is_banned(user_id) and not await is_admin(user_id):
        return
    ok, not_subscribed = await check_subscription(user_id)
    if not ok:
        await message.answer("❗️ Сначала подпишись на каналы.", reply_markup=subscription_inline(not_subscribed))
        return
    page = 1
    try:
        parts = message.text.split()
        if len(parts) > 1:
            page = int(parts[1])
    except:
        pass
    offset = (page - 1) * ITEMS_PER_PAGE
    try:
        async with db_pool.acquire() as conn:
            total = await conn.fetchval("SELECT COUNT(*) FROM giveaways WHERE status='active'")
            rows = await conn.fetch(
                "SELECT id, prize, end_date FROM giveaways WHERE status='active' ORDER BY end_date LIMIT $1 OFFSET $2",
                ITEMS_PER_PAGE, offset
            )
        if not rows:
            await message.answer(
                "Сейчас нет активных розыгрышей.",
                reply_markup=user_main_keyboard(await is_admin(user_id))
            )
            return
        text = f"🎁 Активные розыгрыши (страница {page}):\n\n"
        kb = []
        for row in rows:
            gid, prize, end = row['id'], row['prize'], row['end_date']
            async with db_pool.acquire() as conn2:
                count = await conn2.fetchval("SELECT COUNT(*) FROM participants WHERE giveaway_id=$1", gid)
            text += f"ID: {gid} | {prize} | до {end} | 👥 {count} участников\n"
            kb.append([InlineKeyboardButton(text=f"🔍 Подробнее о {prize}", callback_data=f"detail_{gid}")])
        nav_buttons = []
        if page > 1:
            nav_buttons.append(InlineKeyboardButton(text="⬅️", callback_data=f"giveaways_page_{page-1}"))
        if offset + ITEMS_PER_PAGE < total:
            nav_buttons.append(InlineKeyboardButton(text="➡️", callback_data=f"giveaways_page_{page+1}"))
        if nav_buttons:
            kb.append(nav_buttons)
        kb.append([InlineKeyboardButton(text="« Назад", callback_data="back_main")])
        await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))
    except Exception as e:
        logging.error(f"Giveaways list error: {e}")
        await message.answer("❌ Ошибка загрузки розыгрышей.")

@dp.callback_query_handler(lambda c: c.data.startswith("giveaways_page_"))
async def giveaways_page_callback(callback: types.CallbackQuery):
    page = int(callback.data.split("_")[2])
    callback.message.text = f"🎲 Розыгрыши {page}"
    await giveaways_handler(callback.message)
    await callback.answer()

@dp.callback_query_handler(lambda c: c.data.startswith("detail_"))
async def giveaway_detail(callback: types.CallbackQuery):
    if await is_banned(callback.from_user.id) and not await is_admin(callback.from_user.id):
        await callback.answer("⛔ Вы заблокированы.", show_alert=True)
        return
    giveaway_id = int(callback.data.split("_")[1])
    try:
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT prize, description, end_date, media_file_id, media_type FROM giveaways WHERE id=$1 AND status='active'",
                giveaway_id
            )
            participants_count = await conn.fetchval("SELECT COUNT(*) FROM participants WHERE giveaway_id=$1", giveaway_id)
        if not row:
            await callback.answer("Розыгрыш не найден или завершён.", show_alert=True)
            return
        prize, desc, end_date, media_file_id, media_type = row['prize'], row['description'], row['end_date'], row['media_file_id'], row['media_type']
        caption = f"🎁 Розыгрыш: {prize}\n📝 {desc}\n📅 Окончание: {end_date}\n👥 Участников: {participants_count}\n\nЖелаешь участвовать?"
        confirm_kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Да, участвую", callback_data=f"confirm_part_{giveaway_id}")],
            [InlineKeyboardButton(text="❌ Нет", callback_data="cancel_detail")]
        ])
        if media_file_id and media_type:
            if media_type == 'photo':
                await callback.message.answer_photo(photo=media_file_id, caption=caption, reply_markup=confirm_kb)
            elif media_type == 'video':
                await callback.message.answer_video(video=media_file_id, caption=caption, reply_markup=confirm_kb)
            elif media_type == 'document':
                await callback.message.answer_document(document=media_file_id, caption=caption, reply_markup=confirm_kb)
        else:
            await callback.message.answer(caption, reply_markup=confirm_kb)
        await callback.answer()
    except Exception as e:
        logging.error(f"Giveaway detail error: {e}")
        await callback.answer("Ошибка загрузки деталей.", show_alert=True)

@dp.callback_query_handler(lambda c: c.data.startswith("confirm_part_"))
async def confirm_participation(callback: types.CallbackQuery):
    if await is_banned(callback.from_user.id) and not await is_admin(callback.from_user.id):
        await callback.answer("⛔ Вы заблокированы.", show_alert=True)
        return
    giveaway_id = int(callback.data.split("_")[2])
    user_id = callback.from_user.id
    ok, not_subscribed = await check_subscription(user_id)
    if not ok:
        await callback.message.edit_text("❗️ Сначала подпишись на каналы.", reply_markup=subscription_inline(not_subscribed))
        return
    try:
        async with db_pool.acquire() as conn:
            status = await conn.fetchval("SELECT status FROM giveaways WHERE id=$1", giveaway_id)
            if not status or status != 'active':
                await callback.answer("Розыгрыш не активен", show_alert=True)
                return
            await conn.execute("INSERT INTO participants (user_id, giveaway_id) VALUES ($1, $2) ON CONFLICT DO NOTHING", user_id, giveaway_id)
        await callback.answer("✅ Ты участвуешь в розыгрыше!", show_alert=True)
        await giveaways_handler(callback.message)
    except Exception as e:
        logging.error(f"Participation error: {e}")
        await callback.answer("Ошибка при участии.", show_alert=True)

@dp.callback_query_handler(lambda c: c.data == "cancel_detail")
async def cancel_detail(callback: types.CallbackQuery):
    if await is_banned(callback.from_user.id) and not await is_admin(callback.from_user.id):
        return
    await callback.message.delete()
    await giveaways_handler(callback.message)

@dp.callback_query_handler(lambda c: c.data == "back_main")
async def back_main_callback(callback: types.CallbackQuery):
    if await is_banned(callback.from_user.id) and not await is_admin(callback.from_user.id):
        return
    admin_flag = await is_admin(callback.from_user.id)
    await callback.message.delete()
    await callback.message.answer("Главное меню:", reply_markup=user_main_keyboard(admin_flag))

# ===== ОГРАБЛЕНИЕ (с учётом репутации) =====
async def get_theft_success_chance(attacker_id: int) -> float:
    base = int(await get_setting("theft_success_chance"))
    rep = await get_user_reputation(attacker_id)
    bonus = float(await get_setting("reputation_theft_bonus")) * rep
    return base + bonus

async def get_defense_chance(victim_id: int) -> float:
    base = int(await get_setting("theft_defense_chance"))
    rep = await get_user_reputation(victim_id)
    bonus = float(await get_setting("reputation_defense_bonus")) * rep
    return base + bonus

@dp.message_handler(lambda message: message.text == "🔫 Ограбить")
async def theft_menu(message: types.Message):
    if message.chat.type != 'private':
        return
    user_id = message.from_user.id
    if await is_banned(user_id) and not await is_admin(user_id):
        return
    ok, not_subscribed = await check_subscription(user_id)
    if not ok:
        await message.answer("❗️ Сначала подпишись на каналы.", reply_markup=subscription_inline(not_subscribed))
        return
    phrase = random.choice(THEFT_CHOICE_PHRASES)
    await message.answer(phrase, reply_markup=theft_choice_keyboard())

@dp.message_handler(lambda message: message.text == "🎲 Случайная цель")
async def theft_random(message: types.Message, state: FSMContext):
    if message.chat.type != 'private':
        return
    user_id = message.from_user.id
    cooldown_minutes = int(await get_setting("theft_cooldown_minutes"))
    async with db_pool.acquire() as conn:
        last_time_str = await conn.fetchval("SELECT last_theft_time FROM users WHERE user_id=$1", user_id)
        if last_time_str:
            last_time = datetime.strptime(last_time_str, "%Y-%m-%d %H:%M:%S")
            diff = datetime.now() - last_time
            if diff < timedelta(minutes=cooldown_minutes):
                remaining = cooldown_minutes - int(diff.total_seconds() // 60)
                phrase = random.choice(THEFT_COOLDOWN_PHRASES).format(minutes=remaining)
                await message.answer(phrase, reply_markup=user_main_keyboard(await is_admin(user_id)))
                return
    target_id = await get_random_user(user_id)
    if not target_id:
        await message.answer("😕 В игре пока нет других игроков.", reply_markup=user_main_keyboard(await is_admin(user_id)))
        return
    cost = int(await get_setting("random_attack_cost"))
    if cost > 0:
        balance = await get_user_balance(user_id)
        if balance < cost:
            await message.answer(random.choice(THEFT_NO_MONEY_PHRASES), reply_markup=user_main_keyboard(await is_admin(user_id)))
            return
        await update_user_balance(user_id, -cost)
    await perform_theft(message, user_id, target_id)

@dp.message_handler(lambda message: message.text == "👤 Выбрать пользователя")
async def theft_choose_user(message: types.Message, state: FSMContext):
    if message.chat.type != 'private':
        return
    user_id = message.from_user.id
    cooldown_minutes = int(await get_setting("theft_cooldown_minutes"))
    async with db_pool.acquire() as conn:
        last_time_str = await conn.fetchval("SELECT last_theft_time FROM users WHERE user_id=$1", user_id)
        if last_time_str:
            last_time = datetime.strptime(last_time_str, "%Y-%m-%d %H:%M:%S")
            diff = datetime.now() - last_time
            if diff < timedelta(minutes=cooldown_minutes):
                remaining = cooldown_minutes - int(diff.total_seconds() // 60)
                phrase = random.choice(THEFT_COOLDOWN_PHRASES).format(minutes=remaining)
                await message.answer(phrase, reply_markup=user_main_keyboard(await is_admin(user_id)))
                return
    await message.answer("Введи @username или ID того, кого хочешь ограбить:", reply_markup=back_keyboard())
    await TheftTarget.target.set()

@dp.message_handler(state=TheftTarget.target)
async def theft_target_entered(message: types.Message, state: FSMContext):
    if message.chat.type != 'private':
        await state.finish()
        return
    if message.text == "◀️ Назад":
        await state.finish()
        await message.answer("Главное меню:", reply_markup=user_main_keyboard(await is_admin(message.from_user.id)))
        return
    target_input = message.text.strip()
    robber_id = message.from_user.id

    target_data = await find_user_by_input(target_input)
    if not target_data:
        await message.answer("❌ Пользователь не найден. Проверь username или ID.")
        return
    target_id = target_data['user_id']

    if target_id == robber_id:
        await message.answer("Сам себя не ограбишь, бро! 😆")
        await state.finish()
        return

    if await is_banned(target_id):
        await message.answer("❌ Этот пользователь заблокирован и не может быть целью.")
        await state.finish()
        return

    cost = int(await get_setting("targeted_attack_cost"))
    if cost > 0:
        balance = await get_user_balance(robber_id)
        if balance < cost:
            await message.answer(random.choice(THEFT_NO_MONEY_PHRASES), reply_markup=user_main_keyboard(await is_admin(robber_id)))
            await state.finish()
            return
        await update_user_balance(robber_id, -cost)

    await perform_theft(message, robber_id, target_id)
    await state.finish()

async def perform_theft(message: types.Message, robber_id: int, victim_id: int):
    success_chance = await get_theft_success_chance(robber_id)
    defense_chance = await get_defense_chance(victim_id)
    defense_penalty = int(await get_setting("theft_defense_penalty"))
    min_amount = int(await get_setting("min_theft_amount"))
    max_amount = int(await get_setting("max_theft_amount"))

    try:
        async with db_pool.acquire() as conn:
            victim_balance = await conn.fetchval("SELECT balance FROM users WHERE user_id=$1", victim_id)
            if victim_balance is None:
                await message.answer("❌ Цель не найдена в базе.")
                return

            victim_info = await conn.fetchrow("SELECT username, first_name FROM users WHERE user_id=$1", victim_id)
            victim_name = victim_info['first_name'] if victim_info else str(victim_id)

            defense_triggered = random.randint(1, 100) <= defense_chance
            if defense_triggered:
                penalty = defense_penalty
                robber_balance = await get_user_balance(robber_id)
                if penalty > robber_balance:
                    penalty = robber_balance
                if penalty > 0:
                    await conn.execute("UPDATE users SET balance = balance - $1 WHERE user_id=$2", penalty, robber_id)
                    await conn.execute("UPDATE users SET balance = balance + $1 WHERE user_id=$2", penalty, victim_id)
                await conn.execute("UPDATE users SET theft_attempts = theft_attempts + 1, theft_failed = theft_failed + 1 WHERE user_id=$1", robber_id)
                await conn.execute("UPDATE users SET theft_protected = theft_protected + 1 WHERE user_id=$1", victim_id)
                await conn.execute("UPDATE users SET last_theft_time = $1 WHERE user_id=$2", datetime.now().strftime("%Y-%m-%d %H:%M:%S"), robber_id)

                exp_defense = int(await get_setting("exp_per_theft_defense"))
                await add_exp(victim_id, exp_defense, conn)
                exp_fail = int(await get_setting("exp_per_theft_fail"))
                await add_exp(robber_id, exp_fail, conn)

                robber_phrase = random.choice(THEFT_DEFENSE_PHRASES).format(target=victim_name, penalty=penalty)
                victim_phrase = random.choice(THEFT_VICTIM_DEFENSE_PHRASES).format(attacker=message.from_user.first_name, penalty=penalty)
                await message.answer(robber_phrase, reply_markup=user_main_keyboard(await is_admin(robber_id)))
                await safe_send_message(victim_id, victim_phrase)
                return

            success = random.randint(1, 100) <= success_chance
            if success and victim_balance > 0:
                max_possible = min(max_amount, victim_balance)
                if max_possible < min_amount:
                    steal_amount = victim_balance
                else:
                    steal_amount = random.randint(min_amount, max_possible)

                await conn.execute("UPDATE users SET balance = balance - $1 WHERE user_id=$2", steal_amount, victim_id)
                await conn.execute("UPDATE users SET balance = balance + $1 WHERE user_id=$2", steal_amount, robber_id)
                await conn.execute("UPDATE users SET theft_attempts = theft_attempts + 1, theft_success = theft_success + 1 WHERE user_id=$1", robber_id)

                exp_success = int(await get_setting("exp_per_theft_success"))
                await add_exp(robber_id, exp_success, conn)

                new_success = await conn.fetchval("SELECT theft_success FROM users WHERE user_id=$1", robber_id)
                if new_success == 15:
                    ref = await conn.fetchrow("SELECT referrer_id FROM referrals WHERE referred_id=$1 AND reward_given=FALSE", robber_id)
                    if ref:
                        referrer_id = ref['referrer_id']
                        bonus_coins = int(await get_setting("referral_bonus"))
                        bonus_rep = int(await get_setting("referral_reputation"))
                        await update_user_balance(referrer_id, bonus_coins)
                        await update_user_reputation(referrer_id, bonus_rep)
                        await conn.execute("UPDATE referrals SET reward_given=TRUE WHERE referred_id=$1", robber_id)
                        await safe_send_message(referrer_id, f"🎉 Ваш реферал совершил 15 успешных ограблений! Вы получили {bonus_coins} монет и {bonus_rep} репутации.")

                phrase = random.choice(THEFT_SUCCESS_PHRASES).format(amount=steal_amount, target=victim_name)
                await message.answer(phrase, reply_markup=user_main_keyboard(await is_admin(robber_id)))
                await safe_send_message(victim_id, f"🔫 Вас ограбили! {message.from_user.first_name} украл {steal_amount} монет.")
            else:
                await conn.execute("UPDATE users SET theft_attempts = theft_attempts + 1, theft_failed = theft_failed + 1 WHERE user_id=$1", robber_id)
                exp_fail = int(await get_setting("exp_per_theft_fail"))
                await add_exp(robber_id, exp_fail, conn)
                phrase = random.choice(THEFT_FAIL_PHRASES).format(target=victim_name)
                await message.answer(phrase, reply_markup=user_main_keyboard(await is_admin(robber_id)))

            await conn.execute("UPDATE users SET last_theft_time = $1 WHERE user_id=$2", datetime.now().strftime("%Y-%m-%d %H:%M:%S"), robber_id)

    except Exception as e:
        logging.error(f"Theft error: {e}")
        await message.answer("❌ Ошибка при ограблении.")

# ===== РЕФЕРАЛЬНАЯ ССЫЛКА =====
@dp.message_handler(lambda message: message.text == "🔗 Рефералка")
async def referral_link(message: types.Message):
    if message.chat.type != 'private':
        return
    user_id = message.from_user.id
    bot_username = (await bot.me).username
    link = f"https://t.me/{bot_username}?start=ref{user_id}"
    bonus_coins = await get_setting("referral_bonus")
    bonus_rep = await get_setting("referral_reputation")
    await message.answer(
        f"🔗 Твоя реферальная ссылка:\n{link}\n\n"
        f"Приведи друга и получи {bonus_coins} монет и {bonus_rep} репутации, когда он совершит 15 успешных ограблений!"
    )

# ===== ЗАДАНИЯ =====
@dp.message_handler(lambda message: message.text == "📋 Задания")
async def tasks_menu(message: types.Message):
    if message.chat.type != 'private':
        return
    user_id = message.from_user.id
    if await is_banned(user_id) and not await is_admin(user_id):
        return
    ok, not_subscribed = await check_subscription(user_id)
    if not ok:
        await message.answer("❗️ Сначала подпишись на каналы.", reply_markup=subscription_inline(not_subscribed))
        return

    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT id, name, description, reward_coins, reward_reputation FROM tasks WHERE active=TRUE")
    if not rows:
        await message.answer("📋 Пока нет доступных заданий.", reply_markup=user_main_keyboard(await is_admin(user_id)))
        return

    text = "📋 Доступные задания:\n\n"
    kb = []
    for row in rows:
        text += f"🔹 {row['name']}\n{row['description']}\nНаграда: {row['reward_coins']} монет, {row['reward_reputation']} репутации\n\n"
        kb.append([InlineKeyboardButton(text=f"Выполнить {row['name']}", callback_data=f"task_{row['id']}")])
    await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@dp.callback_query_handler(lambda c: c.data.startswith("task_"))
async def take_task(callback: types.CallbackQuery):
    task_id = int(callback.data.split("_")[1])
    user_id = callback.from_user.id

    async with db_pool.acquire() as conn:
        existing = await conn.fetchval("SELECT 1 FROM user_tasks WHERE user_id=$1 AND task_id=$2", user_id, task_id)
        if existing:
            await callback.answer("Ты уже выполнял это задание!", show_alert=True)
            return

        task = await conn.fetchrow("SELECT * FROM tasks WHERE id=$1 AND active=TRUE", task_id)
        if not task:
            await callback.answer("Задание не найдено или неактивно.", show_alert=True)
            return

        if task['task_type'] == 'subscribe':
            channel_id = task['target_id']
            try:
                member = await bot.get_chat_member(chat_id=channel_id, user_id=user_id)
                if member.status in ['left', 'kicked']:
                    await callback.answer("❌ Ты не подписан на этот канал!", show_alert=True)
                    return
            except Exception as e:
                logging.error(f"Task subscribe check error: {e}")
                await callback.answer("❌ Не удалось проверить подписку. Возможно, бот не админ канала.", show_alert=True)
                return

            async with conn.transaction():
                await conn.execute("UPDATE users SET balance = balance + $1, reputation = reputation + $2 WHERE user_id=$3",
                                   task['reward_coins'], task['reward_reputation'], user_id)
                expires_at = (datetime.now() + timedelta(days=task['required_days'])).strftime("%Y-%m-%d %H:%M:%S") if task['required_days'] > 0 else None
                await conn.execute(
                    "INSERT INTO user_tasks (user_id, task_id, completed_at, expires_at, status) VALUES ($1, $2, $3, $4, $5)",
                    user_id, task_id, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), expires_at, 'completed'
                )

            await callback.answer(f"✅ Задание выполнено! +{task['reward_coins']} монет, +{task['reward_reputation']} репутации", show_alert=True)
            await callback.message.delete()
        else:
            await callback.answer("Этот тип заданий пока не поддерживается.", show_alert=True)

# ===== МУЛЬТИПЛЕЕРНАЯ ИГРА "21" (полная) =====
@dp.message_handler(lambda message: message.text == "👥 Комнатная игра 21")
async def multiplayer_main(message: types.Message):
    if message.chat.type != 'private':
        return
    user_id = message.from_user.id
    if await is_banned(user_id) and not await is_admin(user_id):
        return
    ok, not_subscribed = await check_subscription(user_id)
    if not ok:
        await message.answer("❗️ Сначала подпишись на каналы.", reply_markup=subscription_inline(not_subscribed))
        return
    await message.answer("🎮 Мультиплеер 21 – выбери действие:", reply_markup=room_menu_keyboard())

@dp.message_handler(lambda message: message.text == "ℹ️ Правила игры")
async def game_rules(message: types.Message):
    rules = """
🎯 **Правила игры "21" (мультиплеер):**
• Каждый игрок делает ставку (от 3 монет).
• Цель – набрать сумму очков как можно ближе к 21, но не больше.
• Карты: 2–10 по номиналу, J/Q/K – 10 очков, Туз – 11 или 1.
• Игроки ходят по очереди: можно взять ещё карту ("Ещё") или остановиться ("Хватит").
• Доступна опция **"Удвоить"** – увеличить ставку вдвое и взять ровно одну карту (доступно только на первом ходу).
• Дилер добирает до 17 очков.
• Победитель забирает банк за вычетом комиссии (1 монета с игрока).
• В случае ничьей ставка возвращается.
• Создатель комнаты может начать игру при наличии от 2 до 5 игроков.
• До начала игры можно выйти без потери монет.
• Во время игры выход или сдача приводят к проигрышу ставки.
    """
    await message.answer(rules)

@dp.message_handler(lambda message: message.text == "🏆 Топ игроков")
async def game_top(message: types.Message):
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT first_name, game_wins FROM users WHERE game_wins > 0 ORDER BY game_wins DESC LIMIT 10")
    if not rows:
        await message.answer("🏆 Топ пока пуст.")
        return
    text = "🏆 **Лучшие игроки в 21:**\n\n"
    for i, row in enumerate(rows, 1):
        text += f"{i}. {row['first_name']} – {row['game_wins']} побед\n"
    await message.answer(text)

@dp.message_handler(lambda message: message.text == "📋 Список комнат")
async def list_rooms(message: types.Message):
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT game_id, host_id, max_players, bet_amount, 
                   (SELECT COUNT(*) FROM game_players WHERE game_id = g.game_id) as player_count
            FROM multiplayer_games g
            WHERE status = 'waiting'
            ORDER BY created_at
        """)
    if not rows:
        await message.answer("📭 Нет открытых комнат. Создай свою!")
        return
    text = "📋 **Открытые комнаты:**\n\n"
    kb = []
    for row in rows:
        game_id = row['game_id']
        max_pl = row['max_players']
        cur_pl = row['player_count']
        bet = row['bet_amount']
        text += f"🆔 `{game_id}` | {cur_pl}/{max_pl} игр. | 💰 {bet} монет\n"
        kb.append([InlineKeyboardButton(text=f"Присоединиться к {game_id}", callback_data=f"join_room_{game_id}")])
    await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@dp.callback_query_handler(lambda c: c.data.startswith("join_room_"))
async def join_room_callback(callback: types.CallbackQuery):
    game_id = callback.data.replace("join_room_", "")
    user_id = callback.from_user.id
    username = callback.from_user.username or "NoName"
    async with db_pool.acquire() as conn:
        game = await conn.fetchrow("SELECT * FROM multiplayer_games WHERE game_id=$1 AND status='waiting'", game_id)
        if not game:
            await callback.answer("❌ Комната не найдена или игра уже началась.", show_alert=True)
            return
        players = await conn.fetch("SELECT user_id FROM game_players WHERE game_id=$1", game_id)
        if len(players) >= game['max_players']:
            await callback.answer("❌ Комната уже заполнена.", show_alert=True)
            return
        existing = await conn.fetchval("SELECT 1 FROM game_players WHERE game_id=$1 AND user_id=$2", game_id, user_id)
        if existing:
            await callback.answer("❌ Ты уже в этой комнате.", show_alert=True)
            return
        balance = await get_user_balance(user_id)
        bet = game['bet_amount']
        if balance < bet:
            await callback.answer(f"❌ Недостаточно монет. Нужно {bet}", show_alert=True)
            return
        await conn.execute(
            "INSERT INTO game_players (game_id, user_id, username, cards, value, stopped, joined_at, doubled) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
            game_id, user_id, username, '', 0, False, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), False
        )
        host_id = game['host_id']
        if host_id != user_id:
            await safe_send_message(host_id, f"✅ @{username} присоединился к твоей комнате `{game_id}`.")
    await callback.message.edit_text(f"✅ Ты присоединился к комнате `{game_id}`. Ожидаем остальных...")
    await callback.message.answer("Ты в комнате. Можешь выйти в любой момент до начала игры.", reply_markup=leave_room_keyboard(game_id))
    await callback.answer()

@dp.message_handler(lambda message: message.text == "🎮 Создать комнату")
async def create_room_start(message: types.Message):
    async with db_pool.acquire() as conn:
        count = await conn.fetchval("SELECT COUNT(*) FROM multiplayer_games WHERE status='waiting'")
    if count >= MAX_ROOMS:
        await message.answer(f"❌ Достигнут лимит активных комнат ({MAX_ROOMS}). Попробуй позже.")
        return
    await message.answer("Введи количество игроков (2–5):", reply_markup=back_keyboard())
    await MultiplayerGame.create_max_players.set()

@dp.message_handler(state=MultiplayerGame.create_max_players)
async def create_room_max_players(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await multiplayer_main(message)
        return
    try:
        max_players = int(message.text)
        if max_players < MIN_PLAYERS or max_players > MAX_PLAYERS:
            raise ValueError
    except:
        await message.answer(f"❌ Введи число от {MIN_PLAYERS} до {MAX_PLAYERS}.")
        return
    await state.update_data(max_players=max_players)
    await message.answer(f"Введи ставку (целое число, не меньше {MIN_BET}):")
    await MultiplayerGame.create_bet.set()

@dp.message_handler(state=MultiplayerGame.create_bet)
async def create_room_bet(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await multiplayer_main(message)
        return
    try:
        bet = int(message.text)
        if bet < MIN_BET:
            raise ValueError
    except:
        await message.answer(f"❌ Введи целое число не меньше {MIN_BET}.")
        return
    data = await state.get_data()
    max_players = data['max_players']
    user_id = message.from_user.id
    balance = await get_user_balance(user_id)
    if balance < bet:
        await message.answer(f"❌ У тебя недостаточно монет. Нужно {bet}")
        await state.finish()
        return
    game_id = generate_game_id()
    async with db_pool.acquire() as conn:
        existing = await conn.fetchval("SELECT game_id FROM multiplayer_games WHERE game_id=$1", game_id)
        while existing:
            game_id = generate_game_id()
            existing = await conn.fetchval("SELECT game_id FROM multiplayer_games WHERE game_id=$1", game_id)
        await conn.execute(
            "INSERT INTO multiplayer_games (game_id, host_id, max_players, bet_amount, status, created_at) VALUES ($1, $2, $3, $4, $5, $6)",
            game_id, user_id, max_players, bet, 'waiting', datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        )
        await conn.execute(
            "INSERT INTO game_players (game_id, user_id, username, cards, value, stopped, joined_at, doubled) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
            game_id, user_id, message.from_user.username or "NoName", '', 0, False, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), False
        )
    await state.finish()
    await message.answer(
        f"✅ Комната `{game_id}` создана!\n"
        f"👥 Игроков: 1/{max_players}\n"
        f"💰 Ставка: {bet} монет\n\n"
        f"Ты можешь запустить игру, когда наберётся не менее {MIN_PLAYERS} игроков.",
        reply_markup=room_control_keyboard(game_id)
    )

@dp.callback_query_handler(lambda c: c.data.startswith("close_room_"))
async def close_room_callback(callback: types.CallbackQuery):
    game_id = callback.data.replace("close_room_", "")
    user_id = callback.from_user.id
    async with db_pool.acquire() as conn:
        game = await conn.fetchrow("SELECT * FROM multiplayer_games WHERE game_id=$1 AND status='waiting'", game_id)
        if not game:
            await callback.answer("❌ Комната не найдена или игра уже началась.", show_alert=True)
            return
        if game['host_id'] != user_id:
            await callback.answer("❌ Только создатель может закрыть комнату.", show_alert=True)
            return
        await conn.execute("DELETE FROM game_players WHERE game_id=$1", game_id)
        await conn.execute("DELETE FROM multiplayer_games WHERE game_id=$1", game_id)
    await callback.message.edit_text("🏁 Комната закрыта.")
    await callback.answer()

@dp.callback_query_handler(lambda c: c.data.startswith("leave_room_"))
async def leave_room_callback(callback: types.CallbackQuery):
    game_id = callback.data.replace("leave_room_", "")
    user_id = callback.from_user.id
    async with db_pool.acquire() as conn:
        game = await conn.fetchrow("SELECT * FROM multiplayer_games WHERE game_id=$1", game_id)
        if not game:
            await callback.answer("❌ Комната не найдена.", show_alert=True)
            return
        if game['status'] == 'waiting':
            await conn.execute("DELETE FROM game_players WHERE game_id=$1 AND user_id=$2", game_id, user_id)
            if game['host_id'] == user_id:
                next_host = await conn.fetchval("SELECT user_id FROM game_players WHERE game_id=$1 ORDER BY joined_at LIMIT 1", game_id)
                if next_host:
                    await conn.execute("UPDATE multiplayer_games SET host_id=$1 WHERE game_id=$2", next_host, game_id)
                    await safe_send_message(next_host, f"🎮 Ты стал создателем комнаты `{game_id}`.")
                else:
                    await conn.execute("DELETE FROM multiplayer_games WHERE game_id=$1", game_id)
            await callback.message.edit_text("❌ Ты покинул комнату.")
        else:
            bet = game['bet_amount']
            player = await conn.fetchrow("SELECT doubled FROM game_players WHERE game_id=$1 AND user_id=$2", game_id, user_id)
            if player and player['doubled']:
                bet *= 2
            await update_user_balance(user_id, -bet)
            await conn.execute("UPDATE game_players SET stopped=TRUE WHERE game_id=$1 AND user_id=$2", game_id, user_id)
            await callback.message.edit_text(f"❌ Ты покинул игру и потерял {bet} монет.")
            active = await conn.fetchval("SELECT COUNT(*) FROM game_players WHERE game_id=$1 AND user_id != 0 AND stopped = FALSE", game_id)
            if active == 0:
                await dealer_turn(game_id)
    await callback.answer()

@dp.callback_query_handler(lambda c: c.data.startswith("start_game_"))
async def start_game_callback(callback: types.CallbackQuery):
    game_id = callback.data.replace("start_game_", "")
    user_id = callback.from_user.id
    async with db_pool.acquire() as conn:
        game = await conn.fetchrow("SELECT * FROM multiplayer_games WHERE game_id=$1 AND status='waiting'", game_id)
        if not game:
            await callback.answer("❌ Комната не найдена или игра уже началась.", show_alert=True)
            return
        if game['host_id'] != user_id:
            await callback.answer("❌ Только создатель комнаты может начать игру.", show_alert=True)
            return
        players = await conn.fetch("SELECT user_id FROM game_players WHERE game_id=$1", game_id)
        if len(players) < MIN_PLAYERS:
            await callback.answer(f"❌ Недостаточно игроков. Нужно минимум {MIN_PLAYERS}.", show_alert=True)
            return
        await conn.execute("UPDATE multiplayer_games SET status='playing' WHERE game_id=$1", game_id)
        deck = create_deck()
        for player in players:
            cards = [deck.pop(), deck.pop()]
            cards_str = ','.join(cards)
            value = calculate_hand_value(cards)
            await conn.execute(
                "UPDATE game_players SET cards=$1, value=$2 WHERE game_id=$3 AND user_id=$4",
                cards_str, value, game_id, player['user_id']
            )
        await conn.execute(
            "INSERT INTO game_players (game_id, user_id, username, cards, value, stopped, joined_at, doubled) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
            game_id, 0, 'Дилер', '', 0, False, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), False
        )
        await conn.execute("UPDATE multiplayer_games SET deck=$1 WHERE game_id=$2", ','.join(deck), game_id)
    for player in players:
        await safe_send_message(player['user_id'], f"🎮 Игра в комнате `{game_id}` началась! Твой ход.")
    await process_next_turn(game_id, 0)

async def process_next_turn(game_id: str, player_index: int):
    async with db_pool.acquire() as conn:
        game = await conn.fetchrow("SELECT * FROM multiplayer_games WHERE game_id=$1", game_id)
        if not game or game['status'] != 'playing':
            return
        players = await conn.fetch("SELECT * FROM game_players WHERE game_id=$1 AND user_id != 0 ORDER BY joined_at", game_id)
        if player_index >= len(players):
            await dealer_turn(game_id)
            return
        current_player = players[player_index]
        if current_player['stopped']:
            await process_next_turn(game_id, player_index + 1)
            return
        cards = current_player['cards'].split(',') if current_player['cards'] else []
        value = calculate_hand_value(cards)
        async with dp.current_state(chat=current_player['user_id'], user=current_player['user_id']).proxy() as data:
            data['game_id'] = game_id
            data['player_index'] = player_index
        show_double = len(cards) == 2 and not current_player['doubled']
        kb_buttons = []
        row1 = []
        row1.append(InlineKeyboardButton(text="🎯 Ещё", callback_data="room_hit"))
        row1.append(InlineKeyboardButton(text="🛑 Хватит", callback_data="room_stand"))
        kb_buttons.append(row1)
        row2 = []
        if show_double:
            row2.append(InlineKeyboardButton(text="💰 Удвоить", callback_data="room_double"))
        row2.append(InlineKeyboardButton(text="🏳️ Сдаться", callback_data="room_surrender"))
        kb_buttons.append(row2)
        kb_buttons.append([InlineKeyboardButton(text="💬 Написать в чат", callback_data="room_chat")])
        kb = InlineKeyboardMarkup(inline_keyboard=kb_buttons)
        await safe_send_message(
            current_player['user_id'],
            f"🎮 Твой ход!\nТвои карты: {', '.join(cards)} (очков: {value})\n\nВыбери действие:",
            reply_markup=kb
        )

@dp.callback_query_handler(lambda c: c.data in ["room_hit", "room_stand", "room_double", "room_surrender", "room_chat"])
async def room_action_callback(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    async with dp.current_state(chat=user_id, user=user_id).proxy() as data:
        game_id = data.get('game_id')
        player_index = data.get('player_index')
    if not game_id:
        await callback.answer("❌ Игра не найдена.", show_alert=True)
        return
    async with db_pool.acquire() as conn:
        game = await conn.fetchrow("SELECT * FROM multiplayer_games WHERE game_id=$1", game_id)
        if not game or game['status'] != 'playing':
            await callback.answer("❌ Игра уже завершена.", show_alert=True)
            return
        players = await conn.fetch("SELECT * FROM game_players WHERE game_id=$1 AND user_id != 0 ORDER BY joined_at", game_id)
        if player_index >= len(players) or players[player_index]['user_id'] != user_id:
            await callback.answer("❌ Сейчас не твой ход.", show_alert=True)
            return
        deck = game['deck'].split(',') if game['deck'] else []
        current_player = players[player_index]
        cards = current_player['cards'].split(',') if current_player['cards'] else []
        value = calculate_hand_value(cards)

        if callback.data == "room_hit":
            if not deck:
                await callback.answer("Колода кончилась, передаём ход...", show_alert=True)
                await conn.execute("UPDATE game_players SET stopped=TRUE WHERE game_id=$1 AND user_id=$2", game_id, user_id)
                await callback.answer()
                active = await conn.fetchval("SELECT COUNT(*) FROM game_players WHERE game_id=$1 AND user_id != 0 AND stopped = FALSE", game_id)
                if active == 0:
                    await dealer_turn(game_id)
                else:
                    await process_next_turn(game_id, player_index + 1)
                return
            new_card = deck.pop()
            cards.append(new_card)
            value = calculate_hand_value(cards)
            await conn.execute(
                "UPDATE game_players SET cards=$1, value=$2 WHERE game_id=$3 AND user_id=$4",
                ','.join(cards), value, game_id, user_id
            )
            await conn.execute("UPDATE multiplayer_games SET deck=$1 WHERE game_id=$2", ','.join(deck), game_id)
            if value > 21:
                await conn.execute("UPDATE game_players SET stopped=TRUE WHERE game_id=$1 AND user_id=$2", game_id, user_id)
                await callback.message.edit_text(f"💥 Перебор! Твои карты: {', '.join(cards)} (очков: {value})\nТы проиграл свою ставку.")
                await callback.answer()
                active = await conn.fetchval("SELECT COUNT(*) FROM game_players WHERE game_id=$1 AND user_id != 0 AND stopped = FALSE", game_id)
                if active == 0:
                    await dealer_turn(game_id)
                else:
                    await process_next_turn(game_id, player_index + 1)
                return
            else:
                kb = InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🎯 Ещё", callback_data="room_hit"),
                     InlineKeyboardButton(text="🛑 Хватит", callback_data="room_stand")],
                    [InlineKeyboardButton(text="🏳️ Сдаться", callback_data="room_surrender")],
                    [InlineKeyboardButton(text="💬 Написать в чат", callback_data="room_chat")]
                ])
                await callback.message.edit_text(
                    f"Твои карты: {', '.join(cards)} (очков: {value})\nВыбери действие:",
                    reply_markup=kb
                )
                await callback.answer()
            return

        elif callback.data == "room_stand":
            await conn.execute("UPDATE game_players SET stopped=TRUE WHERE game_id=$1 AND user_id=$2", game_id, user_id)
            await callback.message.edit_text(f"✅ Ты остановился на {value} очках.")
            await callback.answer()
            active = await conn.fetchval("SELECT COUNT(*) FROM game_players WHERE game_id=$1 AND user_id != 0 AND stopped = FALSE", game_id)
            if active == 0:
                await dealer_turn(game_id)
            else:
                await process_next_turn(game_id, player_index + 1)
            return

        elif callback.data == "room_double":
            if len(cards) != 2 or current_player['doubled']:
                await callback.answer("❌ Удвоение сейчас недоступно.", show_alert=True)
                return
            bet = game['bet_amount']
            balance = await get_user_balance(user_id)
            if balance < bet:
                await callback.answer("❌ Недостаточно монет для удвоения.", show_alert=True)
                return
            await conn.execute("UPDATE game_players SET doubled=TRUE WHERE game_id=$1 AND user_id=$2", game_id, user_id)
            if not deck:
                await callback.answer("Колода кончилась, удвоение невозможно.", show_alert=True)
                return
            new_card = deck.pop()
            cards.append(new_card)
            value = calculate_hand_value(cards)
            await conn.execute(
                "UPDATE game_players SET cards=$1, value=$2, stopped=TRUE WHERE game_id=$3 AND user_id=$4",
                ','.join(cards), value, game_id, user_id
            )
            await conn.execute("UPDATE multiplayer_games SET deck=$1 WHERE game_id=$2", ','.join(deck), game_id)
            if value > 21:
                await callback.message.edit_text(f"💥 Перебор! Твои карты: {', '.join(cards)} (очков: {value})\nТы проиграл удвоенную ставку.")
            else:
                await callback.message.edit_text(f"💰 Ты удвоил ставку и взял карту {new_card}. Остановился на {value} очках.")
            await callback.answer()
            active = await conn.fetchval("SELECT COUNT(*) FROM game_players WHERE game_id=$1 AND user_id != 0 AND stopped = FALSE", game_id)
            if active == 0:
                await dealer_turn(game_id)
            else:
                await process_next_turn(game_id, player_index + 1)
            return

        elif callback.data == "room_surrender":
            bet = game['bet_amount']
            effective_bet = bet * 2 if current_player['doubled'] else bet
            loss = effective_bet // 2
            await update_user_balance(user_id, -loss)
            await conn.execute("UPDATE game_players SET stopped=TRUE WHERE game_id=$1 AND user_id=$2", game_id, user_id)
            await callback.message.edit_text(f"🏳️ Ты сдался и потерял {loss} монет.")
            await callback.answer()
            active = await conn.fetchval("SELECT COUNT(*) FROM game_players WHERE game_id=$1 AND user_id != 0 AND stopped = FALSE", game_id)
            if active == 0:
                await dealer_turn(game_id)
            else:
                await process_next_turn(game_id, player_index + 1)
            return

        elif callback.data == "room_chat":
            await callback.message.answer("Введи сообщение для всех в комнате (или /cancel для отмены):")
            await RoomChat.message.set()
            await callback.answer()

@dp.message_handler(state=RoomChat.message)
async def room_chat_message(message: types.Message, state: FSMContext):
    if message.text == "/cancel":
        await state.finish()
        await message.answer("Отправка отменена.")
        return
    user_id = message.from_user.id
    async with dp.current_state(chat=user_id, user=user_id).proxy() as data:
        game_id = data.get('game_id')
    if not game_id:
        await state.finish()
        await message.answer("❌ Игра не найдена.")
        return
    async with db_pool.acquire() as conn:
        players = await conn.fetch("SELECT user_id FROM game_players WHERE game_id=$1 AND user_id != 0", game_id)
        for player in players:
            if player['user_id'] != user_id:
                await safe_send_message(player['user_id'], f"💬 {message.from_user.first_name}: {message.text}")
    await state.finish()
    await message.answer("✅ Сообщение отправлено всем игрокам в комнате.")

async def dealer_turn(game_id: str):
    async with db_pool.acquire() as conn:
        game = await conn.fetchrow("SELECT * FROM multiplayer_games WHERE game_id=$1", game_id)
        if not game or game['status'] != 'playing':
            return
        deck = game['deck'].split(',') if game['deck'] else []
        dealer = await conn.fetchrow("SELECT * FROM game_players WHERE game_id=$1 AND user_id=0", game_id)
        if dealer:
            dealer_cards = dealer['cards'].split(',') if dealer['cards'] else []
            dealer_value = dealer['value']
        else:
            dealer_cards = []
            dealer_value = 0
        while dealer_value < 17 and deck:
            new_card = deck.pop()
            dealer_cards.append(new_card)
            dealer_value = calculate_hand_value(dealer_cards)
            await conn.execute(
                "UPDATE game_players SET cards=$1, value=$2 WHERE game_id=$3 AND user_id=0",
                ','.join(dealer_cards), dealer_value, game_id
            )
            await conn.execute("UPDATE multiplayer_games SET deck=$1 WHERE game_id=$2", ','.join(deck), game_id)
        players = await conn.fetch("SELECT * FROM game_players WHERE game_id=$1 AND user_id != 0 AND stopped = FALSE", game_id)
        bet = game['bet_amount']
        results = []
        for player in players:
            player_value = player['value']
            doubled = player['doubled']
            effective_bet = bet * 2 if doubled else bet
            if player_value > 21:
                results.append((player['user_id'], f"❌ Проигрыш (перебор) -{effective_bet}", -effective_bet))
                await update_user_balance(player['user_id'], -effective_bet)
                exp = int(await get_setting("exp_per_game_lose"))
                await add_exp(player['user_id'], exp, conn)
            elif dealer_value > 21:
                win = effective_bet - 1
                results.append((player['user_id'], f"✅ Выигрыш +{win}", win))
                await update_user_balance(player['user_id'], win)
                await conn.execute("UPDATE users SET game_wins = game_wins + 1 WHERE user_id=$1", player['user_id'])
                exp = int(await get_setting("exp_per_game_win"))
                await add_exp(player['user_id'], exp, conn)
            elif player_value > dealer_value:
                win = effective_bet - 1
                results.append((player['user_id'], f"✅ Выигрыш +{win}", win))
                await update_user_balance(player['user_id'], win)
                await conn.execute("UPDATE users SET game_wins = game_wins + 1 WHERE user_id=$1", player['user_id'])
                exp = int(await get_setting("exp_per_game_win"))
                await add_exp(player['user_id'], exp, conn)
            elif player_value < dealer_value:
                results.append((player['user_id'], f"❌ Проигрыш -{effective_bet}", -effective_bet))
                await update_user_balance(player['user_id'], -effective_bet)
                exp = int(await get_setting("exp_per_game_lose"))
                await add_exp(player['user_id'], exp, conn)
            else:
                results.append((player['user_id'], f"🤝 Ничья (возврат ставки)", 0))
        dealer_cards_str = ', '.join(dealer_cards) if dealer_cards else 'нет карт'
        for user_id, res, _ in results:
            await safe_send_message(user_id,
                f"🎮 Итоги игры в комнате `{game_id}`:\n"
                f"Карты дилера: {dealer_cards_str} (очков: {dealer_value})\n"
                f"Результат: {res}"
            )
        await conn.execute("DELETE FROM game_players WHERE game_id=$1", game_id)
        await conn.execute("DELETE FROM multiplayer_games WHERE game_id=$1", game_id)

# ===== ОБРАБОТЧИК ДОБАВЛЕНИЯ БОТА В ЧАТ =====
@dp.message_handler(content_types=['new_chat_members'])
async def bot_added_to_chat(message: types.Message):
    bot_user = await bot.me
    if bot_user.id not in [user.id for user in message.new_chat_members]:
        return
    chat = message.chat
    user_id = message.from_user.id
    await create_chat_confirmation_request(chat.id, chat.title, chat.type, user_id)
    for admin_id in SUPER_ADMINS:
        kb = confirm_chat_inline(chat.id)
        await safe_send_message(
            admin_id,
            f"🆕 Запрос на активацию бота в чате:\n"
            f"Название: {chat.title}\n"
            f"ID: {chat.id}\n"
            f"Тип: {chat.type}\n"
            f"Запросил: {message.from_user.first_name} (ID: {user_id})",
            reply_markup=kb
        )
    await message.answer("📋 Запрос на активацию бота отправлен администратору. Ожидайте подтверждения.")

# ===== ИНЛАЙН-ПОДТВЕРЖДЕНИЕ ЧАТА =====
@dp.callback_query_handler(lambda c: c.data.startswith("confirm_chat_"))
async def confirm_chat_callback(callback: types.CallbackQuery):
    if not await is_super_admin(callback.from_user.id):
        await callback.answer("❌ Недостаточно прав", show_alert=True)
        return
    chat_id = int(callback.data.split("_")[2])
    async with db_pool.acquire() as conn:
        request = await conn.fetchrow("SELECT * FROM chat_confirmation_requests WHERE chat_id=$1", chat_id)
        if not request:
            await callback.answer("❌ Запрос не найден", show_alert=True)
            return
        await add_confirmed_chat(chat_id, request['title'], request['type'], callback.from_user.id)
        await update_chat_request_status(chat_id, 'approved')
    await callback.message.edit_text(f"✅ Чат {request['title']} (ID: {chat_id}) подтверждён и активирован.")
    await callback.answer("Чат подтверждён")
    await safe_send_message(request['requested_by'], f"✅ Ваш чат «{request['title']}» активирован! Теперь доступны все функции.")

@dp.callback_query_handler(lambda c: c.data.startswith("reject_chat_"))
async def reject_chat_callback(callback: types.CallbackQuery):
    if not await is_super_admin(callback.from_user.id):
        await callback.answer("❌ Недостаточно прав", show_alert=True)
        return
    chat_id = int(callback.data.split("_")[2])
    async with db_pool.acquire() as conn:
        request = await conn.fetchrow("SELECT * FROM chat_confirmation_requests WHERE chat_id=$1", chat_id)
        if not request:
            await callback.answer("❌ Запрос не найден", show_alert=True)
            return
        await update_chat_request_status(chat_id, 'rejected')
    await callback.message.edit_text(f"❌ Запрос для чата {request['title']} (ID: {chat_id}) отклонён.")
    await callback.answer("Запрос отклонён")
    await safe_send_message(request['requested_by'], f"❌ Запрос на активацию чата «{request['title']}» отклонён.")

# ===== ПОДГОН В ЧАТАХ =====
@dp.message_handler(lambda message: message.chat.type != 'private' and message.text == "🎁 Подгон")
async def chat_gift(message: types.Message):
    chat_id = message.chat.id
    user_id = message.from_user.id
    if await is_banned(user_id):
        return
    if not await is_chat_confirmed(chat_id):
        await message.reply("❌ Этот чат ещё не активирован. Ожидайте подтверждения администратора.")
        return

    gift_amount = int(await get_setting("gift_amount"))
    gift_limit_per_chat = int(await get_setting("gift_limit_per_day"))
    gift_global_limit = int(await get_setting("gift_global_limit_per_user"))
    gift_cooldown = int(await get_setting("gift_cooldown"))
    today = date.today().isoformat()
    now = datetime.now()

    async with db_pool.acquire() as conn:
        chat_info = await conn.fetchrow("SELECT * FROM confirmed_chats WHERE chat_id=$1", chat_id)
        if not chat_info:
            return
        last_gift_date = chat_info['last_gift_date']
        gift_count_today = chat_info['gift_count_today'] if last_gift_date == today else 0
        if gift_count_today >= gift_limit_per_chat:
            await message.reply(f"❌ Сегодня в этом чате уже использовано {gift_count_today} из {gift_limit_per_chat} подгонов.")
            return

        user = await conn.fetchrow("SELECT last_gift_time, gift_count_today FROM users WHERE user_id=$1", user_id)
        if not user:
            await conn.execute(
                "INSERT INTO users (user_id, username, first_name, joined_date) VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING",
                user_id, message.from_user.username, message.from_user.first_name, datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            )
            user = {'last_gift_time': None, 'gift_count_today': 0}
        if user['last_gift_time']:
            user_last_gift = user['last_gift_time']
            user_gift_count = user['gift_count_today'] if user_last_gift.startswith(today) else 0
        else:
            user_gift_count = 0
        if user_gift_count >= gift_global_limit:
            await message.reply(f"❌ Сегодня ты уже получил {user_gift_count} из {gift_global_limit} подгонов во всех чатах.")
            return

        if user['last_gift_time']:
            last_gift = datetime.strptime(user['last_gift_time'], "%Y-%m-%d %H:%M:%S")
            diff = (now - last_gift).total_seconds() / 60
            if diff < gift_cooldown:
                remaining = int(gift_cooldown - diff)
                await message.reply(f"⏳ Подгон можно будет использовать через {remaining} мин.")
                return

        try:
            admins = await bot.get_chat_administrators(chat_id)
            eligible = [a.user for a in admins if a.user.id != user_id and not await is_banned(a.user.id)]
            if not eligible:
                await message.reply("❌ Нет подходящих получателей для подарка.")
                return
            recipient = random.choice(eligible)
        except Exception as e:
            logging.error(f"Gift error: {e}")
            await message.reply("❌ Не удалось выбрать получателя.")
            return

        await conn.execute(
            "INSERT INTO users (user_id, username, first_name, joined_date) VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING",
            recipient.id, recipient.username, recipient.first_name, datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        )
        await update_user_balance(recipient.id, gift_amount)

        if last_gift_date == today:
            await conn.execute("UPDATE confirmed_chats SET gift_count_today = gift_count_today + 1 WHERE chat_id=$1", chat_id)
        else:
            await conn.execute("UPDATE confirmed_chats SET last_gift_date=$1, gift_count_today=1 WHERE chat_id=$2", today, chat_id)

        if user['last_gift_time'] and user['last_gift_time'].startswith(today):
            await conn.execute("UPDATE users SET gift_count_today = gift_count_today + 1, last_gift_time=$1 WHERE user_id=$2",
                               now.strftime("%Y-%m-%d %H:%M:%S"), user_id)
        else:
            await conn.execute("UPDATE users SET gift_count_today=1, last_gift_time=$1 WHERE user_id=$2",
                               now.strftime("%Y-%m-%d %H:%M:%S"), user_id)

    await message.answer(
        f"🎁 {message.from_user.first_name} активировал подгон!\n"
        f"Счастливчик: {recipient.first_name} получает {gift_amount} монет! 🎉\n"
        f"📊 Сегодня в этом чате осталось подгонов: {gift_limit_per_chat - (gift_count_today + 1)}"
    )

# ===== СИСТЕМА БОССОВ =====
async def spawn_boss(chat_id: int, level: int = None):
    if level is None:
        level = random.randint(1, 5)
    boss_names = [
        "Гоблин-грабитель", "Тролль-вышибала", "Дракончик", "Злобный Кролик",
        "Крысиный Король", "Лесной Дух", "Каменный Голем", "Огненный Элементаль"
    ]
    name = random.choice(boss_names)
    hp_mult = int(await get_setting("boss_hp_multiplier"))
    hp = level * hp_mult * random.randint(5, 10)
    base_reward = int(await get_setting("boss_reward_coins"))
    variance = int(await get_setting("boss_reward_coins_variance"))
    reward = base_reward + random.randint(-variance, variance)
    now = datetime.now()
    expires_at = now + timedelta(hours=2)
    async with db_pool.acquire() as conn:
        boss_id = await conn.fetchval(
            "INSERT INTO bosses (chat_id, name, level, hp, max_hp, spawned_at, expires_at, reward_coins, participants, status) "
            "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) RETURNING id",
            chat_id, name, level, hp, hp, now.strftime("%Y-%m-%d %H:%M:%S"),
            expires_at.strftime("%Y-%m-%d %H:%M:%S"), reward, [], 'active'
        )
        await conn.execute(
            "UPDATE confirmed_chats SET boss_last_spawn=$1, boss_spawn_count = boss_spawn_count + 1 WHERE chat_id=$2",
            now.strftime("%Y-%m-%d %H:%M:%S"), chat_id
        )
    phrase = random.choice(BOSS_SPAWN_PHRASES).format(name=name, level=level, hp=hp)
    await safe_send_chat(chat_id, phrase, reply_markup=boss_attack_keyboard())

@dp.callback_query_handler(lambda c: c.data == "boss_attack")
async def boss_attack_callback(callback: types.CallbackQuery):
    if callback.message.chat.type == 'private':
        await callback.answer("❌ Эта команда работает только в групповых чатах.", show_alert=True)
        return
    chat_id = callback.message.chat.id
    user_id = callback.from_user.id
    if not await is_chat_confirmed(chat_id):
        await callback.answer("❌ Чат не активирован.", show_alert=True)
        return
    async with db_pool.acquire() as conn:
        boss = await conn.fetchrow(
            "SELECT * FROM bosses WHERE chat_id=$1 AND status='active' AND expires_at > $2 ORDER BY spawned_at DESC LIMIT 1",
            chat_id, datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        )
        if not boss:
            await callback.answer("❌ В этом чате сейчас нет активного босса.", show_alert=True)
            return
        user_exists = await conn.fetchval("SELECT 1 FROM users WHERE user_id=$1", user_id)
        if not user_exists:
            await conn.execute(
                "INSERT INTO users (user_id, username, first_name, joined_date) VALUES ($1, $2, $3, $4)",
                user_id, callback.from_user.username, callback.from_user.first_name, datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            )
        attack = await conn.fetchrow("SELECT * FROM boss_attacks WHERE boss_id=$1 AND user_id=$2", boss['id'], user_id)
        if attack:
            cooldown = int(await get_setting("boss_attack_cooldown"))
            last_attack = datetime.strptime(attack['attack_time'], "%Y-%m-%d %H:%M:%S")
            if datetime.now() - last_attack < timedelta(minutes=cooldown):
                remaining = cooldown - int((datetime.now() - last_attack).total_seconds() // 60)
                await callback.answer(f"⏳ Ты сможешь атаковать снова через {remaining} мин.", show_alert=True)
                return
        else:
            participants = boss['participants'] or []
            if user_id not in participants:
                participants.append(user_id)
                await conn.execute("UPDATE bosses SET participants=$1 WHERE id=$2", participants, boss['id'])

        stats = await get_user_stats(user_id)
        strength = stats['strength']
        agility = stats['agility']
        defense = stats['defense']
        base_damage = int(await get_setting("boss_base_damage"))
        damage = base_damage + random.randint(-3, 3) + strength // 2
        if damage < 1:
            damage = 1
        hit_chance = 50 + agility * 2
        if hit_chance > 95:
            hit_chance = 95

        if random.randint(1, 100) > hit_chance:
            phrase = random.choice(BOSS_MISS_PHRASES)
            damage = 0
        else:
            phrase = random.choice(BOSS_HIT_PHRASES).format(damage=damage)
            new_hp = boss['hp'] - damage
            if new_hp < 0:
                new_hp = 0
            await conn.execute("UPDATE bosses SET hp=$1 WHERE id=$2", new_hp, boss['id'])

        if attack:
            await conn.execute(
                "UPDATE boss_attacks SET damage=$1, attack_time=$2 WHERE boss_id=$3 AND user_id=$4",
                damage, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), boss['id'], user_id
            )
        else:
            await conn.execute(
                "INSERT INTO boss_attacks (boss_id, user_id, damage, attack_time) VALUES ($1, $2, $3, $4)",
                boss['id'], user_id, damage, datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            )

        effect_text = ""
        if random.randint(1, 100) <= 20:
            effect = random.choice(["stun", "slow", "poison"])
            if effect == "stun":
                effect_text = "😵 Босс оглушил тебя! Твой следующий удар задержится."
            elif effect == "slow":
                effect_text = "🐌 Босс замедлил тебя! Кулдаун увеличен на 1 мин."
            elif effect == "poison":
                poison_damage = random.randint(1, 5)
                await update_user_balance(user_id, -poison_damage)
                effect_text = f"☠️ Босс отравил тебя! Ты потерял {poison_damage} монет."

        if new_hp <= 0:
            await finish_boss_fight(boss['id'])
            await callback.answer("⚔️ Ты нанёс последний удар! Босс повержен!", show_alert=False)
        else:
            hp = new_hp
            max_hp = boss['max_hp']
            bar_length = 10
            filled = int((hp / max_hp) * bar_length)
            bar = "🟥" * filled + "⬜" * (bar_length - filled)
            status = f"{boss['name']} | Уровень {boss['level']} | HP: {hp}/{max_hp}\n{bar}"
            await callback.message.answer(f"{phrase}\n\n{status}\n\n{effect_text}".strip())
            await callback.answer()

        try:
            await callback.message.delete()
        except:
            pass

async def finish_boss_fight(boss_id: int):
    async with db_pool.acquire() as conn:
        boss = await conn.fetchrow("SELECT * FROM bosses WHERE id=$1", boss_id)
        if not boss or boss['status'] != 'active':
            return
        participants = boss['participants'] or []
        if not participants:
            await conn.execute("UPDATE bosses SET status='defeated' WHERE id=$1", boss_id)
            return
        reward_total = boss['reward_coins']
        reward_per_player = reward_total // len(participants)
        remainder = reward_total % len(participants)
        for i, uid in enumerate(participants):
            reward = reward_per_player + (1 if i < remainder else 0)
            await update_user_balance(uid, reward)
            exp = int(await get_setting("exp_per_game_win"))
            await add_exp(uid, exp)
        await conn.execute("UPDATE bosses SET status='defeated' WHERE id=$1", boss_id)
        phrase = random.choice(BOSS_DEATH_PHRASES).format(name=boss['name'])
        await safe_send_chat(boss['chat_id'], f"{phrase}\nУчастники получили по {reward_per_player} монет!")

# ===== НАЗАД В ГЛАВНОЕ МЕНЮ =====
@dp.message_handler(lambda message: message.text == "◀️ Назад в главное меню")
async def back_to_main_from_admin(message: types.Message):
    if message.chat.type != 'private':
        return
    admin_flag = await is_admin(message.from_user.id)
    await message.answer("Главное меню:", reply_markup=user_main_keyboard(admin_flag))

@dp.message_handler(lambda message: message.text == "◀️ Назад")
async def back_from_submenu(message: types.Message):
    if message.chat.type != 'private':
        return
    admin_flag = await is_admin(message.from_user.id)
    await message.answer("Главное меню:", reply_markup=user_main_keyboard(admin_flag))

# ===== ОБРАБОТКА НЕИЗВЕСТНЫХ СООБЩЕНИЙ =====
@dp.message_handler()
async def unknown_message(message: types.Message):
    if message.chat.type != 'private':
        return
    if await is_banned(message.from_user.id) and not await is_admin(message.from_user.id):
        return
    admin_flag = await is_admin(message.from_user.id)
    await message.answer("Я не понимаю эту команду. Используй кнопки меню.", reply_markup=user_main_keyboard(admin_flag))

# ===== КОНЕЦ ВТОРОЙ ЧАСТИ =====
# ===== ТРЕТЬЯ ЧАСТЬ (АДМИНИСТРАТИВНЫЕ ХЕНДЛЕРЫ, ФОНОВЫЕ ЗАДАЧИ, ЗАПУСК) =====

# ===== АДМИНИСТРАТИВНЫЕ ХЕНДЛЕРЫ =====

# ----- Вход в админ-панель -----
@dp.message_handler(lambda message: message.text == "⚙️ Админ панель")
async def admin_panel(message: types.Message):
    if message.chat.type != 'private':
        return
    if not await is_admin(message.from_user.id):
        await message.answer("У тебя нет прав администратора.")
        return
    super_admin = await is_super_admin(message.from_user.id)
    await message.answer("Панель администратора:", reply_markup=admin_main_keyboard(super_admin))

# ----- Управление пользователями -----
@dp.message_handler(lambda message: message.text == "👥 Управление пользователями")
async def admin_users_menu(message: types.Message):
    if not await is_admin(message.from_user.id):
        return
    await message.answer("Управление пользователями:", reply_markup=admin_users_keyboard())

@dp.message_handler(lambda message: message.text == "💰 Начислить монеты")
async def add_balance_start(message: types.Message):
    if not await is_super_admin(message.from_user.id):
        await message.answer("❌ Только суперадмин может начислять монеты.")
        return
    await message.answer("Введи ID или @username пользователя:", reply_markup=back_keyboard())
    await AddBalance.user_id.set()

@dp.message_handler(state=AddBalance.user_id)
async def add_balance_user(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_users_menu(message)
        return
    user_data = await find_user_by_input(message.text)
    if not user_data:
        await message.answer("❌ Пользователь не найден.")
        return
    uid = user_data['user_id']
    await state.update_data(user_id=uid)
    await message.answer("Введи сумму начисления (целое положительное число):")
    await AddBalance.amount.set()

@dp.message_handler(state=AddBalance.amount)
async def add_balance_amount(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_users_menu(message)
        return
    try:
        amount = int(message.text)
        if amount <= 0:
            raise ValueError
    except ValueError:
        await message.answer("❌ Введи положительное целое число.")
        return
    data = await state.get_data()
    uid = data['user_id']
    try:
        await update_user_balance(uid, amount)
        await message.answer(f"✅ Пользователю {uid} начислено {amount} монет.")
        await safe_send_message(uid, f"💰 Вам начислено {amount} монет администратором.")
    except Exception as e:
        logging.error(f"Add balance error: {e}")
        await message.answer("❌ Ошибка.")
    await state.finish()

@dp.message_handler(lambda message: message.text == "💸 Списать монеты")
async def remove_balance_start(message: types.Message):
    if not await is_super_admin(message.from_user.id):
        await message.answer("❌ Только суперадмин может списывать монеты.")
        return
    await message.answer("Введи ID или @username пользователя:", reply_markup=back_keyboard())
    await RemoveBalance.user_id.set()

@dp.message_handler(state=RemoveBalance.user_id)
async def remove_balance_user(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_users_menu(message)
        return
    user_data = await find_user_by_input(message.text)
    if not user_data:
        await message.answer("❌ Пользователь не найден.")
        return
    uid = user_data['user_id']
    await state.update_data(user_id=uid)
    await message.answer("Введи сумму списания (целое положительное число):")
    await RemoveBalance.amount.set()

@dp.message_handler(state=RemoveBalance.amount)
async def remove_balance_amount(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_users_menu(message)
        return
    try:
        amount = int(message.text)
        if amount <= 0:
            raise ValueError
    except ValueError:
        await message.answer("❌ Введи положительное целое число.")
        return
    data = await state.get_data()
    uid = data['user_id']
    try:
        await update_user_balance(uid, -amount)
        await message.answer(f"✅ У пользователя {uid} списано {amount} монет.")
        await safe_send_message(uid, f"💸 У тебя списано {amount} монет администратором.")
    except Exception as e:
        logging.error(f"Remove balance error: {e}")
        await message.answer("❌ Ошибка.")
    await state.finish()

@dp.message_handler(lambda message: message.text == "⭐️ Начислить репутацию")
async def add_reputation_start(message: types.Message):
    if not await is_super_admin(message.from_user.id):
        await message.answer("❌ Только суперадмин может начислять репутацию.")
        return
    await message.answer("Введи ID или @username пользователя:", reply_markup=back_keyboard())
    await AddReputation.user_id.set()

@dp.message_handler(state=AddReputation.user_id)
async def add_reputation_user(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_users_menu(message)
        return
    user_data = await find_user_by_input(message.text)
    if not user_data:
        await message.answer("❌ Пользователь не найден.")
        return
    uid = user_data['user_id']
    await state.update_data(user_id=uid)
    await message.answer("Введи количество репутации для начисления (целое число):")
    await AddReputation.amount.set()

@dp.message_handler(state=AddReputation.amount)
async def add_reputation_amount(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_users_menu(message)
        return
    try:
        amount = int(message.text)
    except ValueError:
        await message.answer("❌ Введи целое число.")
        return
    data = await state.get_data()
    uid = data['user_id']
    try:
        await update_user_reputation(uid, amount)
        await message.answer(f"✅ Пользователю {uid} начислено {amount} репутации.")
        await safe_send_message(uid, f"⭐️ Вам начислено {amount} репутации администратором.")
    except Exception as e:
        logging.error(f"Add reputation error: {e}")
        await message.answer("❌ Ошибка.")
    await state.finish()

@dp.message_handler(lambda message: message.text == "🔻 Снять репутацию")
async def remove_reputation_start(message: types.Message):
    if not await is_super_admin(message.from_user.id):
        await message.answer("❌ Только суперадмин может снимать репутацию.")
        return
    await message.answer("Введи ID или @username пользователя:", reply_markup=back_keyboard())
    await RemoveReputation.user_id.set()

@dp.message_handler(state=RemoveReputation.user_id)
async def remove_reputation_user(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_users_menu(message)
        return
    user_data = await find_user_by_input(message.text)
    if not user_data:
        await message.answer("❌ Пользователь не найден.")
        return
    uid = user_data['user_id']
    await state.update_data(user_id=uid)
    await message.answer("Введи количество репутации для снятия (целое число):")
    await RemoveReputation.amount.set()

@dp.message_handler(state=RemoveReputation.amount)
async def remove_reputation_amount(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_users_menu(message)
        return
    try:
        amount = int(message.text)
    except ValueError:
        await message.answer("❌ Введи целое число.")
        return
    data = await state.get_data()
    uid = data['user_id']
    try:
        await update_user_reputation(uid, -amount)
        await message.answer(f"✅ У пользователя {uid} снято {amount} репутации.")
        await safe_send_message(uid, f"🔻 У вас снято {amount} репутации администратором.")
    except Exception as e:
        logging.error(f"Remove reputation error: {e}")
        await message.answer("❌ Ошибка.")
    await state.finish()

@dp.message_handler(lambda message: message.text == "📈 Начислить опыт")
async def add_exp_start(message: types.Message):
    if not await is_super_admin(message.from_user.id):
        await message.answer("❌ Только суперадмин может начислять опыт.")
        return
    await message.answer("Введи ID или @username пользователя:", reply_markup=back_keyboard())
    await AddExp.user_id.set()

@dp.message_handler(state=AddExp.user_id)
async def add_exp_user(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_users_menu(message)
        return
    user_data = await find_user_by_input(message.text)
    if not user_data:
        await message.answer("❌ Пользователь не найден.")
        return
    uid = user_data['user_id']
    await state.update_data(user_id=uid)
    await message.answer("Введи количество опыта для начисления (целое число):")
    await AddExp.amount.set()

@dp.message_handler(state=AddExp.amount)
async def add_exp_amount(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_users_menu(message)
        return
    try:
        amount = int(message.text)
    except ValueError:
        await message.answer("❌ Введи целое число.")
        return
    data = await state.get_data()
    uid = data['user_id']
    try:
        await add_exp(uid, amount)
        await message.answer(f"✅ Пользователю {uid} начислено {amount} опыта.")
    except Exception as e:
        logging.error(f"Add exp error: {e}")
        await message.answer("❌ Ошибка.")
    await state.finish()

@dp.message_handler(lambda message: message.text == "🔝 Установить уровень")
async def set_level_start(message: types.Message):
    if not await is_super_admin(message.from_user.id):
        await message.answer("❌ Только суперадмин может устанавливать уровень.")
        return
    await message.answer("Введи ID или @username пользователя:", reply_markup=back_keyboard())
    await SetLevel.user_id.set()

@dp.message_handler(state=SetLevel.user_id)
async def set_level_user(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_users_menu(message)
        return
    user_data = await find_user_by_input(message.text)
    if not user_data:
        await message.answer("❌ Пользователь не найден.")
        return
    uid = user_data['user_id']
    await state.update_data(user_id=uid)
    await message.answer("Введи новый уровень (целое число ≥ 1):")
    await SetLevel.level.set()

@dp.message_handler(state=SetLevel.level)
async def set_level_value(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_users_menu(message)
        return
    try:
        level = int(message.text)
        if level < 1:
            raise ValueError
    except ValueError:
        await message.answer("❌ Введи целое число ≥ 1.")
        return
    data = await state.get_data()
    uid = data['user_id']
    try:
        async with db_pool.acquire() as conn:
            await conn.execute("UPDATE users SET level=$1 WHERE user_id=$2", level, uid)
        await message.answer(f"✅ Пользователю {uid} установлен уровень {level}.")
        await safe_send_message(uid, f"🔝 Ваш уровень изменён на {level} администратором.")
    except Exception as e:
        logging.error(f"Set level error: {e}")
        await message.answer("❌ Ошибка.")
    await state.finish()

@dp.message_handler(lambda message: message.text == "👥 Найти пользователя")
async def find_user_start(message: types.Message):
    if not await is_admin(message.from_user.id):
        await message.answer("❌ У тебя нет прав администратора.")
        return
    await message.answer("Введи ID или @username пользователя:", reply_markup=back_keyboard())
    await FindUser.query.set()

@dp.message_handler(state=FindUser.query)
async def find_user_result(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        super_admin = await is_super_admin(message.from_user.id)
        await message.answer("Панель администратора:", reply_markup=admin_main_keyboard(super_admin))
        return
    user_data = await find_user_by_input(message.text)
    if not user_data:
        await message.answer("❌ Пользователь не найден.")
        return
    uid = user_data['user_id']
    name = user_data['first_name']
    bal = user_data['balance']
    rep = user_data['reputation']
    spent = user_data['total_spent']
    joined = user_data['joined_date']
    attempts = user_data['theft_attempts']
    success = user_data['theft_success']
    failed = user_data['theft_failed']
    protected = user_data['theft_protected']
    level = user_data['level']
    exp = user_data['exp']
    strength = user_data['strength']
    agility = user_data['agility']
    defense = user_data['defense']
    banned = await is_banned(uid)
    ban_status = "⛔ Заблокирован" if banned else "✅ Активен"
    text = (
        f"👤 Пользователь: {name} (ID: {uid})\n"
        f"📊 Уровень: {level}, опыт: {exp}\n"
        f"💪 Сила: {strength} | 🏃 Ловкость: {agility} | 🛡 Защита: {defense}\n"
        f"💰 Баланс: {bal}\n"
        f"⭐️ Репутация: {rep}\n"
        f"💸 Потрачено: {spent}\n"
        f"📅 Регистрация: {joined}\n"
        f"🔫 Ограблений: {attempts} (успешно: {success}, провал: {failed})\n"
        f"⚔️ Отбито атак: {protected}\n"
        f"Статус: {ban_status}"
    )
    await message.answer(text)
    await state.finish()

# ----- Управление магазином -----
@dp.message_handler(lambda message: message.text == "🛒 Управление магазином")
async def admin_shop_menu(message: types.Message):
    if not await is_super_admin(message.from_user.id):
        await message.answer("❌ Только суперадмин может управлять магазином.")
        return
    await message.answer("Управление магазином:", reply_markup=admin_shop_keyboard())

@dp.message_handler(lambda message: message.text == "➕ Добавить товар")
async def add_shop_item_start(message: types.Message):
    if not await is_super_admin(message.from_user.id):
        await message.answer("❌ Только суперадмин может добавлять товары.")
        return
    await message.answer("Введи название товара:", reply_markup=back_keyboard())
    await AddShopItem.name.set()

@dp.message_handler(state=AddShopItem.name)
async def add_shop_item_name(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_shop_menu(message)
        return
    await state.update_data(name=message.text)
    await message.answer("Введи описание товара:")
    await AddShopItem.next()

@dp.message_handler(state=AddShopItem.description)
async def add_shop_item_description(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_shop_menu(message)
        return
    await state.update_data(description=message.text)
    await message.answer("Введи цену (целое число):")
    await AddShopItem.next()

@dp.message_handler(state=AddShopItem.price)
async def add_shop_item_price(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_shop_menu(message)
        return
    try:
        price = int(message.text)
        if price <= 0:
            raise ValueError
    except ValueError:
        await message.answer("❌ Цена должна быть положительным целым числом.")
        return
    await state.update_data(price=price)
    await message.answer("Введи количество товара (целое число, -1 для бесконечного):")
    await AddShopItem.stock.set()

@dp.message_handler(state=AddShopItem.stock)
async def add_shop_item_stock(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_shop_menu(message)
        return
    try:
        stock = int(message.text)
    except ValueError:
        await message.answer("❌ Введи целое число.")
        return
    data = await state.get_data()
    try:
        async with db_pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO shop_items (name, description, price, stock) VALUES ($1, $2, $3, $4)",
                data['name'], data['description'], data['price'], stock
            )
        await message.answer("✅ Товар добавлен!", reply_markup=admin_shop_keyboard())
    except Exception as e:
        logging.error(f"Add shop item error: {e}")
        await message.answer("❌ Ошибка при добавлении товара.")
    await state.finish()

@dp.message_handler(lambda message: message.text == "➖ Удалить товар")
async def remove_shop_item_start(message: types.Message):
    if not await is_super_admin(message.from_user.id):
        await message.answer("❌ Только суперадмин может удалять товары.")
        return
    try:
        async with db_pool.acquire() as conn:
            items = await conn.fetch("SELECT id, name FROM shop_items ORDER BY id")
        if not items:
            await message.answer("В магазине нет товаров.")
            return
        text = "Товары:\n" + "\n".join([f"ID {i['id']}: {i['name']}" for i in items])
        await message.answer(text + "\n\nВведи ID товара для удаления:", reply_markup=back_keyboard())
    except Exception as e:
        logging.error(f"List items for remove error: {e}")
        await message.answer("❌ Ошибка.")
        return
    await RemoveShopItem.item_id.set()

@dp.message_handler(state=RemoveShopItem.item_id)
async def remove_shop_item(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_shop_menu(message)
        return
    try:
        item_id = int(message.text)
    except ValueError:
        await message.answer("❌ Введи число.")
        return
    try:
        async with db_pool.acquire() as conn:
            await conn.execute("DELETE FROM shop_items WHERE id=$1", item_id)
        await message.answer("✅ Товар удалён, если существовал.", reply_markup=admin_shop_keyboard())
    except Exception as e:
        logging.error(f"Remove shop item error: {e}")
        await message.answer("❌ Ошибка.")
    await state.finish()

@dp.message_handler(lambda message: message.text == "📋 Список товаров")
async def list_shop_items(message: types.Message):
    if not await is_super_admin(message.from_user.id):
        await message.answer("❌ Только суперадмин может просматривать список товаров.")
        return
    page = 1
    try:
        parts = message.text.split()
        if len(parts) > 1:
            page = int(parts[1])
    except:
        pass
    offset = (page - 1) * ITEMS_PER_PAGE
    try:
        async with db_pool.acquire() as conn:
            total = await conn.fetchval("SELECT COUNT(*) FROM shop_items")
            items = await conn.fetch(
                "SELECT id, name, description, price, stock FROM shop_items ORDER BY id LIMIT $1 OFFSET $2",
                ITEMS_PER_PAGE, offset
            )
        if not items:
            await message.answer("В магазине нет товаров.")
            return
        text = f"📦 Товары (страница {page}):\n"
        for item in items:
            text += f"\nID {item['id']} | {item['name']}\n{item['description']}\n💰 {item['price']} | наличие: {item['stock'] if item['stock']!=-1 else '∞'}\n"
        kb = []
        nav_buttons = []
        if page > 1:
            nav_buttons.append(InlineKeyboardButton(text="⬅️", callback_data=f"shopitems_page_{page-1}"))
        if offset + ITEMS_PER_PAGE < total:
            nav_buttons.append(InlineKeyboardButton(text="➡️", callback_data=f"shopitems_page_{page+1}"))
        if nav_buttons:
            kb.append(nav_buttons)
        if kb:
            await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))
        else:
            await message.answer(text, reply_markup=admin_shop_keyboard())
    except Exception as e:
        logging.error(f"List shop items error: {e}")
        await message.answer("❌ Ошибка.")

@dp.callback_query_handler(lambda c: c.data.startswith("shopitems_page_"))
async def shopitems_page_callback(callback: types.CallbackQuery):
    page = int(callback.data.split("_")[2])
    callback.message.text = f"📋 Список товаров {page}"
    await list_shop_items(callback.message)
    await callback.answer()

@dp.message_handler(lambda message: message.text == "✏️ Редактировать товар")
async def edit_shop_item_start(message: types.Message):
    if not await is_super_admin(message.from_user.id):
        await message.answer("❌ Только суперадмин может редактировать товары.")
        return
    await message.answer("Введи ID товара для редактирования:", reply_markup=back_keyboard())
    await EditShopItem.item_id.set()

@dp.message_handler(state=EditShopItem.item_id)
async def edit_shop_item_field(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_shop_menu(message)
        return
    try:
        item_id = int(message.text)
    except ValueError:
        await message.answer("❌ Введи число.")
        return
    await state.update_data(item_id=item_id)
    await message.answer("Что хочешь изменить? (price/stock)", reply_markup=back_keyboard())
    await EditShopItem.field.set()

@dp.message_handler(state=EditShopItem.field)
async def edit_shop_item_value(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_shop_menu(message)
        return
    field = message.text.lower()
    if field not in ['price', 'stock']:
        await message.answer("❌ Можно изменить только price или stock.")
        return
    await state.update_data(field=field)
    await message.answer(f"Введи новое значение для {field}:")
    await EditShopItem.value.set()

@dp.message_handler(state=EditShopItem.value)
async def edit_shop_item_final(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_shop_menu(message)
        return
    try:
        value = int(message.text)
    except ValueError:
        await message.answer("❌ Введи целое число.")
        return
    data = await state.get_data()
    item_id = data['item_id']
    field = data['field']
    try:
        async with db_pool.acquire() as conn:
            await conn.execute(f"UPDATE shop_items SET {field}=$1 WHERE id=$2", value, item_id)
        await message.answer("✅ Товар обновлён.", reply_markup=admin_shop_keyboard())
    except Exception as e:
        logging.error(f"Edit shop item error: {e}")
        await message.answer("❌ Ошибка.")
    await state.finish()

@dp.message_handler(lambda message: message.text == "🛍️ Список покупок")
async def admin_purchases(message: types.Message):
    if not await is_admin(message.from_user.id):
        await message.answer("❌ У тебя нет прав администратора.")
        return
    try:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT p.id, u.user_id, u.username, s.name, p.purchase_date, p.status FROM purchases p "
                "JOIN users u ON p.user_id = u.user_id JOIN shop_items s ON p.item_id = s.id "
                "WHERE p.status='pending' ORDER BY p.purchase_date"
            )
        if not rows:
            await message.answer("Нет необработанных покупок.")
            return
        for row in rows:
            pid, uid, username, item_name, date, status = row['id'], row['user_id'], row['username'], row['name'], row['purchase_date'], row['status']
            text = f"🆔 {pid}\nПользователь: {uid} (@{username})\nТовар: {item_name}\nДата: {date}"
            await message.answer(text, reply_markup=purchase_action_keyboard(pid))
    except Exception as e:
        logging.error(f"Admin purchases error: {e}")
        await message.answer("❌ Ошибка загрузки покупок.")

@dp.callback_query_handler(lambda c: c.data.startswith("purchase_done_"))
async def purchase_done(callback: types.CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("Недостаточно прав", show_alert=True)
        return
    purchase_id = int(callback.data.split("_")[2])
    try:
        async with db_pool.acquire() as conn:
            await conn.execute("UPDATE purchases SET status='completed' WHERE id=$1", purchase_id)
            user_id = await conn.fetchval("SELECT user_id FROM purchases WHERE id=$1", purchase_id)
            if user_id:
                await safe_send_message(user_id, "✅ Твоя покупка обработана! Админ выслал подарок.")
        await callback.answer("Покупка отмечена как выполненная")
        await callback.message.delete()
    except Exception as e:
        logging.error(f"Purchase done error: {e}")
        await callback.answer("Ошибка", show_alert=True)

@dp.callback_query_handler(lambda c: c.data.startswith("purchase_reject_"))
async def purchase_reject(callback: types.CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("Недостаточно прав", show_alert=True)
        return
    purchase_id = int(callback.data.split("_")[2])
    try:
        async with db_pool.acquire() as conn:
            await conn.execute("UPDATE purchases SET status='rejected' WHERE id=$1", purchase_id)
            user_id = await conn.fetchval("SELECT user_id FROM purchases WHERE id=$1", purchase_id)
            if user_id:
                await safe_send_message(user_id, "❌ К сожалению, твоя покупка не может быть выполнена. Свяжись с админом.")
        await callback.answer("Покупка отклонена")
        await callback.message.delete()
    except Exception as e:
        logging.error(f"Purchase reject error: {e}")
        await callback.answer("Ошибка", show_alert=True)

# ----- Управление розыгрышами -----
@dp.message_handler(lambda message: message.text == "🎁 Управление розыгрышами")
async def admin_giveaway_menu(message: types.Message):
    if not await is_super_admin(message.from_user.id):
        await message.answer("❌ Только суперадмин может управлять розыгрышами.")
        return
    await message.answer("Управление розыгрышами:", reply_markup=admin_giveaway_keyboard())

@dp.message_handler(lambda message: message.text == "➕ Создать розыгрыш")
async def create_giveaway_start(message: types.Message):
    if not await is_super_admin(message.from_user.id):
        await message.answer("❌ Только суперадмин может создавать розыгрыши.")
        return
    await message.answer("Введи название приза:", reply_markup=back_keyboard())
    await CreateGiveaway.prize.set()

@dp.message_handler(state=CreateGiveaway.prize)
async def create_giveaway_prize(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_giveaway_menu(message)
        return
    await state.update_data(prize=message.text)
    await message.answer("Введи описание розыгрыша:")
    await CreateGiveaway.description.set()

@dp.message_handler(state=CreateGiveaway.description)
async def create_giveaway_description(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_giveaway_menu(message)
        return
    await state.update_data(description=message.text)
    await message.answer("Введи дату окончания в формате ДД.ММ.ГГГГ ЧЧ:ММ (например, 31.12.2025 23:59):")
    await CreateGiveaway.end_date.set()

@dp.message_handler(state=CreateGiveaway.end_date)
async def create_giveaway_end_date(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_giveaway_menu(message)
        return
    try:
        end_date = datetime.strptime(message.text, "%d.%m.%Y %H:%M")
        if end_date <= datetime.now():
            await message.answer("Дата окончания должна быть в будущем.")
            return
        await state.update_data(end_date=end_date.strftime("%Y-%m-%d %H:%M:%S"))
    except ValueError:
        await message.answer("Неверный формат. Используй ДД.ММ.ГГГГ ЧЧ:ММ")
        return
    await message.answer("Отправь медиа (фото, видео или документ) для розыгрыша или отправь 'пропустить':")
    await CreateGiveaway.media.set()

@dp.message_handler(state=CreateGiveaway.media, content_types=['text', 'photo', 'video', 'document'])
async def create_giveaway_media(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_giveaway_menu(message)
        return
    data = await state.get_data()
    media_file_id = None
    media_type = None
    if message.photo:
        media_file_id = message.photo[-1].file_id
        media_type = 'photo'
    elif message.video:
        media_file_id = message.video.file_id
        media_type = 'video'
    elif message.document:
        media_file_id = message.document.file_id
        media_type = 'document'
    elif message.text and message.text.lower() == 'пропустить':
        pass
    else:
        await message.answer("Пожалуйста, отправь фото, видео, документ или 'пропустить'.")
        return

    try:
        async with db_pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO giveaways (prize, description, end_date, media_file_id, media_type) VALUES ($1, $2, $3, $4, $5)",
                data['prize'], data['description'], data['end_date'], media_file_id, media_type
            )
        await message.answer("✅ Розыгрыш создан!", reply_markup=admin_giveaway_keyboard())
    except Exception as e:
        logging.error(f"Create giveaway error: {e}")
        await message.answer("❌ Ошибка при создании розыгрыша.")
    await state.finish()

@dp.message_handler(lambda message: message.text == "📋 Активные розыгрыши")
async def list_active_giveaways(message: types.Message):
    if not await is_super_admin(message.from_user.id):
        await message.answer("❌ Только суперадмин может просматривать активные розыгрыши.")
        return
    page = 1
    try:
        parts = message.text.split()
        if len(parts) > 1:
            page = int(parts[1])
    except:
        pass
    offset = (page - 1) * ITEMS_PER_PAGE
    try:
        async with db_pool.acquire() as conn:
            total = await conn.fetchval("SELECT COUNT(*) FROM giveaways WHERE status='active'")
            rows = await conn.fetch(
                "SELECT id, prize, end_date, description FROM giveaways WHERE status='active' ORDER BY end_date LIMIT $1 OFFSET $2",
                ITEMS_PER_PAGE, offset
            )
        if not rows:
            await message.answer("Нет активных розыгрышей.")
            return
        text = f"Активные розыгрыши (страница {page}):\n"
        for row in rows:
            gid, prize, end, desc = row['id'], row['prize'], row['end_date'], row['description']
            async with db_pool.acquire() as conn2:
                count = await conn2.fetchval("SELECT COUNT(*) FROM participants WHERE giveaway_id=$1", gid)
            text += f"ID: {gid} | {prize} | до {end} | 👥 {count} участников\n{desc}\n\n"
        kb = []
        nav_buttons = []
        if page > 1:
            nav_buttons.append(InlineKeyboardButton(text="⬅️", callback_data=f"activegiveaways_page_{page-1}"))
        if offset + ITEMS_PER_PAGE < total:
            nav_buttons.append(InlineKeyboardButton(text="➡️", callback_data=f"activegiveaways_page_{page+1}"))
        if nav_buttons:
            kb.append(nav_buttons)
        if kb:
            await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))
        else:
            await message.answer(text, reply_markup=admin_giveaway_keyboard())
    except Exception as e:
        logging.error(f"List giveaways error: {e}")
        await message.answer("❌ Ошибка.")

@dp.callback_query_handler(lambda c: c.data.startswith("activegiveaways_page_"))
async def activegiveaways_page_callback(callback: types.CallbackQuery):
    page = int(callback.data.split("_")[2])
    callback.message.text = f"📋 Активные розыгрыши {page}"
    await list_active_giveaways(callback.message)
    await callback.answer()

@dp.message_handler(lambda message: message.text == "✅ Завершить розыгрыш")
async def finish_giveaway_start(message: types.Message):
    if not await is_super_admin(message.from_user.id):
        await message.answer("❌ Только суперадмин может завершать розыгрыши.")
        return
    await message.answer("Введи ID розыгрыша, который нужно завершить:", reply_markup=back_keyboard())
    await CompleteGiveaway.giveaway_id.set()

@dp.message_handler(state=CompleteGiveaway.giveaway_id)
async def finish_giveaway(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_giveaway_menu(message)
        return
    try:
        gid = int(message.text)
    except ValueError:
        await message.answer("❌ Введи число.")
        return
    await state.update_data(giveaway_id=gid)
    await message.answer("Введи количество победителей (целое число):")
    await CompleteGiveaway.winners_count.set()

@dp.message_handler(state=CompleteGiveaway.winners_count)
async def finish_giveaway_winners(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_giveaway_menu(message)
        return
    try:
        winners_count = int(message.text)
        if winners_count < 1:
            raise ValueError
    except ValueError:
        await message.answer("❌ Введи положительное целое число.")
        return
    data = await state.get_data()
    gid = data['giveaway_id']
    try:
        async with db_pool.acquire() as conn:
            status = await conn.fetchval("SELECT status FROM giveaways WHERE id=$1", gid)
            if not status or status != 'active':
                await message.answer("Розыгрыш не активен или не существует.")
                await state.finish()
                return
            participants = await conn.fetch("SELECT user_id FROM participants WHERE giveaway_id=$1", gid)
            participants = [r['user_id'] for r in participants]
            if not participants:
                await message.answer("В этом розыгрыше нет участников.")
                await state.finish()
                return
            if winners_count > len(participants):
                winners_count = len(participants)
            winners = random.sample(participants, winners_count)
            await conn.execute("UPDATE giveaways SET status='completed', winner_id=$1 WHERE id=$2", winners[0], gid)
            for wid in winners:
                await safe_send_message(wid, f"🎉 Поздравляем! Ты выиграл в розыгрыше! Свяжись с админом.")
        await message.answer(f"🏆 Победители выбраны! ({len(winners)})", reply_markup=admin_giveaway_keyboard())
    except Exception as e:
        logging.error(f"Finish giveaway error: {e}")
        await message.answer("❌ Ошибка.")
    await state.finish()

# ----- Управление каналами (для подписки) -----
@dp.message_handler(lambda message: message.text == "📢 Управление каналами")
async def admin_channel_menu(message: types.Message):
    if not await is_super_admin(message.from_user.id):
        await message.answer("❌ Только суперадмин может управлять каналами.")
        return
    await message.answer("Управление каналами:", reply_markup=admin_channel_keyboard())

@dp.message_handler(lambda message: message.text == "➕ Добавить канал")
async def add_channel_start(message: types.Message):
    if not await is_super_admin(message.from_user.id):
        await message.answer("❌ Только суперадмин может добавлять каналы.")
        return
    await message.answer("Введи chat_id канала (можно получить у @username_to_id_bot):", reply_markup=back_keyboard())
    await AddChannel.chat_id.set()

@dp.message_handler(state=AddChannel.chat_id)
async def add_channel_chat_id(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_channel_menu(message)
        return
    await state.update_data(chat_id=message.text.strip())
    await message.answer("Введи название канала:")
    await AddChannel.next()

@dp.message_handler(state=AddChannel.title)
async def add_channel_title(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_channel_menu(message)
        return
    await state.update_data(title=message.text)
    await message.answer("Введи invite-ссылку (или отправь 'нет'):")
    await AddChannel.next()

@dp.message_handler(state=AddChannel.invite_link)
async def add_channel_link(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_channel_menu(message)
        return
    link = None if message.text.lower() == 'нет' else message.text.strip()
    data = await state.get_data()
    try:
        async with db_pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO channels (chat_id, title, invite_link) VALUES ($1, $2, $3)",
                data['chat_id'], data['title'], link
            )
        await message.answer("✅ Канал добавлен!", reply_markup=admin_channel_keyboard())
    except asyncpg.UniqueViolationError:
        await message.answer("❌ Канал с таким chat_id уже существует.")
    except Exception as e:
        logging.error(f"Add channel error: {e}")
        await message.answer("❌ Ошибка.")
    await state.finish()

@dp.message_handler(lambda message: message.text == "➖ Удалить канал")
async def remove_channel_start(message: types.Message):
    if not await is_super_admin(message.from_user.id):
        await message.answer("❌ Только суперадмин может удалять каналы.")
        return
    await message.answer("Введи chat_id канала для удаления:", reply_markup=back_keyboard())
    await RemoveChannel.chat_id.set()

@dp.message_handler(state=RemoveChannel.chat_id)
async def remove_channel(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_channel_menu(message)
        return
    chat_id = message.text.strip()
    try:
        async with db_pool.acquire() as conn:
            await conn.execute("DELETE FROM channels WHERE chat_id=$1", chat_id)
        await message.answer("✅ Канал удалён, если существовал.", reply_markup=admin_channel_keyboard())
    except Exception as e:
        logging.error(f"Remove channel error: {e}")
        await message.answer("❌ Ошибка.")
    await state.finish()

@dp.message_handler(lambda message: message.text == "📋 Список каналов")
async def list_channels(message: types.Message):
    if not await is_super_admin(message.from_user.id):
        await message.answer("❌ Только суперадмин может просматривать список каналов.")
        return
    channels = await get_channels()
    if not channels:
        await message.answer("Нет добавленных каналов.")
        return
    text = "📺 Каналы для подписки:\n"
    for chat_id, title, link in channels:
        text += f"• {title} (chat_id: {chat_id})\n  Ссылка: {link or 'нет'}\n"
    await message.answer(text, reply_markup=admin_channel_keyboard())

# ----- Управление промокодами -----
@dp.message_handler(lambda message: message.text == "🎫 Управление промокодами")
async def admin_promo_menu(message: types.Message):
    if not await is_super_admin(message.from_user.id):
        await message.answer("❌ Только суперадмин может управлять промокодами.")
        return
    await message.answer("Управление промокодами:", reply_markup=admin_promo_keyboard())

@dp.message_handler(lambda message: message.text == "➕ Создать промокод")
async def create_promo_start(message: types.Message):
    if not await is_super_admin(message.from_user.id):
        await message.answer("❌ Только суперадмин может создавать промокоды.")
        return
    await message.answer("Введи код промокода (латиница, цифры):", reply_markup=back_keyboard())
    await CreatePromocode.code.set()

@dp.message_handler(state=CreatePromocode.code)
async def create_promo_code(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_promo_menu(message)
        return
    code = message.text.strip().upper()
    await state.update_data(code=code)
    await message.answer("Введи количество монет, которые даёт промокод:")
    await CreatePromocode.next()

@dp.message_handler(state=CreatePromocode.reward)
async def create_promo_reward(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_promo_menu(message)
        return
    try:
        reward = int(message.text)
        if reward <= 0:
            raise ValueError
    except ValueError:
        await message.answer("❌ Введи положительное целое число.")
        return
    await state.update_data(reward=reward)
    await message.answer("Введи максимальное количество использований:")
    await CreatePromocode.next()

@dp.message_handler(state=CreatePromocode.max_uses)
async def create_promo_max_uses(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_promo_menu(message)
        return
    try:
        max_uses = int(message.text)
        if max_uses <= 0:
            raise ValueError
    except ValueError:
        await message.answer("❌ Введи положительное целое число.")
        return
    data = await state.get_data()
    try:
        async with db_pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO promocodes (code, reward, max_uses, created_at) VALUES ($1, $2, $3, $4)",
                data['code'], data['reward'], max_uses, datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            )
        await message.answer("✅ Промокод создан!", reply_markup=admin_promo_keyboard())
    except asyncpg.UniqueViolationError:
        await message.answer("❌ Промокод с таким кодом уже существует.")
    except Exception as e:
        logging.error(f"Create promo error: {e}")
        await message.answer("❌ Ошибка.")
    await state.finish()

@dp.message_handler(lambda message: message.text == "📋 Список промокодов")
async def list_promos(message: types.Message):
    if not await is_super_admin(message.from_user.id):
        await message.answer("❌ Только суперадмин может просматривать список промокодов.")
        return
    page = 1
    try:
        parts = message.text.split()
        if len(parts) > 1:
            page = int(parts[1])
    except:
        pass
    offset = (page - 1) * ITEMS_PER_PAGE
    try:
        async with db_pool.acquire() as conn:
            total = await conn.fetchval("SELECT COUNT(*) FROM promocodes")
            rows = await conn.fetch(
                "SELECT code, reward, max_uses, used_count FROM promocodes LIMIT $1 OFFSET $2",
                ITEMS_PER_PAGE, offset
            )
        if not rows:
            await message.answer("Нет промокодов.")
            return
        text = f"🎫 Промокоды (страница {page}):\n"
        for row in rows:
            text += f"• {row['code']}: {row['reward']} монет, использовано {row['used_count']}/{row['max_uses']}\n"
        kb = []
        nav_buttons = []
        if page > 1:
            nav_buttons.append(InlineKeyboardButton(text="⬅️", callback_data=f"promos_page_{page-1}"))
        if offset + ITEMS_PER_PAGE < total:
            nav_buttons.append(InlineKeyboardButton(text="➡️", callback_data=f"promos_page_{page+1}"))
        if nav_buttons:
            kb.append(nav_buttons)
        if kb:
            await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))
        else:
            await message.answer(text, reply_markup=admin_promo_keyboard())
    except Exception as e:
        logging.error(f"List promos error: {e}")
        await message.answer("❌ Ошибка.")

@dp.callback_query_handler(lambda c: c.data.startswith("promos_page_"))
async def promos_page_callback(callback: types.CallbackQuery):
    page = int(callback.data.split("_")[2])
    callback.message.text = f"📋 Список промокодов {page}"
    await list_promos(callback.message)
    await callback.answer()

# ----- Управление заданиями -----
@dp.message_handler(lambda message: message.text == "📋 Управление заданиями")
async def admin_tasks_menu(message: types.Message):
    if not await is_super_admin(message.from_user.id):
        await message.answer("❌ Только суперадмин может управлять заданиями.")
        return
    await message.answer("Управление заданиями:", reply_markup=admin_tasks_keyboard())

@dp.message_handler(lambda message: message.text == "➕ Создать задание")
async def create_task_start(message: types.Message):
    if not await is_super_admin(message.from_user.id):
        await message.answer("❌ Только суперадмин может создавать задания.")
        return
    await message.answer("Введи название задания:", reply_markup=back_keyboard())
    await CreateTask.name.set()

@dp.message_handler(state=CreateTask.name)
async def create_task_name(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_tasks_menu(message)
        return
    await state.update_data(name=message.text)
    await message.answer("Введи описание задания:")
    await CreateTask.next()

@dp.message_handler(state=CreateTask.description)
async def create_task_description(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_tasks_menu(message)
        return
    await state.update_data(description=message.text)
    await message.answer("Введи тип задания (subscribe):")
    await CreateTask.next()

@dp.message_handler(state=CreateTask.task_type)
async def create_task_type(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_tasks_menu(message)
        return
    task_type = message.text.lower()
    if task_type not in ['subscribe']:
        await message.answer("Поддерживается только 'subscribe'")
        return
    await state.update_data(task_type=task_type)
    await message.answer("Введи ID канала (с -100) для подписки:")
    await CreateTask.next()

@dp.message_handler(state=CreateTask.target_id)
async def create_task_target(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_tasks_menu(message)
        return
    await state.update_data(target_id=message.text.strip())
    await message.answer("Введи награду (монеты):")
    await CreateTask.next()

@dp.message_handler(state=CreateTask.reward_coins)
async def create_task_reward_coins(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_tasks_menu(message)
        return
    try:
        coins = int(message.text)
    except:
        await message.answer("Введи целое число.")
        return
    await state.update_data(reward_coins=coins)
    await message.answer("Введи награду (репутация):")
    await CreateTask.next()

@dp.message_handler(state=CreateTask.reward_reputation)
async def create_task_reward_rep(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_tasks_menu(message)
        return
    try:
        rep = int(message.text)
    except:
        await message.answer("Введи целое число.")
        return
    await state.update_data(reward_reputation=rep)
    await message.answer("Сколько дней нужно быть подписанным? (0 - не проверять):")
    await CreateTask.next()

@dp.message_handler(state=CreateTask.required_days)
async def create_task_required_days(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_tasks_menu(message)
        return
    try:
        days = int(message.text)
        if days < 0:
            raise ValueError
    except:
        await message.answer("Введи неотрицательное целое число.")
        return
    await state.update_data(required_days=days)
    await message.answer("Штрафных дней (если отписался раньше, 0 - нет штрафа):")
    await CreateTask.next()

@dp.message_handler(state=CreateTask.penalty_days)
async def create_task_penalty_days(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_tasks_menu(message)
        return
    try:
        days = int(message.text)
        if days < 0:
            raise ValueError
    except:
        await message.answer("Введи неотрицательное целое число.")
        return
    data = await state.get_data()
    try:
        async with db_pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO tasks (name, description, task_type, target_id, reward_coins, reward_reputation, required_days, penalty_days, created_by, created_at, active) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, TRUE)",
                data['name'], data['description'], data['task_type'], data['target_id'], data['reward_coins'], data['reward_reputation'], data['required_days'], days, message.from_user.id, datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            )
        await message.answer("✅ Задание создано!", reply_markup=admin_tasks_keyboard())
    except Exception as e:
        logging.error(f"Create task error: {e}")
        await message.answer("❌ Ошибка при создании задания.")
    await state.finish()

@dp.message_handler(lambda message: message.text == "📋 Список заданий")
async def list_tasks(message: types.Message):
    if not await is_super_admin(message.from_user.id):
        await message.answer("❌ Только суперадмин может просматривать список заданий.")
        return
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT id, name, active FROM tasks ORDER BY id")
    if not rows:
        await message.answer("Нет заданий.")
        return
    text = "📋 Задания:\n"
    for row in rows:
        text += f"ID {row['id']}: {row['name']} ({'активно' if row['active'] else 'неактивно'})\n"
    await message.answer(text, reply_markup=admin_tasks_keyboard())

@dp.message_handler(lambda message: message.text == "❌ Удалить задание")
async def delete_task_start(message: types.Message):
    if not await is_super_admin(message.from_user.id):
        await message.answer("❌ Только суперадмин может удалять задания.")
        return
    await message.answer("Введи ID задания для удаления (деактивации):", reply_markup=back_keyboard())
    await DeleteTask.task_id.set()

@dp.message_handler(state=DeleteTask.task_id)
async def delete_task_finish(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_tasks_menu(message)
        return
    try:
        task_id = int(message.text)
    except:
        await message.answer("Введи число.")
        return
    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE tasks SET active=FALSE WHERE id=$1", task_id)
    await message.answer("✅ Задание деактивировано.", reply_markup=admin_tasks_keyboard())
    await state.finish()

# ----- Управление чатами (подтверждение, список) -----
@dp.message_handler(lambda message: message.text == "🤖 Управление чатами")
async def admin_chats_menu(message: types.Message):
    if not await is_super_admin(message.from_user.id):
        await message.answer("❌ Только суперадмин может управлять чатами.")
        return
    await message.answer("Управление чатами:", reply_markup=admin_chats_keyboard())

@dp.message_handler(lambda message: message.text == "📋 Список запросов на подтверждение")
async def list_pending_requests(message: types.Message):
    if not await is_super_admin(message.from_user.id):
        return
    requests = await get_pending_chat_requests()
    if not requests:
        await message.answer("Нет ожидающих запросов.")
        return
    text = "📋 Ожидающие запросы:\n\n"
    for req in requests:
        text += f"• {req['title']} (ID: {req['chat_id']})\n  Запросил: {req['requested_by']} ({req['request_date']})\n"
    await message.answer(text)

@dp.message_handler(lambda message: message.text == "✅ Подтвердить чат")
async def confirm_chat_manual(message: types.Message):
    if not await is_super_admin(message.from_user.id):
        return
    await message.answer("Введи ID чата, который хочешь подтвердить:", reply_markup=back_keyboard())
    await ManageChats.chat_id.set()
    async with state.proxy() as data:
        data['action'] = "confirm"

@dp.message_handler(lambda message: message.text == "❌ Отклонить запрос")
async def reject_chat_manual(message: types.Message):
    if not await is_super_admin(message.from_user.id):
        return
    await message.answer("Введи ID чата, запрос которого хочешь отклонить:", reply_markup=back_keyboard())
    await ManageChats.chat_id.set()
    async with state.proxy() as data:
        data['action'] = "reject"

@dp.message_handler(lambda message: message.text == "🗑 Удалить чат из подтверждённых")
async def remove_confirmed_chat_start(message: types.Message):
    if not await is_super_admin(message.from_user.id):
        return
    await message.answer("Введи ID чата, который нужно удалить из подтверждённых:", reply_markup=back_keyboard())
    await ManageChats.chat_id.set()
    async with state.proxy() as data:
        data['action'] = "remove"

@dp.message_handler(lambda message: message.text == "📋 Список подтверждённых чатов")
async def list_confirmed_chats(message: types.Message):
    if not await is_super_admin(message.from_user.id):
        return
    confirmed = await get_confirmed_chats(force_update=True)
    if not confirmed:
        await message.answer("Нет подтверждённых чатов.")
        return
    text = "✅ Подтверждённые чаты:\n\n"
    for chat_id, data in confirmed.items():
        text += f"• {data['title']} (ID: {chat_id})\n  Подтверждён: {data.get('confirmed_date', 'неизвестно')}\n"
    await message.answer(text)

@dp.message_handler(state=ManageChats.chat_id)
async def process_chat_id(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_chats_menu(message)
        return
    try:
        chat_id = int(message.text)
    except:
        await message.answer("❌ Введи число.")
        return
    data = await state.get_data()
    action = data.get('action')
    async with db_pool.acquire() as conn:
        if action == "confirm":
            request = await conn.fetchrow("SELECT * FROM chat_confirmation_requests WHERE chat_id=$1", chat_id)
            if not request:
                await message.answer("❌ Запрос не найден.")
                await state.finish()
                return
            await add_confirmed_chat(chat_id, request['title'], request['type'], message.from_user.id)
            await update_chat_request_status(chat_id, 'approved')
            await message.answer(f"✅ Чат {request['title']} подтверждён.")
            await safe_send_message(request['requested_by'], f"✅ Ваш чат «{request['title']}» активирован!")
        elif action == "reject":
            request = await conn.fetchrow("SELECT * FROM chat_confirmation_requests WHERE chat_id=$1", chat_id)
            if not request:
                await message.answer("❌ Запрос не найден.")
                await state.finish()
                return
            await update_chat_request_status(chat_id, 'rejected')
            await message.answer(f"❌ Запрос для чата {request['title']} отклонён.")
            await safe_send_message(request['requested_by'], f"❌ Запрос на активацию чата «{request['title']}» отклонён.")
        elif action == "remove":
            await remove_confirmed_chat(chat_id)
            await message.answer(f"✅ Чат {chat_id} удалён из подтверждённых.")
    await state.finish()

# ----- Управление боссами -----
@dp.message_handler(lambda message: message.text == "👾 Управление боссами")
async def admin_boss_menu(message: types.Message):
    if not await is_super_admin(message.from_user.id):
        await message.answer("❌ Только суперадмин может управлять боссами.")
        return
    await message.answer("Управление боссами:", reply_markup=admin_boss_keyboard())

@dp.message_handler(lambda message: message.text == "📋 Активные боссы")
async def list_active_bosses(message: types.Message):
    if not await is_super_admin(message.from_user.id):
        return
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM bosses WHERE status='active' ORDER BY spawned_at")
    if not rows:
        await message.answer("Нет активных боссов.")
        return
    text = "Активные боссы:\n"
    for row in rows:
        text += f"ID {row['id']}: {row['name']} (ур. {row['level']}) в чате {row['chat_id']}, HP {row['hp']}/{row['max_hp']}\n"
    await message.answer(text)

@dp.message_handler(lambda message: message.text == "⚔️ Создать босса вручную")
async def manual_spawn_boss_start(message: types.Message):
    if not await is_super_admin(message.from_user.id):
        return
    await message.answer("Введи ID чата, где создать босса:", reply_markup=back_keyboard())
    await BossSpawn.chat_id.set()

@dp.message_handler(state=BossSpawn.chat_id)
async def manual_spawn_boss_chat(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_boss_menu(message)
        return
    try:
        chat_id = int(message.text)
    except:
        await message.answer("❌ Введи число.")
        return
    if not await is_chat_confirmed(chat_id):
        await message.answer("❌ Чат не подтверждён. Сначала подтвердите его.")
        await state.finish()
        return
    await state.update_data(chat_id=chat_id)
    await message.answer("Введи уровень босса (1-10):")
    await BossSpawn.level.set()

@dp.message_handler(state=BossSpawn.level)
async def manual_spawn_boss_level(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_boss_menu(message)
        return
    try:
        level = int(message.text)
        if level < 1 or level > 10:
            raise ValueError
    except:
        await message.answer("❌ Введи число от 1 до 10.")
        return
    data = await state.get_data()
    chat_id = data['chat_id']
    await spawn_boss(chat_id, level=level)
    await message.answer(f"✅ Босс {level} уровня создан в чате {chat_id}.")
    await state.finish()

# ----- Настройки игры -----
@dp.message_handler(lambda message: message.text == "⚙️ Настройки игры")
async def settings_menu(message: types.Message):
    if not await is_super_admin(message.from_user.id):
        await message.answer("❌ Только суперадмин может изменять настройки игры.")
        return
    settings = {}
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT key, value FROM settings")
        for row in rows:
            settings[row['key']] = row['value']
    text = "⚙️ <b>Текущие настройки игры:</b>\n\n"
    text += f"💰 Стоимость случайной кражи: {settings.get('random_attack_cost', '0')} монет\n"
    text += f"👤 Стоимость кражи по username: {settings.get('targeted_attack_cost', '50')} монет\n"
    text += f"⏱ Кулдаун между кражами: {settings.get('theft_cooldown_minutes', '30')} мин\n"
    text += f"🎲 Шанс успеха кражи: {settings.get('theft_success_chance', '40')}%\n"
    text += f"🛡 Шанс защиты жертвы: {settings.get('theft_defense_chance', '20')}%\n"
    text += f"💥 Штраф при защите: {settings.get('theft_defense_penalty', '10')} монет\n"
    text += f"🎰 Шанс выигрыша в казино: {settings.get('casino_win_chance', '30')}%\n"
    text += f"💰 Мин. сумма кражи: {settings.get('min_theft_amount', '5')}\n"
    text += f"💰 Макс. сумма кражи: {settings.get('max_theft_amount', '15')}\n"
    text += f"🎲 Множитель костей: {settings.get('dice_multiplier', '2')}\n"
    text += f"🔢 Множитель угадайки: {settings.get('guess_multiplier', '5')}\n"
    text += f"⭐️ Репутация за угадайку: {settings.get('guess_reputation', '1')}\n"
    text += f"📢 Уведомления в чатах: {settings.get('chat_notify_big_win', '1')} (1-вкл, 0-выкл)\n"
    text += f"💰 Сумма подарка в чате: {settings.get('gift_amount', '30')}\n"
    text += f"📊 Лимит подарков в день (чат): {settings.get('gift_limit_per_day', '3')}\n"
    text += f"👥 Реферальный бонус (монеты): {settings.get('referral_bonus', '50')}\n"
    text += f"⭐️ Реферальный бонус (репутация): {settings.get('referral_reputation', '2')}\n"
    text += f"📈 Опыт за казино (победа): {settings.get('exp_per_casino_win', '5')}\n"
    text += f"📉 Опыт за казино (поражение): {settings.get('exp_per_casino_lose', '1')}\n"
    text += f"🎲 Опыт за кости (победа): {settings.get('exp_per_dice_win', '3')}\n"
    text += f"🎲 Опыт за кости (поражение): {settings.get('exp_per_dice_lose', '1')}\n"
    text += f"🔢 Опыт за угадайку (победа): {settings.get('exp_per_guess_win', '4')}\n"
    text += f"🔢 Опыт за угадайку (поражение): {settings.get('exp_per_guess_lose', '1')}\n"
    text += f"🔫 Опыт за успешный грабёж: {settings.get('exp_per_theft_success', '10')}\n"
    text += f"🔫 Опыт за провал грабежа: {settings.get('exp_per_theft_fail', '2')}\n"
    text += f"🛡 Опыт за защиту: {settings.get('exp_per_theft_defense', '5')}\n"
    text += f"👥 Опыт за победу в 21: {settings.get('exp_per_game_win', '15')}\n"
    text += f"👥 Опыт за поражение в 21: {settings.get('exp_per_game_lose', '3')}\n"
    text += f"📈 Множитель опыта для уровня: {settings.get('level_multiplier', '100')}\n"
    text += f"💰 Базовая награда за уровень (монеты): {settings.get('level_reward_coins', '50')}\n"
    text += f"⭐️ Базовая награда за уровень (репутация): {settings.get('level_reward_reputation', '5')}\n"
    text += f"📈 Инкремент награды (монеты): {settings.get('level_reward_coins_increment', '10')}\n"
    text += f"⭐️ Инкремент награды (репутация): {settings.get('level_reward_reputation_increment', '1')}\n"
    text += f"🎯 Бонус репутации к грабежу (%): {settings.get('reputation_theft_bonus', '0.5')}\n"
    text += f"🛡 Бонус репутации к защите (%): {settings.get('reputation_defense_bonus', '0.5')}\n"
    text += f"👾 Шанс появления босса (%): {settings.get('boss_spawn_chance', '20')}\n"
    text += f"⏱ Мин. интервал между боссами (мин): {settings.get('boss_min_interval', '360')}\n"
    text += f"📊 Макс. боссов в день: {settings.get('boss_max_per_day', '2')}\n"
    text += f"❤️ Множитель HP босса: {settings.get('boss_hp_multiplier', '100')}\n"
    text += f"⚔️ Кулдаун атаки (мин): {settings.get('boss_attack_cooldown', '3')}\n"
    text += f"💥 Базовый урон игрока: {settings.get('boss_base_damage', '10')}\n"
    text += f"💰 Базовая награда за босса: {settings.get('boss_reward_coins', '500')}\n"
    text += f"💰 Вариация награды: {settings.get('boss_reward_coins_variance', '200')}\n"
    text += f"🎁 Глобальный лимит подгона в день: {settings.get('gift_global_limit_per_user', '4')}\n"
    text += f"⏱ Кулдаун подгона (мин): {settings.get('gift_cooldown', '60')}\n"
    text += f"💪 Силы за уровень: {settings.get('stat_strength_per_level', '1')}\n"
    text += f"🏃 Ловкости за уровень: {settings.get('stat_agility_per_level', '1')}\n"
    text += f"🛡 Защиты за уровень: {settings.get('stat_defense_per_level', '1')}\n\n"
    text += "Выбери параметр для изменения (нажми на кнопку):"
    await message.answer(text, reply_markup=settings_reply_keyboard())

@dp.message_handler(lambda message: message.text in [
    "💰 Стоимость случайной кражи", "👤 Стоимость кражи по username", "⏱ Кулдаун (минут)",
    "🎲 Шанс успеха %", "🛡 Шанс защиты %", "💥 Штраф при защите", "🎰 Шанс казино %",
    "💰 Мин. сумма кражи", "💰 Макс. сумма кражи", "🎲 Множитель костей", "🔢 Множитель угадайки",
    "⭐️ Репутация за угадайку", "📢 Уведомления в чатах", "💰 Сумма подарка в чате",
    "📊 Лимит подарков в день", "👥 Реферальный бонус (монеты)", "⭐️ Реферальный бонус (репутация)",
    "📈 Опыт за казино (победа)", "📉 Опыт за казино (поражение)", "🎲 Опыт за кости (победа)",
    "🎲 Опыт за кости (поражение)", "🔢 Опыт за угадайку (победа)", "🔢 Опыт за угадайку (поражение)",
    "🔫 Опыт за успешный грабёж", "🔫 Опыт за провал грабежа", "🛡 Опыт за защиту",
    "👥 Опыт за победу в 21", "👥 Опыт за поражение в 21", "📈 Множитель опыта для уровня",
    "💰 Базовая награда за уровень (монеты)", "⭐️ Базовая награда за уровень (репутация)",
    "📈 Инкремент награды (монеты)", "⭐️ Инкремент награды (репутация)",
    "🎯 Бонус репутации к грабежу (%)", "🛡 Бонус репутации к защите (%)",
    "👾 Шанс появления босса (%)", "⏱ Мин. интервал между боссами (мин)",
    "📊 Макс. боссов в день", "❤️ Множитель HP босса", "⚔️ Кулдаун атаки (мин)",
    "💥 Базовый урон игрока", "💰 Базовая награда за босса", "💰 Вариация награды",
    "🎁 Глобальный лимит подгона в день", "⏱ Кулдаун подгона (мин)",
    "💪 Силы за уровень", "🏃 Ловкости за уровень", "🛡 Защиты за уровень"
])
async def settings_edit_start(message: types.Message, state: FSMContext):
    if not await is_super_admin(message.from_user.id):
        await message.answer("❌ Только суперадмин может изменять настройки.")
        return
    key_map = {
        "💰 Стоимость случайной кражи": "random_attack_cost",
        "👤 Стоимость кражи по username": "targeted_attack_cost",
        "⏱ Кулдаун (минут)": "theft_cooldown_minutes",
        "🎲 Шанс успеха %": "theft_success_chance",
        "🛡 Шанс защиты %": "theft_defense_chance",
        "💥 Штраф при защите": "theft_defense_penalty",
        "🎰 Шанс казино %": "casino_win_chance",
        "💰 Мин. сумма кражи": "min_theft_amount",
        "💰 Макс. сумма кражи": "max_theft_amount",
        "🎲 Множитель костей": "dice_multiplier",
        "🔢 Множитель угадайки": "guess_multiplier",
        "⭐️ Репутация за угадайку": "guess_reputation",
        "📢 Уведомления в чатах": "chat_notify_big_win",
        "💰 Сумма подарка в чате": "gift_amount",
        "📊 Лимит подарков в день": "gift_limit_per_day",
        "👥 Реферальный бонус (монеты)": "referral_bonus",
        "⭐️ Реферальный бонус (репутация)": "referral_reputation",
        "📈 Опыт за казино (победа)": "exp_per_casino_win",
        "📉 Опыт за казино (поражение)": "exp_per_casino_lose",
        "🎲 Опыт за кости (победа)": "exp_per_dice_win",
        "🎲 Опыт за кости (поражение)": "exp_per_dice_lose",
        "🔢 Опыт за угадайку (победа)": "exp_per_guess_win",
        "🔢 Опыт за угадайку (поражение)": "exp_per_guess_lose",
        "🔫 Опыт за успешный грабёж": "exp_per_theft_success",
        "🔫 Опыт за провал грабежа": "exp_per_theft_fail",
        "🛡 Опыт за защиту": "exp_per_theft_defense",
        "👥 Опыт за победу в 21": "exp_per_game_win",
        "👥 Опыт за поражение в 21": "exp_per_game_lose",
        "📈 Множитель опыта для уровня": "level_multiplier",
        "💰 Базовая награда за уровень (монеты)": "level_reward_coins",
        "⭐️ Базовая награда за уровень (репутация)": "level_reward_reputation",
        "📈 Инкремент награды (монеты)": "level_reward_coins_increment",
        "⭐️ Инкремент награды (репутация)": "level_reward_reputation_increment",
        "🎯 Бонус репутации к грабежу (%)": "reputation_theft_bonus",
        "🛡 Бонус репутации к защите (%)": "reputation_defense_bonus",
        "👾 Шанс появления босса (%)": "boss_spawn_chance",
        "⏱ Мин. интервал между боссами (мин)": "boss_min_interval",
        "📊 Макс. боссов в день": "boss_max_per_day",
        "❤️ Множитель HP босса": "boss_hp_multiplier",
        "⚔️ Кулдаун атаки (мин)": "boss_attack_cooldown",
        "💥 Базовый урон игрока": "boss_base_damage",
        "💰 Базовая награда за босса": "boss_reward_coins",
        "💰 Вариация награды": "boss_reward_coins_variance",
        "🎁 Глобальный лимит подгона в день": "gift_global_limit_per_user",
        "⏱ Кулдаун подгона (мин)": "gift_cooldown",
        "💪 Силы за уровень": "stat_strength_per_level",
        "🏃 Ловкости за уровень": "stat_agility_per_level",
        "🛡 Защиты за уровень": "stat_defense_per_level",
    }
    key = key_map.get(message.text)
    if not key:
        return
    await state.update_data(setting_key=key)
    await message.answer(f"Введи новое значение для параметра (целое число):", reply_markup=back_keyboard())
    await EditSettings.key.set()

@dp.message_handler(state=EditSettings.key)
async def set_setting_value(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await settings_menu(message)
        return
    try:
        value = int(message.text)
    except ValueError:
        await message.answer("❌ Введи целое число.")
        return
    data = await state.get_data()
    key = data['setting_key']
    await set_setting(key, str(value))
    await message.answer(f"✅ Параметр обновлён.")
    await state.finish()
    await settings_menu(message)

# ----- Статистика -----
@dp.message_handler(lambda message: message.text == "📊 Статистика")
async def stats_handler(message: types.Message):
    if not await is_admin(message.from_user.id):
        await message.answer("❌ У тебя нет прав администратора.")
        return
    try:
        async with db_pool.acquire() as conn:
            users = await conn.fetchval("SELECT COUNT(*) FROM users")
            total_balance = await conn.fetchval("SELECT SUM(balance) FROM users") or 0
            total_reputation = await conn.fetchval("SELECT SUM(reputation) FROM users") or 0
            total_spent = await conn.fetchval("SELECT SUM(total_spent) FROM users") or 0
            active_giveaways = await conn.fetchval("SELECT COUNT(*) FROM giveaways WHERE status='active'") or 0
            shop_items = await conn.fetchval("SELECT COUNT(*) FROM shop_items") or 0
            purchases_pending = await conn.fetchval("SELECT COUNT(*) FROM purchases WHERE status='pending'") or 0
            purchases_completed = await conn.fetchval("SELECT COUNT(*) FROM purchases WHERE status='completed'") or 0
            total_thefts = await conn.fetchval("SELECT SUM(theft_attempts) FROM users") or 0
            total_thefts_success = await conn.fetchval("SELECT SUM(theft_success) FROM users") or 0
            promos = await conn.fetchval("SELECT COUNT(*) FROM promocodes") or 0
            banned = await conn.fetchval("SELECT COUNT(*) FROM banned_users") or 0
            total_bosses = await conn.fetchval("SELECT COUNT(*) FROM bosses") or 0
            active_bosses = await conn.fetchval("SELECT COUNT(*) FROM bosses WHERE status='active'") or 0
            confirmed_chats = await conn.fetchval("SELECT COUNT(*) FROM confirmed_chats") or 0
        text = (
            f"📊 Статистика:\n"
            f"👥 Пользователей: {users}\n"
            f"💰 Всего монет: {total_balance}\n"
            f"⭐️ Всего репутации: {total_reputation}\n"
            f"💸 Всего потрачено: {total_spent}\n"
            f"🎁 Активных розыгрышей: {active_giveaways}\n"
            f"🛒 Товаров в магазине: {shop_items}\n"
            f"🛍️ Ожидающих покупок: {purchases_pending}\n"
            f"✅ Выполненных покупок: {purchases_completed}\n"
            f"🔫 Всего ограблений: {total_thefts} (успешно: {total_thefts_success})\n"
            f"🎫 Промокодов создано: {promos}\n"
            f"⛔ Заблокировано: {banned}\n"
            f"👾 Всего боссов: {total_bosses} (активных: {active_bosses})\n"
            f"✅ Подтверждённых чатов: {confirmed_chats}"
        )
        await message.answer(text, reply_markup=admin_main_keyboard(await is_super_admin(message.from_user.id)))
    except Exception as e:
        logging.error(f"Stats error: {e}")
        await message.answer("❌ Ошибка получения статистики.")

# ----- Рассылка -----
@dp.message_handler(lambda message: message.text == "📢 Рассылка")
async def broadcast_start(message: types.Message):
    if not await is_super_admin(message.from_user.id):
        await message.answer("❌ Только суперадмин может делать рассылку.")
        return
    await message.answer("Отправь сообщение для рассылки (текст, фото, видео или документ).", reply_markup=back_keyboard())
    await Broadcast.media.set()

@dp.message_handler(state=Broadcast.media, content_types=['text', 'photo', 'video', 'document'])
async def broadcast_media(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        super_admin = await is_super_admin(message.from_user.id)
        await message.answer("Панель администратора:", reply_markup=admin_main_keyboard(super_admin))
        return

    content = {}
    if message.text:
        content['type'] = 'text'
        content['text'] = message.text
    elif message.photo:
        content['type'] = 'photo'
        content['file_id'] = message.photo[-1].file_id
        content['caption'] = message.caption or ""
    elif message.video:
        content['type'] = 'video'
        content['file_id'] = message.video.file_id
        content['caption'] = message.caption or ""
    elif message.document:
        content['type'] = 'document'
        content['file_id'] = message.document.file_id
        content['caption'] = message.caption or ""
    else:
        await message.answer("Неподдерживаемый тип.")
        return

    await state.finish()

    status_msg = await message.answer("⏳ Рассылка начата... Это может занять некоторое время.")

    async with db_pool.acquire() as conn:
        users = await conn.fetch("SELECT user_id FROM users")
        users = [r['user_id'] for r in users]

    sent = 0
    failed = 0
    total = len(users)

    for i, uid in enumerate(users):
        if await is_banned(uid):
            continue
        try:
            if content['type'] == 'text':
                await bot.send_message(uid, content['text'])
            elif content['type'] == 'photo':
                await bot.send_photo(uid, content['file_id'], caption=content['caption'])
            elif content['type'] == 'video':
                await bot.send_video(uid, content['file_id'], caption=content['caption'])
            elif content['type'] == 'document':
                await bot.send_document(uid, content['file_id'], caption=content['caption'])
            sent += 1
        except (BotBlocked, UserDeactivated, ChatNotFound):
            failed += 1
        except RetryAfter as e:
            logging.warning(f"Flood limit, waiting {e.timeout} seconds")
            await asyncio.sleep(e.timeout)
            try:
                if content['type'] == 'text':
                    await bot.send_message(uid, content['text'])
                else:
                    if content['type'] == 'photo':
                        await bot.send_photo(uid, content['file_id'], caption=content['caption'])
                    elif content['type'] == 'video':
                        await bot.send_video(uid, content['file_id'], caption=content['caption'])
                    elif content['type'] == 'document':
                        await bot.send_document(uid, content['file_id'], caption=content['caption'])
                sent += 1
            except:
                failed += 1
        except Exception as e:
            failed += 1
            logging.warning(f"Failed to send to {uid}: {e}")

        if (i + 1) % 10 == 0:
            try:
                await status_msg.edit_text(f"⏳ Прогресс: {i+1}/{total}\n✅ Отправлено: {sent}\n❌ Ошибок: {failed}")
            except:
                pass

        await asyncio.sleep(0.05)

    await status_msg.edit_text(f"✅ Рассылка завершена!\n📊 Отправлено: {sent}\n❌ Ошибок: {failed}\n👥 Всего: {total}")

# ----- Блокировки -----
@dp.message_handler(lambda message: message.text == "🔨 Блокировки")
async def admin_ban_menu(message: types.Message):
    if not await is_super_admin(message.from_user.id):
        await message.answer("❌ Только суперадмин может управлять блокировками.")
        return
    await message.answer("Управление блокировками:", reply_markup=admin_ban_keyboard())

@dp.message_handler(lambda message: message.text == "🔨 Заблокировать пользователя")
async def block_user_start(message: types.Message):
    if not await is_super_admin(message.from_user.id):
        await message.answer("❌ Только суперадмин может блокировать пользователей.")
        return
    await message.answer("Введи ID или @username пользователя для блокировки:", reply_markup=back_keyboard())
    await BlockUser.user_id.set()

@dp.message_handler(state=BlockUser.user_id)
async def block_user_id(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_ban_menu(message)
        return
    user_data = await find_user_by_input(message.text)
    if not user_data:
        await message.answer("❌ Пользователь не найден.")
        return
    uid = user_data['user_id']
    if await is_admin(uid):
        await message.answer("❌ Нельзя заблокировать администратора.")
        await state.finish()
        return
    await state.update_data(user_id=uid)
    await message.answer("Введи причину блокировки (можно отправить 'нет'):")
    await BlockUser.reason.set()

@dp.message_handler(state=BlockUser.reason)
async def block_user_reason(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_ban_menu(message)
        return
    reason = None if message.text.lower() == 'нет' else message.text
    data = await state.get_data()
    uid = data['user_id']
    try:
        async with db_pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO banned_users (user_id, banned_by, banned_date, reason) VALUES ($1, $2, $3, $4) ON CONFLICT (user_id) DO NOTHING",
                uid, message.from_user.id, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), reason
            )
        await message.answer(f"✅ Пользователь {uid} заблокирован.")
        await safe_send_message(uid, f"⛔ Вы заблокированы в боте. Причина: {reason if reason else 'не указана'}")
    except Exception as e:
        logging.error(f"Block user error: {e}")
        await message.answer("❌ Ошибка.")
    await state.finish()

@dp.message_handler(lambda message: message.text == "🔓 Разблокировать пользователя")
async def unblock_user_start(message: types.Message):
    if not await is_super_admin(message.from_user.id):
        await message.answer("❌ Только суперадмин может разблокировать пользователей.")
        return
    await message.answer("Введи ID или @username пользователя для разблокировки:", reply_markup=back_keyboard())
    await UnblockUser.user_id.set()

@dp.message_handler(state=UnblockUser.user_id)
async def unblock_user_finish(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_ban_menu(message)
        return
    user_data = await find_user_by_input(message.text)
    if not user_data:
        await message.answer("❌ Пользователь не найден.")
        return
    uid = user_data['user_id']
    try:
        async with db_pool.acquire() as conn:
            await conn.execute("DELETE FROM banned_users WHERE user_id=$1", uid)
        await message.answer(f"✅ Пользователь {uid} разблокирован.")
        await safe_send_message(uid, "🔓 Вы разблокированы в боте.")
    except Exception as e:
        logging.error(f"Unblock user error: {e}")
        await message.answer("❌ Ошибка.")
    await state.finish()

@dp.message_handler(lambda message: message.text == "📋 Список заблокированных")
async def list_banned(message: types.Message):
    if not await is_super_admin(message.from_user.id):
        return
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT user_id, banned_date, reason FROM banned_users ORDER BY banned_date DESC")
    if not rows:
        await message.answer("Нет заблокированных пользователей.")
        return
    text = "⛔ Заблокированные пользователи:\n\n"
    for row in rows:
        text += f"ID: {row['user_id']}, Дата: {row['banned_date']}\nПричина: {row['reason'] or 'не указана'}\n\n"
    await message.answer(text)

# ----- Управление админами -----
@dp.message_handler(lambda message: message.text == "➕ Управление админами")
async def admin_admins_menu(message: types.Message):
    if not await is_super_admin(message.from_user.id):
        await message.answer("❌ Только суперадмин может управлять админами.")
        return
    await message.answer("Управление админами:", reply_markup=admin_admins_keyboard())

@dp.message_handler(lambda message: message.text == "➕ Добавить админа")
async def add_admin_start(message: types.Message):
    if not await is_super_admin(message.from_user.id):
        await message.answer("Только суперадмин может добавлять админов.")
        return
    await message.answer("Введи ID или @username пользователя, которого хочешь сделать младшим админом:", reply_markup=back_keyboard())
    await AddJuniorAdmin.user_id.set()

@dp.message_handler(state=AddJuniorAdmin.user_id)
async def add_admin_finish(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_admins_menu(message)
        return
    user_data = await find_user_by_input(message.text)
    if not user_data:
        await message.answer("❌ Пользователь не найден.")
        return
    uid = user_data['user_id']
    try:
        async with db_pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO admins (user_id, added_by, added_date) VALUES ($1, $2, $3)",
                uid, message.from_user.id, datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            )
        await message.answer(f"✅ Пользователь {uid} теперь младший админ.")
    except asyncpg.UniqueViolationError:
        await message.answer("❌ Этот пользователь уже админ.")
    except Exception as e:
        logging.error(f"Add admin error: {e}")
        await message.answer("❌ Ошибка.")
    await state.finish()

@dp.message_handler(lambda message: message.text == "➖ Удалить админа")
async def remove_admin_start(message: types.Message):
    if not await is_super_admin(message.from_user.id):
        await message.answer("Только суперадмин может удалять админов.")
        return
    await message.answer("Введи ID или @username пользователя, которого хочешь лишить прав админа:", reply_markup=back_keyboard())
    await RemoveJuniorAdmin.user_id.set()

@dp.message_handler(state=RemoveJuniorAdmin.user_id)
async def remove_admin_finish(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_admins_menu(message)
        return
    user_data = await find_user_by_input(message.text)
    if not user_data:
        await message.answer("❌ Пользователь не найден.")
        return
    uid = user_data['user_id']
    try:
        async with db_pool.acquire() as conn:
            await conn.execute("DELETE FROM admins WHERE user_id=$1", uid)
        await message.answer(f"✅ Пользователь {uid} больше не админ, если был им.")
    except Exception as e:
        logging.error(f"Remove admin error: {e}")
        await message.answer("❌ Ошибка.")
    await state.finish()

@dp.message_handler(lambda message: message.text == "📋 Список админов")
async def list_admins(message: types.Message):
    if not await is_super_admin(message.from_user.id):
        return
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT user_id, added_date FROM admins ORDER BY added_date")
    if not rows:
        await message.answer("Нет младших админов.")
        return
    text = "👥 Младшие админы:\n"
    for row in rows:
        text += f"• ID: {row['user_id']}, назначен: {row['added_date']}\n"
    await message.answer(text)

# ----- Очистка старых записей -----
@dp.message_handler(lambda message: message.text == "🧹 Очистка старых записей")
async def cleanup_old_data(message: types.Message):
    if not await is_super_admin(message.from_user.id):
        return
    async with db_pool.acquire() as conn:
        await conn.execute("DELETE FROM bosses WHERE status IN ('defeated', 'expired') AND spawned_at < NOW() - INTERVAL '7 days'")
        await conn.execute("DELETE FROM boss_attacks WHERE attack_time < NOW() - INTERVAL '7 days'")
        await conn.execute("DELETE FROM giveaways WHERE status='completed' AND end_date < NOW() - INTERVAL '30 days'")
    await message.answer("✅ Старые записи очищены.")

# ===== ФОНОВЫЕ ЗАДАЧИ =====
async def boss_spawn_loop():
    while True:
        await asyncio.sleep(300)  # каждые 5 минут
        try:
            confirmed = await get_confirmed_chats()
            now = datetime.now()
            for chat_id, data in confirmed.items():
                boss_max_per_day = int(await get_setting("boss_max_per_day"))
                boss_spawn_count = data.get('boss_spawn_count', 0)
                if boss_spawn_count >= boss_max_per_day:
                    continue
                last_spawn_str = data.get('boss_last_spawn')
                if last_spawn_str:
                    last_spawn = datetime.strptime(last_spawn_str, "%Y-%m-%d %H:%M:%S")
                    min_interval = int(await get_setting("boss_min_interval"))
                    if (now - last_spawn).total_seconds() < min_interval * 60:
                        continue
                chance = int(await get_setting("boss_spawn_chance"))
                if random.randint(1, 100) <= chance:
                    await spawn_boss(chat_id)
        except Exception as e:
            logging.error(f"Boss spawn loop error: {e}")

async def check_expired_bosses():
    while True:
        await asyncio.sleep(600)  # каждые 10 минут
        try:
            async with db_pool.acquire() as conn:
                now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                await conn.execute("UPDATE bosses SET status='expired' WHERE status='active' AND expires_at < $1", now)
                await conn.execute("DELETE FROM bosses WHERE status='expired' AND expires_at < $1", (datetime.now() - timedelta(hours=2)).strftime("%Y-%m-%d %H:%M:%S"))
        except Exception as e:
            logging.error(f"Check expired bosses error: {e}")

async def reset_daily_limits():
    while True:
        now = datetime.now()
        next_reset = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        sleep_seconds = (next_reset - now).total_seconds()
        await asyncio.sleep(sleep_seconds)
        try:
            async with db_pool.acquire() as conn:
                await conn.execute("UPDATE users SET gift_count_today = 0")
                await conn.execute("UPDATE confirmed_chats SET gift_count_today = 0, boss_spawn_count = 0")
            logging.info("Daily limits reset.")
        except Exception as e:
            logging.error(f"Reset daily limits error: {e}")

# ===== ВЕБ-СЕРВЕР =====
async def handle(request):
    return web.Response(text="Bot is running")

async def start_web_server():
    app = web.Application()
    app.router.add_get("/", handle)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get("PORT", 8080))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logging.info(f"Web server started on port {port}")

# ===== ЗАПУСК =====
async def on_startup(dp):
    await before_start()
    await create_db_pool()
    await init_db()
    asyncio.create_task(boss_spawn_loop())
    asyncio.create_task(check_expired_bosses())
    asyncio.create_task(reset_daily_limits())
    asyncio.create_task(start_web_server())
    logging.info("🤖 Бот запущен и готов к работе!")
    logging.info(f"👑 Суперадмины: {SUPER_ADMINS}")
    logging.info(f"🗄 База данных: PostgreSQL")

async def on_shutdown(dp):
    await db_pool.close()
    await storage.close()
    await dp.storage.close()
    await bot.close()
    logging.info("Бот остановлен")

if __name__ == "__main__":
    while True:
        try:
            executor.start_polling(dp, skip_updates=True, on_startup=on_startup, on_shutdown=on_shutdown)
        except TerminatedByOtherGetUpdates:
            logging.error("Конфликт с другим экземпляром. Жду 5 сек...")
            time.sleep(5)
            continue
        except Exception as e:
            logging.error(f"Критическая ошибка: {e}")
            time.sleep(5)
            continue

# ===== КОНЕЦ КОДА =====
